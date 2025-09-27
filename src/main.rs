mod anysql_parser;
mod btree;
mod config;
mod database;
mod health_server;
mod identifier;
mod parser;
mod route;
mod storage;
mod types;

use anysql_parser::AnySQL;
use config::ConfigManager;
use database::Database;
use health_server::start_health_server;
use route::RouteConfig;
use types::DatabaseError;
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_HEALTH_PORT: u16 = 3306;
const HEARTBEAT_INTERVAL_SECS: u64 = 60;

fn main() {
    println!("[MirseoDB] Starting MirseoDB Server...");

    let (database, database_name) = match initialize_database() {
        Ok(pair) => {
            println!("[MirseoDB] Database initialized successfully");
            pair
        }
        Err(e) => {
            eprintln!("[MirseoDB] Database initialization failed: {:?}", e);
            return;
        }
    };

    let parser = Arc::new(AnySQL::new());
    println!(
        "[MirseoDB] AnySQL HYPERTHINKING engine initialized - All SQL dialects supported automatically!"
    );

    let route_config = match RouteConfig::load() {
        Ok(config) => {
            println!("[MirseoDB] Route configuration loaded successfully");
            Arc::new(config)
        }
        Err(e) => {
            eprintln!("[MirseoDB] Route configuration failed to load: {}", e);
            println!("[MirseoDB] Continuing without route forwarding...");
            Arc::new(RouteConfig {
                routes: std::collections::HashMap::new(),
            })
        }
    };

    let security_config = ConfigManager::load();
    if security_config.sql_injection_protect {
        println!("[MirseoDB] SQL injection protection enabled (SQL_INJECTON_PROTECT=1)");
    } else {
        println!("[MirseoDB] SQL injection protection disabled (SQL_INJECTON_PROTECT=0)");
    }

    let api_token = env::var("MIRSEODB_API_TOKEN").ok();
    if api_token.is_some() {
        println!("[MirseoDB] API authentication enabled via MIRSEODB_API_TOKEN");
    } else {
        println!("[MirseoDB] API authentication disabled (set MIRSEODB_API_TOKEN to enable)");
    }

    let health_port = match start_health_server(
        DEFAULT_HEALTH_PORT,
        Arc::clone(&database),
        Arc::clone(&parser),
        Arc::clone(&route_config),
        api_token,
    ) {
        Ok(port) => {
            println!(
                "[MirseoDB] HTTP endpoint ready: http://127.0.0.1:{}/health (and /query)",
                port
            );
            Some(port)
        }
        Err(err) => {
            eprintln!("[MirseoDB] HTTP endpoint failed to start: {}", err);
            None
        }
    };

    if let Some(port) = health_port {
        println!("[MirseoDB] Server is running on HTTP port: {}", port);
    } else {
        println!("[MirseoDB] Server is running without an HTTP endpoint.");
    }

    println!("[MirseoDB] Server startup complete. Ready to accept connections.");

    loop {
        thread::sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));

        match database.lock() {
            Ok(db) => println!(
                "[MirseoDB] Heartbeat: database='{}' tables={}",
                database_name,
                db.tables.len()
            ),
            Err(_) => eprintln!("[MirseoDB] Heartbeat failed: database lock poisoned"),
        }
    }
}

fn initialize_database() -> Result<(Arc<Mutex<Database>>, String), DatabaseError> {
    let db_name = "mirseodb".to_string();

    ConfigManager::ensure_exists()?;

    println!("[MirseoDB] Loading database '{}'...", db_name);

    let database = match Database::load(db_name.clone()) {
        Ok(db) => {
            println!(
                "[MirseoDB] Existing database '{}' loaded successfully",
                db_name
            );
            db
        }
        Err(_) => {
            println!("[MirseoDB] Creating new database '{}'", db_name);
            Database::new(db_name.clone())
        }
    };

    Ok((Arc::new(Mutex::new(database)), db_name))
}
