mod auth;
mod bloom_filter;
mod configuration;
mod core_types;
mod engine;
mod indexing;
mod legacy_parser;
mod persistence;
mod routing;
mod security;
mod server;
mod smart_parser;
mod two_factor_auth;

use auth::AuthConfig;
use configuration::ConfigManager;
use core_types::DatabaseError;
use engine::Database;
use routing::RouteConfig;
use server::start_health_server;
use smart_parser::AnySQL;
use std::env;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

static SVELTEKIT_PROCESS: OnceLock<Arc<Mutex<Option<Child>>>> = OnceLock::new();

const DEFAULT_HEALTH_PORT: u16 = 3306;
const HEARTBEAT_INTERVAL_SECS: u64 = 60;
const CONSOLE_DIR: &str = "console";

fn register_shutdown_handler() {
    let storage = Arc::clone(SVELTEKIT_PROCESS.get_or_init(|| Arc::new(Mutex::new(None))));

    if let Err(err) = ctrlc::try_set_handler(move || {
        if let Ok(mut guard) = storage.lock() {
            if let Some(child) = guard.as_mut() {
                if let Err(kill_err) = child.kill() {
                    eprintln!(
                        "[MirseoDB][Console] Failed to terminate SvelteKit dev server: {}",
                        kill_err
                    );
                }
            }
        }

        std::process::exit(0);
    }) {
        eprintln!(
            "[MirseoDB][Console] Failed to install shutdown handler: {}",
            err
        );
    }
}

fn spawn_console_server() {
    if env::var("MIRSEODB_SKIP_CONSOLE").is_ok() {
        println!("[MirseoDB][Console] Skipping console startup (MIRSEODB_SKIP_CONSOLE set)");
        return;
    }

    let storage = Arc::clone(SVELTEKIT_PROCESS.get_or_init(|| Arc::new(Mutex::new(None))));

    if let Ok(guard) = storage.lock() {
        if guard.is_some() {
            return;
        }
    }

    let console_path = Path::new(CONSOLE_DIR);
    if !console_path.exists() {
        println!(
            "[MirseoDB][Console] Console directory '{}' not found; skipping web console startup",
            CONSOLE_DIR
        );
        return;
    }

    let mut command = Command::new("npm");
    command.arg("run").arg("dev");
    command.current_dir(console_path);
    command.stdout(Stdio::inherit());
    command.stderr(Stdio::inherit());

    match command.spawn() {
        Ok(child) => {
            if let Ok(mut guard) = storage.lock() {
                let pid = child.id();
                *guard = Some(child);
                println!(
                    "[MirseoDB][Console] SvelteKit dev server started (npm run dev) [pid={}]; web console proxied at http://127.0.0.1:3306/ (dev server http://localhost:5173)",
                    pid
                );
            }
        }
        Err(err) => {
            eprintln!(
                "[MirseoDB][Console] Failed to start SvelteKit dev server via 'npm run dev': {}",
                err
            );
            eprintln!(
                "[MirseoDB][Console] Ensure Node.js/npm are installed and dependencies in '{}/package.json' are set up",
                CONSOLE_DIR
            );
        }
    }
}

fn main() {
    register_shutdown_handler();
    println!("[MirseoDB] Starting MirseoDB Server...");
    spawn_console_server();

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
    AuthConfig::ensure_exists().map_err(|e| DatabaseError::IoError(e))?;

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
