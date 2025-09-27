mod modules;

use modules::{start_health_server, Database, AnySQL};
use std::thread;
use std::time::Duration;

const DEFAULT_HEALTH_PORT: u16 = 3306;

fn main() {
    println!("[MirseoDB] Starting MirseoDB Server...");

    let health_port = match start_health_server(DEFAULT_HEALTH_PORT) {
        Ok(port) => {
            println!(
                "[MirseoDB] Health endpoint ready on http://127.0.0.1:{}/health",
                port
            );
            Some(port)
        }
        Err(err) => {
            eprintln!("[MirseoDB] Health server failed to start: {}", err);
            None
        }
    };

    if let Some(port) = health_port {
        println!("[MirseoDB] Server is running on port: {}", port);
    } else {
        println!("[MirseoDB] Server is running without an HTTP health endpoint.");
    }

    let database = match initialize_database() {
        Ok(db) => {
            println!("[MirseoDB] Database initialized successfully");
            db
        }
        Err(e) => {
            eprintln!("[MirseoDB] Database initialization failed: {:?}", e);
            return;
        }
    };

    let _parser = AnySQL::new();
    println!("[MirseoDB] AnySQL HYPERTHINKING engine initialized - All SQL dialects supported automatically!");

    println!("[MirseoDB] Server startup complete. Ready to accept connections.");

    loop {
        thread::sleep(Duration::from_secs(60));
        println!("[MirseoDB] Server is running... Database: '{}' | Tables: {}",
                database.name, database.tables.len());
    }
}

fn initialize_database() -> Result<Database, modules::DatabaseError> {
    let db_name = "mirseodb".to_string();

    println!("[MirseoDB] Loading database '{}'...", db_name);

    match Database::load(db_name.clone()) {
        Ok(db) => {
            println!("[MirseoDB] Existing database '{}' loaded successfully", db_name);
            Ok(db)
        }
        Err(_) => {
            println!("[MirseoDB] Creating new database '{}'", db_name);
            Ok(Database::new(db_name))
        }
    }
}

