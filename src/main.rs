mod modules;

use modules::{start_health_server, Database, AnySQL};
use std::io::{self, Write};

const DEFAULT_HEALTH_PORT: u16 = 3306;

fn main() {
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
        println!("[MirseoDB] Server is running on port : {}", port);
    } else {
        println!("[MirseoDB] Server is running without an HTTP health endpoint.");
    }

    let mut database = match get_database_instance() {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Database initialization failed: {:?}", e);
            return;
        }
    };

    let parser = AnySQL::new();
    println!("[MirseoDB] Initialized - All SQL dialects supported automatically!");

    loop {
        print!("MirseoDB> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let command = input.trim();

                if command.is_empty() {
                    continue;
                }

                if command.eq_ignore_ascii_case("exit") || command.eq_ignore_ascii_case("quit") {
                    println!("Shutting down MirseoDB...");
                    break;
                }

                if command.eq_ignore_ascii_case("help") {
                    show_help();
                    continue;
                }

                if command.eq_ignore_ascii_case("clear") {
                    clear_screen();
                    continue;
                }

                if command.starts_with(".") {
                    handle_meta_command(command, &database);
                    continue;
                }

                execute_sql_command(&mut database, &parser, command);
            }
            Err(error) => {
                eprintln!("Cannot read user input: {}", error);
            }
        }
    }
}

fn get_database_instance() -> Result<Database, modules::DatabaseError> {
    print!("Enter database name (default: mirseodb): ");
    io::stdout().flush().unwrap();

    let mut db_name = String::new();
    io::stdin().read_line(&mut db_name).unwrap();
    let db_name = db_name.trim();

    let db_name = if db_name.is_empty() {
        "mirseodb".to_string()
    } else {
        db_name.to_string()
    };

    println!("Loading database '{}'...", db_name);

    match Database::load(db_name.clone()) {
        Ok(db) => {
            println!("Existing database '{}' loaded successfully", db_name);
            Ok(db)
        }
        Err(_) => {
            println!("Creating new database '{}'", db_name);
            Ok(Database::new(db_name))
        }
    }
}

fn execute_sql_command(database: &mut Database, parser: &AnySQL, command: &str) {
    match parser.parse(command) {
        Ok(statement) => {
            match database.execute(statement) {
                Ok(rows) => {
                    if rows.is_empty() {
                        println!("Command executed successfully.");
                    } else {
                        println!("Results:");
                        display_results(&rows);
                    }
                }
                Err(e) => {
                    eprintln!("Execution error: {:?}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("Parse error: {:?}", e);
            println!("Type 'help' for assistance.");
        }
    }
}

fn display_results(rows: &[modules::Row]) {
    if rows.is_empty() {
        println!("No results found.");
        return;
    }

    let first_row = &rows[0];
    let columns: Vec<String> = first_row.columns.keys().cloned().collect();

    let mut column_widths = std::collections::HashMap::new();
    for column in &columns {
        column_widths.insert(column.clone(), column.len().max(10));
    }

    for row in rows {
        for (column, value) in &row.columns {
            let value_str = format_sql_value(value);
            let current_width = column_widths.get(column).unwrap_or(&10);
            column_widths.insert(column.clone(), (*current_width).max(value_str.len()));
        }
    }

    print!("|");
    for column in &columns {
        let width = column_widths.get(column).unwrap_or(&10);
        print!(" {:width$} |", column, width = width);
    }
    println!();

    print!("|");
    for column in &columns {
        let width = column_widths.get(column).unwrap_or(&10);
        print!("{}", "-".repeat(width + 2));
        print!("|");
    }
    println!();

    for row in rows {
        print!("|");
        for column in &columns {
            let width = column_widths.get(column).unwrap_or(&10);
            let value = row.columns.get(column)
                .map(format_sql_value)
                .unwrap_or_else(|| "NULL".to_string());
            print!(" {:width$} |", value, width = width);
        }
        println!();
    }

    println!("\nTotal {} rows retrieved.", rows.len());
}

fn format_sql_value(value: &modules::SqlValue) -> String {
    match value {
        modules::SqlValue::Integer(i) => i.to_string(),
        modules::SqlValue::Float(f) => f.to_string(),
        modules::SqlValue::Text(s) => s.clone(),
        modules::SqlValue::Boolean(b) => b.to_string(),
        modules::SqlValue::Null => "NULL".to_string(),
    }
}

fn handle_meta_command(command: &str, database: &Database) {
    match command {
        ".tables" => {
            println!("Current database tables:");
            if database.tables.is_empty() {
                println!("  (No tables found)");
            } else {
                for table_name in database.tables.keys() {
                    println!("  - {}", table_name);
                }
            }
        }
        ".schema" => {
            println!("Database schema:");
            for table in database.tables.values() {
                println!("\nTable: {}", table.name);
                println!("Columns:");
                for column in &table.columns {
                    let pk_marker = if column.primary_key { " (PK)" } else { "" };
                    let null_marker = if column.nullable { "" } else { " NOT NULL" };
                    println!("  - {} {:?}{}{}", column.name, column.data_type, null_marker, pk_marker);
                }
                println!("Row count: {}", table.rows.len());
            }
        }
        ".version" => {
            println!("MirseoDB Version 0.1.0");
            println!("Lightweight high-performance database system");
        }
        _ => {
            println!("Unknown meta command: {}", command);
            println!("Available meta commands:");
            println!("  .tables  - Show all tables");
            println!("  .schema  - Show database schema");
            println!("  .version - Show version information");
        }
    }
}

fn show_help() {
    println!("=== MirseoDB Help ===");
    println!("Basic SQL commands:");
    println!("  CREATE TABLE table_name (column1 type, column2 type, ...)");
    println!("  INSERT INTO table_name (col1, col2) VALUES (val1, val2)");
    println!("  SELECT col1, col2 FROM table_name [WHERE condition]");
    println!("  UPDATE table_name SET col1=val1 [WHERE condition]");
    println!("  DELETE FROM table_name [WHERE condition]");
    println!();
    println!("Supported data types:");
    println!("  INTEGER, INT, BIGINT, SMALLINT - integers");
    println!("  FLOAT, DOUBLE, REAL, DECIMAL - floating point");
    println!("  VARCHAR, TEXT, CHAR, STRING - text");
    println!("  BOOLEAN, BOOL, BIT - boolean");
    println!();
    println!("Meta commands (start with dot):");
    println!("  .tables  - List all tables");
    println!("  .schema  - Show database schema");
    println!("  .version - Show version information");
    println!();
    println!("Other commands:");
    println!("  help  - Show this help");
    println!("  clear - Clear screen");
    println!("  exit, quit - Exit program");
    println!();
    println!("Examples:");
    println!("  CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)");
    println!("  INSERT INTO users (id, name) VALUES (1, 'Alice')");
    println!("  SELECT * FROM users WHERE id = 1");
}

fn clear_screen() {
    print!("\x1B[2J\x1B[1;1H");
    io::stdout().flush().unwrap();
}
