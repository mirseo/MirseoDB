use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use super::types::DatabaseError;

const CONFIG_DIR: &str = ".mirseoDB";
const CONFIG_FILE: &str = "config.cfg";
pub const SQL_INJECTION_KEY: &str = "SQL_INJECTON_PROTECT";

#[derive(Clone, Debug)]
pub struct ConfigOptions {
    pub sql_injection_protect: bool,
}

impl Default for ConfigOptions {
    fn default() -> Self {
        Self {
            sql_injection_protect: true,
        }
    }
}

pub struct ConfigManager;

impl ConfigManager {
    fn config_dir() -> PathBuf {
        Path::new(CONFIG_DIR).to_path_buf()
    }

    fn config_path() -> PathBuf {
        Self::config_dir().join(CONFIG_FILE)
    }

    pub fn ensure_exists() -> Result<(), DatabaseError> {
        let dir = Self::config_dir();
        if !dir.exists() {
            fs::create_dir_all(&dir).map_err(|e| {
                DatabaseError::IoError(format!("Failed to create config dir: {}", e))
            })?;
        }

        let path = Self::config_path();
        if !path.exists() {
            let mut file = File::create(&path).map_err(|e| {
                DatabaseError::IoError(format!("Failed to create config file: {}", e))
            })?;

            let default_content = format!("{}=1\n", SQL_INJECTION_KEY);
            file.write_all(default_content.as_bytes()).map_err(|e| {
                DatabaseError::IoError(format!("Failed to write default config: {}", e))
            })?;
        }

        Ok(())
    }

    pub fn load() -> ConfigOptions {
        let path = Self::config_path();
        let mut contents = String::new();

        if File::open(&path)
            .and_then(|mut file| file.read_to_string(&mut contents))
            .is_err()
        {
            return ConfigOptions::default();
        }

        let enabled = contents
            .lines()
            .filter_map(|line| parse_key_value(line))
            .find_map(|(key, value)| {
                if key.eq_ignore_ascii_case(SQL_INJECTION_KEY) {
                    Some(parse_bool_flag(&value))
                } else {
                    None
                }
            })
            .unwrap_or(true);

        ConfigOptions {
            sql_injection_protect: enabled,
        }
    }
}

fn parse_key_value(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let mut parts = trimmed.splitn(2, '=');
    let key = parts.next()?.trim();
    let value = parts.next()?.trim();

    if key.is_empty() {
        return None;
    }

    Some((key.to_string(), value.to_string()))
}

fn parse_bool_flag(value: &str) -> bool {
    match value.trim() {
        "1" => true,
        "0" => false,
        other if other.eq_ignore_ascii_case("true") => true,
        other if other.eq_ignore_ascii_case("false") => false,
        _ => true,
    }
}
