use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub emails: HashMap<String, String>,
    pub perm_manager: Vec<String>,
    pub perms: HashMap<String, PermissionGroup>,
    pub setup_completed: bool,
    pub admin_email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionGroup {
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        let emails = HashMap::new(); // 초기에는 빈 상태로 시작
        let perm_manager = Vec::new();

        let mut perms = HashMap::new();
        perms.insert(
            "admin".to_string(),
            PermissionGroup {
                allow: vec!["*".to_string()],
                deny: vec![],
            },
        );
        perms.insert(
            "user".to_string(),
            PermissionGroup {
                allow: vec!["SELECT".to_string(), "SHOW".to_string(), "INSERT".to_string()],
                deny: vec![
                    "DROP".to_string(),
                    "DELETE".to_string(),
                    "ALTER".to_string(),
                ],
            },
        );

        AuthConfig {
            emails,
            perm_manager,
            perms,
            setup_completed: false,
            admin_email: None,
        }
    }
}

impl AuthConfig {
    pub fn load() -> Result<Self, String> {
        let config_path = Path::new(".mirseoDB/auth_config.json");

        if !config_path.exists() {
            return Ok(Self::default());
        }

        let content = fs::read_to_string(config_path)
            .map_err(|e| format!("Failed to read auth config: {}", e))?;

        serde_json::from_str(&content).map_err(|e| format!("Failed to parse auth config: {}", e))
    }

    pub fn save(&self) -> Result<(), String> {
        let config_dir = Path::new(".mirseoDB");
        if !config_dir.exists() {
            fs::create_dir_all(config_dir)
                .map_err(|e| format!("Failed to create config directory: {}", e))?;
        }

        let config_path = config_dir.join("auth_config.json");
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize auth config: {}", e))?;

        fs::write(config_path, content).map_err(|e| format!("Failed to write auth config: {}", e))
    }

    pub fn ensure_exists() -> Result<(), String> {
        let config_path = Path::new(".mirseoDB/auth_config.json");
        if !config_path.exists() {
            let default_config = Self::default();
            default_config.save()?;
            println!("[MirseoDB][Auth] Created default authentication configuration at .mirseoDB/auth_config.json");
            println!("[MirseoDB][Auth] Database setup required - please complete initial setup");
        }
        Ok(())
    }

    pub fn is_setup_completed(&self) -> bool {
        self.setup_completed
    }

    pub fn complete_setup(&mut self, admin_email: String) -> Result<(), String> {
        if self.setup_completed {
            return Err("Setup already completed".to_string());
        }

        // 관리자 계정 생성
        self.emails.insert(admin_email.clone(), "admin".to_string());
        self.perm_manager.push(admin_email.clone());
        self.admin_email = Some(admin_email);
        self.setup_completed = true;

        self.save()?;
        Ok(())
    }

    pub fn add_user(&mut self, email: String, role: String) -> Result<(), String> {
        if !self.setup_completed {
            return Err("Setup not completed yet".to_string());
        }

        if !self.perms.contains_key(&role) {
            return Err(format!("Unknown role: {}", role));
        }

        self.emails.insert(email, role);
        self.save()?;
        Ok(())
    }

    pub fn get_user_role(&self, email: &str) -> Option<&str> {
        self.emails.get(email).map(|s| s.as_str())
    }

    pub fn is_permission_manager(&self, email: &str) -> bool {
        self.perm_manager.contains(&email.to_string())
    }

    pub fn check_sql_permission(&self, email: &str, sql_statement: &str) -> bool {
        let role = self.get_user_role(email).unwrap_or("default");

        if let Some(perms) = self.perms.get(role) {
            let sql_upper = sql_statement.trim().to_uppercase();
            let operation = extract_sql_operation(&sql_upper);

            // 먼저 deny 리스트 확인
            for deny_pattern in &perms.deny {
                if matches_sql_pattern(&sql_upper, deny_pattern) {
                    return false;
                }
            }

            // allow 리스트 확인
            for allow_pattern in &perms.allow {
                if matches_sql_pattern(&sql_upper, allow_pattern) {
                    return true;
                }
            }

            // 기본적으로 거부
            false
        } else {
            // 알 수 없는 역할은 기본 권한으로 처리
            if let Some(default_perms) = self.perms.get("default") {
                let sql_upper = sql_statement.trim().to_uppercase();

                for deny_pattern in &default_perms.deny {
                    if matches_sql_pattern(&sql_upper, deny_pattern) {
                        return false;
                    }
                }

                for allow_pattern in &default_perms.allow {
                    if matches_sql_pattern(&sql_upper, allow_pattern) {
                        return true;
                    }
                }
            }
            false
        }
    }
}

fn extract_sql_operation(sql: &str) -> &str {
    let words: Vec<&str> = sql.split_whitespace().collect();
    if words.is_empty() {
        return "";
    }

    match words[0] {
        "SELECT" => "SELECT",
        "INSERT" => "INSERT",
        "UPDATE" => "UPDATE",
        "DELETE" => "DELETE",
        "CREATE" => {
            if words.len() > 1 {
                match words[1] {
                    "TABLE" => "CREATE TABLE",
                    "DATABASE" => "CREATE DATABASE",
                    "INDEX" => "CREATE INDEX",
                    _ => "CREATE",
                }
            } else {
                "CREATE"
            }
        }
        "DROP" => {
            if words.len() > 1 {
                match words[1] {
                    "TABLE" => "DROP TABLE",
                    "DATABASE" => "DROP DATABASE",
                    "INDEX" => "DROP INDEX",
                    _ => "DROP",
                }
            } else {
                "DROP"
            }
        }
        "ALTER" => {
            if words.len() > 1 {
                match words[1] {
                    "TABLE" => "ALTER TABLE",
                    _ => "ALTER",
                }
            } else {
                "ALTER"
            }
        }
        "SHOW" => "SHOW",
        "DESCRIBE" | "DESC" => "DESCRIBE",
        _ => words[0],
    }
}

fn matches_sql_pattern(sql: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let pattern_upper = pattern.to_uppercase();
    let operation = extract_sql_operation(sql);

    // 정확한 매치
    if operation == pattern_upper {
        return true;
    }

    // 패턴이 와일드카드를 포함하는 경우
    if pattern_upper.contains('*') {
        let pattern_parts: Vec<&str> = pattern_upper.split('*').collect();
        if pattern_parts.len() == 2 {
            let prefix = pattern_parts[0];
            let suffix = pattern_parts[1];

            if prefix.is_empty() {
                return operation.ends_with(suffix);
            } else if suffix.is_empty() {
                return operation.starts_with(prefix);
            } else {
                return operation.starts_with(prefix) && operation.ends_with(suffix);
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_operation_extraction() {
        assert_eq!(extract_sql_operation("SELECT * FROM users"), "SELECT");
        assert_eq!(extract_sql_operation("CREATE TABLE test"), "CREATE TABLE");
        assert_eq!(extract_sql_operation("DROP DATABASE test"), "DROP DATABASE");
        assert_eq!(extract_sql_operation("INSERT INTO users"), "INSERT");
        assert_eq!(extract_sql_operation("UPDATE users SET"), "UPDATE");
        assert_eq!(extract_sql_operation("DELETE FROM users"), "DELETE");
    }

    #[test]
    fn test_pattern_matching() {
        assert!(matches_sql_pattern("SELECT * FROM users", "*"));
        assert!(matches_sql_pattern("SELECT * FROM users", "SELECT"));
        assert!(matches_sql_pattern("CREATE TABLE test", "CREATE TABLE"));
        assert!(matches_sql_pattern("DROP TABLE test", "DROP*"));
        assert!(!matches_sql_pattern("SELECT * FROM users", "DROP"));
        assert!(!matches_sql_pattern("CREATE INDEX test", "CREATE TABLE"));
    }

    #[test]
    fn test_permission_check() {
        let config = AuthConfig::default();

        // restricted_user 테스트
        assert!(config.check_sql_permission("user@example.com", "SELECT * FROM users"));
        assert!(!config.check_sql_permission("user@example.com", "DROP TABLE users"));
        assert!(!config.check_sql_permission("user@example.com", "DELETE FROM users"));

        // power_user 테스트
        assert!(config.check_sql_permission("admin@example.com", "SELECT * FROM users"));
        assert!(config.check_sql_permission("admin@example.com", "INSERT INTO users"));
        assert!(!config.check_sql_permission("admin@example.com", "DROP TABLE users"));
        assert!(!config.check_sql_permission("admin@example.com", "DROP DATABASE test"));
    }
}
