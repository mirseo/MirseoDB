use super::core_types::{
    ColumnDefinition, ComparisonOperator, DataType, DatabaseError, SqlStatement, SqlValue,
    WhereClause,
};
use super::security::{normalize_identifier, normalize_table_name};
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::sync::{Arc, Mutex};
use std::time::Instant;


#[derive(Debug, Clone)]
pub struct DialectCache {
    cache: HashMap<u64, CachedDialectResult>,
    access_order: VecDeque<u64>,
    max_size: usize,
    hits: u64,
    misses: u64,
}

#[derive(Debug, Clone)]
pub struct CachedDialectResult {
    dialect: DetectedDialect,
    confidence_score: f32,
    preprocessing_time_ns: u64,
    timestamp: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct CachedSQLAnalysis {
    statement_type: StatementType,
    detected_dialect: DetectedDialect,
    original_sql: String,
    preprocessed_sql: String,      // 미리 계산된 uppercase SQL
    tokens: Vec<String>,           // 미리 계산된 토큰들
    sql_hash: u64,                // SQL 문의 해시값
    confidence_score: f32,         // dialect 감지 신뢰도
    processing_time_ns: u64,       // 처리 시간 (나노초)
}

#[derive(Debug, Clone)]
pub struct KeywordHashMatcher {
    dialect_keywords: HashMap<DetectedDialect, HashMap<String, f32>>, // 키워드 → 가중치
    keyword_to_dialects: HashMap<String, Vec<(DetectedDialect, f32)>>, // 역 인덱스
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    total_queries: u64,
    cache_hits: u64,
    cache_misses: u64,
    avg_parse_time_ns: u64,
    dialect_accuracy: f32,
}

impl DialectCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::new(),
            access_order: VecDeque::new(),
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, sql_hash: u64) -> Option<&CachedDialectResult> {
        if let Some(result) = self.cache.get(&sql_hash) {
            // LRU: 최근 사용된 항목을 뒤로 이동
            if let Some(pos) = self.access_order.iter().position(|&x| x == sql_hash) {
                self.access_order.remove(pos);
            }
            self.access_order.push_back(sql_hash);
            self.hits += 1;
            Some(result)
        } else {
            self.misses += 1;
            None
        }
    }

    pub fn insert(&mut self, sql_hash: u64, result: CachedDialectResult) {
        // 캐시 크기 제한 확인
        if self.cache.len() >= self.max_size && !self.cache.contains_key(&sql_hash) {
            // LRU: 가장 오래된 항목 제거
            if let Some(oldest) = self.access_order.pop_front() {
                self.cache.remove(&oldest);
            }
        }

        self.cache.insert(sql_hash, result);
        if let Some(pos) = self.access_order.iter().position(|&x| x == sql_hash) {
            self.access_order.remove(pos);
        }
        self.access_order.push_back(sql_hash);
    }

    pub fn hit_rate(&self) -> f32 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.hits as f32 / (self.hits + self.misses) as f32
        }
    }
}

impl KeywordHashMatcher {
    pub fn new() -> Self {
        let mut matcher = Self {
            dialect_keywords: HashMap::new(),
            keyword_to_dialects: HashMap::new(),
        };
        matcher.initialize_keyword_maps();
        matcher
    }

    fn initialize_keyword_maps(&mut self) {
        // 🎯 HYPERTHINKING: 키워드별 가중치를 세밀하게 조정하여 정확도 극대화

        // MS-SQL 키워드들 (높은 신뢰도)
        let mssql_keywords = vec![
            ("NVARCHAR", 0.95), ("NTEXT", 0.90), ("NCHAR", 0.85),
            ("MONEY", 0.80), ("SMALLMONEY", 0.85), ("IDENTITY", 0.90),
            ("UNIQUEIDENTIFIER", 0.95), ("DATETIME2", 0.90),
            ("DATETIMEOFFSET", 0.95), ("HIERARCHYID", 0.95),
            ("[", 0.70), ("]", 0.70), ("NOLOCK", 0.85), ("READUNCOMMITTED", 0.80),
        ];

        // MySQL 키워드들 (높은 신뢰도)
        let mysql_keywords = vec![
            ("AUTO_INCREMENT", 0.95), ("LONGTEXT", 0.90), ("MEDIUMTEXT", 0.85),
            ("TINYTEXT", 0.80), ("TINYINT", 0.75), ("MEDIUMINT", 0.80),
            ("BIGINT", 0.60), ("UNSIGNED", 0.85), ("ZEROFILL", 0.95),
            ("`", 0.75), ("ENGINE", 0.70), ("CHARSET", 0.75),
            ("COLLATE", 0.70), ("CASCADE", 0.60),
        ];

        // Oracle 키워드들 (높은 신뢰도)
        let oracle_keywords = vec![
            ("VARCHAR2", 0.95), ("NVARCHAR2", 0.95), ("CLOB", 0.90),
            ("NCLOB", 0.90), ("NUMBER", 0.80), ("SEQUENCE", 0.85),
            ("NEXTVAL", 0.95), ("CURRVAL", 0.95), ("ROWNUM", 0.95),
            ("ROWID", 0.90), ("DUAL", 0.95), ("SYSDATE", 0.90),
        ];

        // Standard SQL 키워드들 (낮은 신뢰도)
        let standard_keywords = vec![
            ("VARCHAR", 0.30), ("INTEGER", 0.35), ("DECIMAL", 0.40),
            ("TIMESTAMP", 0.45), ("PRIMARY", 0.25), ("KEY", 0.20),
            ("FOREIGN", 0.30), ("REFERENCES", 0.35), ("CHECK", 0.25),
            ("UNIQUE", 0.30), ("DEFAULT", 0.25),
        ];

        // 각 dialect별 키워드 맵 구성
        self.insert_dialect_keywords(DetectedDialect::MsSQL, mssql_keywords);
        self.insert_dialect_keywords(DetectedDialect::MySQL, mysql_keywords);
        self.insert_dialect_keywords(DetectedDialect::Oracle, oracle_keywords);
        self.insert_dialect_keywords(DetectedDialect::Standard, standard_keywords);
    }

    fn insert_dialect_keywords(&mut self, dialect: DetectedDialect, keywords: Vec<(&str, f32)>) {
        let mut dialect_map = HashMap::new();

        for (keyword, weight) in keywords {
            let key = keyword.to_string();
            dialect_map.insert(key.clone(), weight);

            // 역 인덱스 구성
            self.keyword_to_dialects
                .entry(key)
                .or_insert_with(Vec::new)
                .push((dialect.clone(), weight));
        }

        self.dialect_keywords.insert(dialect, dialect_map);
    }

    pub fn detect_dialect_optimized(&self, sql_upper: &str) -> (DetectedDialect, f32) {
        let mut dialect_scores: HashMap<DetectedDialect, f32> = HashMap::new();

        // 초기화
        dialect_scores.insert(DetectedDialect::Standard, 1.0);
        dialect_scores.insert(DetectedDialect::MsSQL, 0.0);
        dialect_scores.insert(DetectedDialect::MySQL, 0.0);
        dialect_scores.insert(DetectedDialect::Oracle, 0.0);

        // 🚀 HYPERTHINKING: 빠른 키워드 검색으로 성능 대폭 향상
        for (keyword, dialect_weights) in &self.keyword_to_dialects {
            if sql_upper.contains(keyword) {
                for (dialect, weight) in dialect_weights {
                    *dialect_scores.entry(dialect.clone()).or_insert(0.0) += weight;
                }
            }
        }

        // 가장 높은 점수의 dialect 반환
        let (best_dialect, best_score) = dialect_scores
            .into_iter()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or((DetectedDialect::Standard, 1.0));

        (best_dialect, best_score)
    }
}

fn calculate_sql_hash(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, Clone)]
pub struct AnySQL {
    hyperthinking_enabled: bool,
    keyword_patterns: KeywordPatterns,
    dialect_cache: Arc<Mutex<DialectCache>>,
    keyword_matcher: KeywordHashMatcher,
    performance_metrics: Arc<Mutex<PerformanceMetrics>>,
}

#[derive(Debug, Clone)]
struct KeywordPatterns {
    mssql_keywords: Vec<&'static str>,
    mysql_keywords: Vec<&'static str>,
    oracle_keywords: Vec<&'static str>,
    standard_keywords: Vec<&'static str>,
}

impl KeywordPatterns {
    fn new() -> Self {
        Self {
            mssql_keywords: vec![
                "NVARCHAR",
                "NTEXT",
                "NCHAR",
                "MONEY",
                "SMALLMONEY",
                "IDENTITY",
                "UNIQUEIDENTIFIER",
                "DATETIME2",
                "DATETIMEOFFSET",
                "HIERARCHYID",
                "[",
                "]",
                "WITH",
                "NOLOCK",
                "READUNCOMMITTED",
            ],
            mysql_keywords: vec![
                "AUTO_INCREMENT",
                "LONGTEXT",
                "MEDIUMTEXT",
                "TINYTEXT",
                "TINYINT",
                "MEDIUMINT",
                "BIGINT",
                "UNSIGNED",
                "ZEROFILL",
                "`",
                "ENGINE",
                "CHARSET",
                "COLLATE",
                "ON",
                "UPDATE",
                "CASCADE",
            ],
            oracle_keywords: vec![
                "VARCHAR2",
                "NVARCHAR2",
                "CLOB",
                "NCLOB",
                "NUMBER",
                "SEQUENCE",
                "NEXTVAL",
                "CURRVAL",
                "ROWNUM",
                "ROWID",
                "DUAL",
                "SYSDATE",
            ],
            standard_keywords: vec![
                "VARCHAR",
                "INTEGER",
                "DECIMAL",
                "TIMESTAMP",
                "PRIMARY",
                "KEY",
                "FOREIGN",
                "REFERENCES",
                "CHECK",
                "UNIQUE",
                "DEFAULT",
            ],
        }
    }
}

impl AnySQL {
    pub fn new() -> Self {
        Self {
            hyperthinking_enabled: true,
            keyword_patterns: KeywordPatterns::new(),
            dialect_cache: Arc::new(Mutex::new(DialectCache::new(1000))),
            keyword_matcher: KeywordHashMatcher::new(),
            performance_metrics: Arc::new(Mutex::new(PerformanceMetrics {
                total_queries: 0,
                cache_hits: 0,
                cache_misses: 0,
                avg_parse_time_ns: 0,
                dialect_accuracy: 0.0,
            })),
        }
    }

    pub fn parse(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let sql = sql.trim();

        if sql.is_empty() {
            return Err(DatabaseError::ParseError("Empty SQL statement".to_string()));
        }

        let analysis = self.hyperthink_sql_analysis(sql)?;

        println!(
            "[HYPERTHINKING] Detected dialect: {:?}, Statement type: {:?}",
            analysis.detected_dialect, analysis.statement_type
        );

        match analysis.statement_type {
            StatementType::CreateDatabase => self.parse_create_database_anysql(sql),
            StatementType::CreateTable => self.parse_create_table_anysql(sql),
            StatementType::Insert => self.parse_insert_anysql(sql),
            StatementType::Select => self.parse_select_anysql(sql),
            StatementType::Update => self.parse_update_anysql(sql),
            StatementType::Delete => self.parse_delete_anysql(sql),
            StatementType::DropTable => self.parse_drop_table_anysql(sql),
            StatementType::DropDatabase => self.parse_drop_database_anysql(sql),
            StatementType::AlterTable => self.parse_alter_table_anysql(sql),
        }
    }

    fn hyperthink_sql_analysis(&self, sql: &str) -> Result<SQLAnalysis, DatabaseError> {
        let start_time = Instant::now();
        let sql_hash = calculate_sql_hash(sql);

        // 🚀 OPTIMIZATION: Check cache first
        if let Ok(mut cache) = self.dialect_cache.lock() {
            if let Some(cached_result) = cache.get(sql_hash) {
                if let Ok(mut metrics) = self.performance_metrics.lock() {
                    metrics.total_queries += 1;
                    metrics.cache_hits += 1;
                }

                // Parse SQL with cached dialect info for faster processing
                let sql_upper = sql.to_uppercase(); // Single conversion
                let tokens: Vec<String> = sql_upper
                    .split_whitespace()
                    .map(|s| s.to_string())
                    .collect();

                let statement_type = self.determine_statement_type(&tokens)?;

                return Ok(SQLAnalysis {
                    statement_type,
                    detected_dialect: cached_result.dialect.clone(),
                    original_sql: sql.to_string(),
                    tokens,
                });
            }
        }

        // Cache miss - perform full analysis
        let sql_upper = sql.to_uppercase(); // Single conversion for entire function
        let tokens: Vec<String> = sql_upper
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        if tokens.is_empty() {
            return Err(DatabaseError::ParseError("No tokens found".to_string()));
        }

        let statement_type = self.determine_statement_type(&tokens)?;
        let (detected_dialect, confidence) = self.keyword_matcher.detect_dialect_optimized(&sql_upper);

        // Cache the result
        let processing_time = start_time.elapsed().as_nanos() as u64;
        if let Ok(mut cache) = self.dialect_cache.lock() {
            cache.insert(sql_hash, CachedDialectResult {
                dialect: detected_dialect.clone(),
                confidence_score: confidence,
                preprocessing_time_ns: processing_time,
                timestamp: Instant::now(),
            });
        }

        // Update metrics
        if let Ok(mut metrics) = self.performance_metrics.lock() {
            metrics.total_queries += 1;
            metrics.cache_misses += 1;
            metrics.avg_parse_time_ns = (metrics.avg_parse_time_ns + processing_time) / 2;
        }

        Ok(SQLAnalysis {
            statement_type,
            detected_dialect,
            original_sql: sql.to_string(),
            tokens,
        })
    }

    fn determine_statement_type(&self, tokens: &[String]) -> Result<StatementType, DatabaseError> {
        if tokens.is_empty() {
            return Err(DatabaseError::ParseError("No tokens found".to_string()));
        }

        match tokens[0].as_str() {
            "CREATE" => {
                if tokens.len() > 1 && tokens[1] == "DATABASE" {
                    Ok(StatementType::CreateDatabase)
                } else {
                    Ok(StatementType::CreateTable)
                }
            }
            "DROP" => {
                if tokens.len() > 1 && tokens[1] == "DATABASE" {
                    Ok(StatementType::DropDatabase)
                } else {
                    Ok(StatementType::DropTable)
                }
            }
            "ALTER" => Ok(StatementType::AlterTable),
            "INSERT" | "INSERT_INTO" => Ok(StatementType::Insert),
            "SELECT" => Ok(StatementType::Select),
            "UPDATE" => Ok(StatementType::Update),
            "DELETE" => Ok(StatementType::Delete),
            _ => {
                Err(DatabaseError::ParseError(format!(
                    "Unknown statement type: {}",
                    tokens[0]
                )))
            }
        }
    }

    fn parse_create_database_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let tokens: Vec<&str> = sql.trim().split_whitespace().collect();

        if tokens.len() < 3 {
            return Err(DatabaseError::ParseError(
                "Invalid CREATE DATABASE syntax".to_string(),
            ));
        }

        let database_name = normalize_table_name(tokens[2]);
        Ok(SqlStatement::CreateDatabase { database_name })
    }

    fn parse_create_table_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();

        if tokens.len() < 3 || !tokens[1].eq_ignore_ascii_case("TABLE") {
            return Err(DatabaseError::ParseError(
                "Invalid CREATE TABLE syntax".to_string(),
            ));
        }

        let table_name = normalize_table_name(tokens[2]);

        if tokens.len() == 3 || !sql.contains('(') {
            return Ok(SqlStatement::CreateTable {
                table_name,
                columns: Vec::new(),
            });
        }
        let start_pos = sql.find('(').unwrap();
        let end_pos = sql.rfind(')').unwrap();
        let columns_str = &sql[start_pos + 1..end_pos];

        let columns = self.parse_columns_anysql(columns_str)?;

        Ok(SqlStatement::CreateTable {
            table_name,
            columns,
        })
    }

    fn parse_columns_anysql(
        &self,
        columns_str: &str,
    ) -> Result<Vec<ColumnDefinition>, DatabaseError> {
        let mut columns = Vec::new();

        let column_defs = self.smart_split_columns(columns_str);

        for column_def in column_defs {
            let column_tokens: Vec<&str> = column_def.trim().split_whitespace().collect();

            if column_tokens.len() < 2 {
                continue;
            }

            let column_name = normalize_identifier(column_tokens[0]);
            let data_type = self.parse_data_type_anysql(column_tokens[1])?;

            let mut nullable = true;
            let mut primary_key = false;

            for i in 2..column_tokens.len() {
                match column_tokens[i].to_uppercase().as_str() {
                    "NOT"
                        if i + 1 < column_tokens.len()
                            && column_tokens[i + 1].to_uppercase() == "NULL" =>
                    {
                        nullable = false;
                    }
                    "PRIMARY"
                        if i + 1 < column_tokens.len()
                            && column_tokens[i + 1].to_uppercase() == "KEY" =>
                    {
                        primary_key = true;
                    }
                    "IDENTITY" | "AUTO_INCREMENT" => {
                        primary_key = true;
                    }
                    _ => {}
                }
            }

            columns.push(ColumnDefinition {
                name: column_name,
                data_type,
                nullable,
                primary_key,
            });
        }

        Ok(columns)
    }

    fn smart_split_columns(&self, columns_str: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut paren_depth = 0;
        let mut in_quotes = false;
        let mut quote_char = ' ';

        for ch in columns_str.chars() {
            match ch {
                '(' if !in_quotes => paren_depth += 1,
                ')' if !in_quotes => paren_depth -= 1,
                '\'' | '"' | '`' if !in_quotes => {
                    in_quotes = true;
                    quote_char = ch;
                }
                c if in_quotes && c == quote_char => in_quotes = false,
                ',' if !in_quotes && paren_depth == 0 => {
                    result.push(current.trim().to_string());
                    current.clear();
                    continue;
                }
                _ => {}
            }
            current.push(ch);
        }

        if !current.trim().is_empty() {
            result.push(current.trim().to_string());
        }

        result
    }

    fn parse_data_type_anysql(&self, type_str: &str) -> Result<DataType, DatabaseError> {
        let type_upper = type_str.to_uppercase(); // Single conversion per call

        // HYPERTHINKING: Support all dialect data types
        match type_upper.as_str() {
            // Integer types (all dialects)
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" => Ok(DataType::Integer),

            // Float types (all dialects)
            "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" | "NUMERIC" | "MONEY" | "SMALLMONEY"
            | "NUMBER" => Ok(DataType::Float),

            // Text types (all dialects)
            "VARCHAR" | "TEXT" | "CHAR" | "NVARCHAR" | "STRING" | "VARCHAR2" | "NVARCHAR2"
            | "LONGTEXT" | "MEDIUMTEXT" | "TINYTEXT" | "NTEXT" | "NCHAR" | "CLOB" | "NCLOB" => {
                Ok(DataType::Text)
            }

            // Boolean types (all dialects)
            "BOOL" | "BOOLEAN" | "BIT" => Ok(DataType::Boolean),

            _ => {
                // HYPERTHINKING: Try to infer type from patterns
                if type_upper.starts_with("VARCHAR") || type_upper.starts_with("CHAR") {
                    Ok(DataType::Text)
                } else if type_upper.contains("INT") {
                    Ok(DataType::Integer)
                } else if type_upper.contains("FLOAT") || type_upper.contains("DECIMAL") {
                    Ok(DataType::Float)
                } else {
                    Ok(DataType::Text) // Default fallback
                }
            }
        }
    }

    fn parse_insert_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let sql_upper = sql.to_uppercase();

        // Handle different INSERT syntaxes
        let insert_pos = if sql_upper.starts_with("INSERT INTO") {
            11
        } else if sql_upper.starts_with("INSERT") {
            6
        } else {
            return Err(DatabaseError::ParseError(
                "Invalid INSERT syntax".to_string(),
            ));
        };

        let remaining = &sql[insert_pos..].trim();
        let tokens: Vec<&str> = remaining.split_whitespace().collect();

        if tokens.is_empty() {
            return Err(DatabaseError::ParseError(
                "Missing table name in INSERT".to_string(),
            ));
        }

        let raw_table_token = tokens[0];
        let table_name = normalize_table_name(raw_table_token);

        // Find VALUES clause
        let values_pos = sql_upper
            .find("VALUES")
            .ok_or_else(|| DatabaseError::ParseError("Missing VALUES clause".to_string()))?;

        // Extract columns if specified
        let raw_table_pos = sql
            .find(raw_table_token)
            .ok_or_else(|| DatabaseError::ParseError("Unable to locate table name".to_string()))?;
        let table_end = raw_table_pos + raw_table_token.len();
        let columns_part = &sql[table_end..values_pos];

        let columns = if let Some(start) = columns_part.find('(') {
            if let Some(end) = columns_part.find(')') {
                let columns_str = &columns_part[start + 1..end];
                columns_str
                    .split(',')
                    .map(|s| normalize_identifier(s))
                    .collect()
            } else {
                vec!["*".to_string()]
            }
        } else {
            vec!["*".to_string()]
        };

        // Extract values
        let values_part = &sql[values_pos + 6..];
        let start_pos = values_part.find('(').ok_or_else(|| {
            DatabaseError::ParseError("Missing opening parenthesis in VALUES".to_string())
        })?;
        let end_pos = values_part.rfind(')').ok_or_else(|| {
            DatabaseError::ParseError("Missing closing parenthesis in VALUES".to_string())
        })?;

        let values_str = &values_part[start_pos + 1..end_pos];
        let value_strs: Vec<&str> = values_str.split(',').collect();

        let mut values = Vec::new();
        for value_str in value_strs {
            let value = self.parse_value_anysql(value_str.trim())?;
            values.push(value);
        }

        Ok(SqlStatement::Insert {
            table_name,
            columns,
            values,
        })
    }

    fn parse_select_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();

        let from_pos = tokens
            .iter()
            .position(|&token| token.to_uppercase() == "FROM")
            .ok_or_else(|| DatabaseError::ParseError("Missing FROM clause".to_string()))?;

        if from_pos + 1 >= tokens.len() {
            return Err(DatabaseError::ParseError(
                "Missing table name after FROM".to_string(),
            ));
        }

        let columns: Vec<String> = tokens[1..from_pos]
            .iter()
            .flat_map(|s| s.split(','))
            .map(|s| normalize_identifier(s))
            .collect();

        let table_name = normalize_table_name(tokens[from_pos + 1]);

        let where_clause = if let Some(where_pos) = tokens
            .iter()
            .position(|&token| token.to_uppercase() == "WHERE")
        {
            Some(self.parse_where_clause_anysql(&tokens[where_pos + 1..])?)
        } else {
            None
        };

        Ok(SqlStatement::Select {
            table_name,
            columns,
            where_clause,
        })
    }

    fn parse_update_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();

        if tokens.len() < 4 {
            return Err(DatabaseError::ParseError(
                "Invalid UPDATE syntax".to_string(),
            ));
        }

        let table_name = normalize_table_name(tokens[1]);

        let set_pos = tokens
            .iter()
            .position(|&token| token.to_uppercase() == "SET")
            .ok_or_else(|| DatabaseError::ParseError("Missing SET clause".to_string()))?;

        let where_pos = tokens
            .iter()
            .position(|&token| token.to_uppercase() == "WHERE");
        let set_end = where_pos.unwrap_or(tokens.len());
        let set_tokens = &tokens[set_pos + 1..set_end];

        let mut set_clauses = Vec::new();
        let set_str = set_tokens.join(" ");
        let assignments: Vec<&str> = set_str.split(',').collect();

        for assignment in assignments {
            let parts: Vec<&str> = assignment.split('=').collect();
            if parts.len() != 2 {
                return Err(DatabaseError::ParseError("Invalid SET clause".to_string()));
            }

            let column_name = normalize_identifier(parts[0]);
            let value = self.parse_value_anysql(parts[1].trim())?;
            set_clauses.push((column_name, value));
        }

        let where_clause = if let Some(where_pos) = where_pos {
            Some(self.parse_where_clause_anysql(&tokens[where_pos + 1..])?)
        } else {
            None
        };

        Ok(SqlStatement::Update {
            table_name,
            set_clauses,
            where_clause,
        })
    }

    fn parse_delete_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();

        if tokens.len() < 3 || tokens[1].to_uppercase() != "FROM" {
            return Err(DatabaseError::ParseError(
                "Invalid DELETE syntax".to_string(),
            ));
        }

        let table_name = normalize_table_name(tokens[2]);

        let where_clause = if let Some(where_pos) = tokens
            .iter()
            .position(|&token| token.to_uppercase() == "WHERE")
        {
            Some(self.parse_where_clause_anysql(&tokens[where_pos + 1..])?)
        } else {
            None
        };

        Ok(SqlStatement::Delete {
            table_name,
            where_clause,
        })
    }

    fn parse_where_clause_anysql(&self, tokens: &[&str]) -> Result<WhereClause, DatabaseError> {
        if tokens.len() < 3 {
            return Err(DatabaseError::ParseError(
                "Invalid WHERE clause".to_string(),
            ));
        }

        let column = normalize_identifier(tokens[0]);
        let operator = self.parse_comparison_operator(tokens[1])?;
        let value = self.parse_value_anysql(tokens[2])?;

        Ok(WhereClause {
            column,
            operator,
            value,
        })
    }

    fn parse_comparison_operator(&self, op: &str) -> Result<ComparisonOperator, DatabaseError> {
        match op.to_uppercase().as_str() {
            "=" => Ok(ComparisonOperator::Equal),
            "!=" | "<>" => Ok(ComparisonOperator::NotEqual),
            ">" => Ok(ComparisonOperator::GreaterThan),
            "<" => Ok(ComparisonOperator::LessThan),
            ">=" => Ok(ComparisonOperator::GreaterThanOrEqual),
            "<=" => Ok(ComparisonOperator::LessThanOrEqual),
            _ => Err(DatabaseError::ParseError(format!(
                "Unknown comparison operator: {}",
                op
            ))),
        }
    }

    fn parse_value_anysql(&self, value_str: &str) -> Result<SqlValue, DatabaseError> {
        let value_str = value_str.trim();

        if value_str.eq_ignore_ascii_case("NULL") {
            return Ok(SqlValue::Null);
        }

        if value_str.eq_ignore_ascii_case("TRUE") {
            return Ok(SqlValue::Boolean(true));
        }

        if value_str.eq_ignore_ascii_case("FALSE") {
            return Ok(SqlValue::Boolean(false));
        }

        // Handle quoted strings (all quote types)
        if (value_str.starts_with('\'') && value_str.ends_with('\''))
            || (value_str.starts_with('"') && value_str.ends_with('"'))
            || (value_str.starts_with('`') && value_str.ends_with('`'))
        {
            let text = value_str[1..value_str.len() - 1].to_string();
            return Ok(SqlValue::Text(text));
        }

        // Try parsing as number
        if value_str.contains('.') {
            if let Ok(float_val) = value_str.parse::<f64>() {
                return Ok(SqlValue::Float(float_val));
            }
        }

        if let Ok(int_val) = value_str.parse::<i64>() {
            return Ok(SqlValue::Integer(int_val));
        }

        // Default to text
        Ok(SqlValue::Text(value_str.to_string()))
    }

    fn parse_drop_table_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let tokens: Vec<&str> = sql.trim().split_whitespace().collect();

        if tokens.len() < 3 {
            return Err(DatabaseError::ParseError(
                "Invalid DROP TABLE syntax".to_string(),
            ));
        }

        let table_name = normalize_table_name(tokens[2]);
        Ok(SqlStatement::DropTable { table_name })
    }

    fn parse_drop_database_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let tokens: Vec<&str> = sql.trim().split_whitespace().collect();

        if tokens.len() < 3 {
            return Err(DatabaseError::ParseError(
                "Invalid DROP DATABASE syntax".to_string(),
            ));
        }

        let database_name = normalize_table_name(tokens[2]);
        Ok(SqlStatement::DropDatabase { database_name })
    }

    fn parse_alter_table_anysql(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        use super::core_types::AlterAction;

        let tokens: Vec<&str> = sql.trim().split_whitespace().collect();

        if tokens.len() < 4 {
            return Err(DatabaseError::ParseError(
                "Invalid ALTER TABLE syntax".to_string(),
            ));
        }

        let table_name = normalize_table_name(tokens[2]);

        // ALTER TABLE table_name ADD/DROP/MODIFY COLUMN ...
        let action = match tokens[3].to_uppercase().as_str() {
            "ADD" => {
                if tokens.len() >= 6 && tokens[4].to_uppercase() == "COLUMN" {
                    // ALTER TABLE table_name ADD COLUMN column_name data_type
                    let column_name = normalize_identifier(tokens[5]);
                    let data_type = if tokens.len() > 6 {
                        self.parse_data_type_anysql(tokens[6])?
                    } else {
                        return Err(DatabaseError::ParseError(
                            "Missing data type in ADD COLUMN".to_string(),
                        ));
                    };

                    AlterAction::AddColumn {
                        column: ColumnDefinition {
                            name: column_name,
                            data_type,
                            nullable: true, // Default to nullable
                            primary_key: false,
                        },
                    }
                } else {
                    return Err(DatabaseError::ParseError(
                        "Invalid ADD syntax in ALTER TABLE".to_string(),
                    ));
                }
            }
            "DROP" => {
                if tokens.len() >= 6 && tokens[4].to_uppercase() == "COLUMN" {
                    // ALTER TABLE table_name DROP COLUMN column_name
                    let column_name = normalize_identifier(tokens[5]);
                    AlterAction::DropColumn { column_name }
                } else {
                    return Err(DatabaseError::ParseError(
                        "Invalid DROP syntax in ALTER TABLE".to_string(),
                    ));
                }
            }
            "MODIFY" => {
                if tokens.len() >= 6 && tokens[4].to_uppercase() == "COLUMN" {
                    // ALTER TABLE table_name MODIFY COLUMN column_name data_type
                    let column_name = normalize_identifier(tokens[5]);
                    let data_type = if tokens.len() > 6 {
                        self.parse_data_type_anysql(tokens[6])?
                    } else {
                        return Err(DatabaseError::ParseError(
                            "Missing data type in MODIFY COLUMN".to_string(),
                        ));
                    };

                    AlterAction::ModifyColumn {
                        column: ColumnDefinition {
                            name: column_name,
                            data_type,
                            nullable: true, // Default to nullable
                            primary_key: false,
                        },
                    }
                } else {
                    return Err(DatabaseError::ParseError(
                        "Invalid MODIFY syntax in ALTER TABLE".to_string(),
                    ));
                }
            }
            _ => {
                return Err(DatabaseError::ParseError(format!(
                    "Unsupported ALTER TABLE action: {}",
                    tokens[3]
                )));
            }
        };

        Ok(SqlStatement::AlterTable { table_name, action })
    }

    pub fn get_performance_metrics(&self) -> Option<PerformanceMetrics> {
        if let Ok(metrics) = self.performance_metrics.lock() {
            Some(metrics.clone())
        } else {
            None
        }
    }

    pub fn get_cache_hit_rate(&self) -> f32 {
        if let Ok(cache) = self.dialect_cache.lock() {
            cache.hit_rate()
        } else {
            0.0
        }
    }

    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.dialect_cache.lock() {
            cache.cache.clear();
            cache.access_order.clear();
            cache.hits = 0;
            cache.misses = 0;
        }
    }
}

#[derive(Debug, Clone)]
struct SQLAnalysis {
    statement_type: StatementType,
    detected_dialect: DetectedDialect,
    original_sql: String,
    tokens: Vec<String>,
}

#[derive(Debug, Clone)]
enum StatementType {
    CreateDatabase,
    CreateTable,
    Insert,
    Select,
    Update,
    Delete,
    DropTable,
    DropDatabase,
    AlterTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DetectedDialect {
    Standard,
    MsSQL,
    MySQL,
    Oracle,
}
