use super::security::{normalize_identifier, normalize_table_name};
use super::core_types::{
    ColumnDefinition, ComparisonOperator, DataType, DatabaseError, SqlStatement, SqlValue,
    WhereClause,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct AnySQL {
    hyperthinking_enabled: bool,
    keyword_patterns: KeywordPatterns,
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
        }
    }

    fn hyperthink_sql_analysis(&self, sql: &str) -> Result<SQLAnalysis, DatabaseError> {
        let sql_upper = sql.to_uppercase();
        let tokens: Vec<String> = sql_upper
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        if tokens.is_empty() {
            return Err(DatabaseError::ParseError("No tokens found".to_string()));
        }

        let statement_type = match tokens[0].as_str() {
            "CREATE" => {
                if tokens.len() > 1 && tokens[1] == "DATABASE" {
                    StatementType::CreateDatabase
                } else {
                    StatementType::CreateTable
                }
            }
            "INSERT" | "INSERT_INTO" => StatementType::Insert,
            "SELECT" => StatementType::Select,
            "UPDATE" => StatementType::Update,
            "DELETE" => StatementType::Delete,
            _ => {
                return Err(DatabaseError::ParseError(format!(
                    "Unknown statement type: {}",
                    tokens[0]
                )))
            }
        };

        let detected_dialect = self.detect_dialect(&sql_upper, &tokens);

        Ok(SQLAnalysis {
            statement_type,
            detected_dialect,
            original_sql: sql.to_string(),
            tokens,
        })
    }

    fn detect_dialect(&self, sql: &str, _tokens: &[String]) -> DetectedDialect {
        let sql_upper = sql.to_uppercase();

        if sql_upper.contains("VARCHAR2")
            || sql_upper.contains("NVARCHAR2")
            || sql_upper.contains("ROWNUM")
            || sql_upper.contains("DUAL")
            || sql_upper.contains("NEXTVAL")
            || sql_upper.contains("SYSDATE")
        {
            return DetectedDialect::Oracle;
        }

        if sql_upper.contains("NVARCHAR")
            || sql_upper.contains("IDENTITY")
            || sql_upper.contains("UNIQUEIDENTIFIER")
            || sql_upper.contains("DATETIME2")
            || (sql.contains("[") && sql.contains("]"))
        {
            return DetectedDialect::MsSQL;
        }

        if sql_upper.contains("AUTO_INCREMENT")
            || sql_upper.contains("LONGTEXT")
            || sql_upper.contains("MEDIUMTEXT")
            || sql_upper.contains("TINYTEXT")
            || sql_upper.contains("UNSIGNED")
            || sql.contains("`")
        {
            return DetectedDialect::MySQL;
        }

        let mut dialect_scores = HashMap::new();
        dialect_scores.insert(DetectedDialect::Standard, 1);
        dialect_scores.insert(DetectedDialect::MsSQL, 0);
        dialect_scores.insert(DetectedDialect::MySQL, 0);
        dialect_scores.insert(DetectedDialect::Oracle, 0);

        for keyword in &self.keyword_patterns.mssql_keywords {
            if sql_upper.contains(keyword) {
                *dialect_scores.get_mut(&DetectedDialect::MsSQL).unwrap() += 3;
            }
        }

        for keyword in &self.keyword_patterns.mysql_keywords {
            if sql_upper.contains(keyword) {
                *dialect_scores.get_mut(&DetectedDialect::MySQL).unwrap() += 3;
            }
        }

        for keyword in &self.keyword_patterns.oracle_keywords {
            if sql_upper.contains(keyword) {
                *dialect_scores.get_mut(&DetectedDialect::Oracle).unwrap() += 3;
            }
        }

        dialect_scores
            .into_iter()
            .max_by_key(|(_, score)| *score)
            .map(|(dialect, _)| dialect)
            .unwrap_or(DetectedDialect::Standard)
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
        let type_upper = type_str.to_uppercase();

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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum DetectedDialect {
    Standard,
    MsSQL,
    MySQL,
    Oracle,
}
