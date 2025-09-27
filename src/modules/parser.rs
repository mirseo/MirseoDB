use super::types::{SqlStatement, ColumnDefinition, DataType, SqlValue, WhereClause, ComparisonOperator, DatabaseError};

#[derive(Debug, Clone)]
pub enum SqlDialect {
    Standard,
    MsSql,
    MariaSql,
    OracleSql,
}

pub struct Parser {
    dialect: SqlDialect,
}

impl Parser {
    pub fn new(dialect: SqlDialect) -> Self {
        Self { dialect }
    }

    pub fn parse(&self, sql: &str) -> Result<SqlStatement, DatabaseError> {
        let sql = sql.trim().to_uppercase();
        let tokens: Vec<&str> = sql.split_whitespace().collect();

        if tokens.is_empty() {
            return Err(DatabaseError::ParseError("Empty SQL statement".to_string()));
        }

        match tokens[0] {
            "CREATE" => self.parse_create(&tokens),
            "INSERT" => self.parse_insert(&tokens),
            "SELECT" => self.parse_select(&tokens),
            "UPDATE" => self.parse_update(&tokens),
            "DELETE" => self.parse_delete(&tokens),
            _ => Err(DatabaseError::ParseError(format!("Unsupported statement: {}", tokens[0]))),
        }
    }

    fn parse_create(&self, tokens: &[&str]) -> Result<SqlStatement, DatabaseError> {
        if tokens.len() < 3 {
            return Err(DatabaseError::ParseError("Invalid CREATE syntax".to_string()));
        }

        match tokens[1] {
            "DATABASE" => self.parse_create_database(&tokens),
            "TABLE" => self.parse_create_table(&tokens),
            _ => Err(DatabaseError::ParseError(format!("Unsupported CREATE statement: CREATE {}", tokens[1]))),
        }
    }

    fn parse_create_database(&self, tokens: &[&str]) -> Result<SqlStatement, DatabaseError> {
        if tokens.len() < 3 || tokens[1] != "DATABASE" {
            return Err(DatabaseError::ParseError("Invalid CREATE DATABASE syntax".to_string()));
        }

        let database_name = tokens[2].to_string();
        Ok(SqlStatement::CreateDatabase { database_name })
    }

    fn parse_create_table(&self, tokens: &[&str]) -> Result<SqlStatement, DatabaseError> {
        if tokens.len() < 3 || tokens[1] != "TABLE" {
            return Err(DatabaseError::ParseError("Invalid CREATE TABLE syntax".to_string()));
        }

        let table_name = tokens[2].to_string();

        // If there are only 3 tokens (CREATE TABLE tablename), create an empty table
        if tokens.len() == 3 {
            return Ok(SqlStatement::CreateTable {
                table_name,
                columns: Vec::new()
            });
        }

        let sql_str = tokens.join(" ");

        // Check if there are column definitions in parentheses
        if let Some(start_pos) = sql_str.find('(') {
            let end_pos = sql_str.rfind(')').ok_or_else(||
                DatabaseError::ParseError("Missing closing parenthesis".to_string()))?;

            let columns_str = &sql_str[start_pos + 1..end_pos];
            let column_defs: Vec<&str> = columns_str.split(',').collect();

            let mut columns = Vec::new();
            for column_def in column_defs {
                let column_tokens: Vec<&str> = column_def.trim().split_whitespace().collect();
                if column_tokens.len() < 2 {
                    return Err(DatabaseError::ParseError("Invalid column definition".to_string()));
                }

                let column_name = column_tokens[0].to_string();
                let data_type = self.parse_data_type(column_tokens[1])?;

                let mut nullable = true;
                let mut primary_key = false;

                for i in 2..column_tokens.len() {
                    match column_tokens[i] {
                        "NOT" if i + 1 < column_tokens.len() && column_tokens[i + 1] == "NULL" => {
                            nullable = false;
                        },
                        "PRIMARY" if i + 1 < column_tokens.len() && column_tokens[i + 1] == "KEY" => {
                            primary_key = true;
                        },
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

            Ok(SqlStatement::CreateTable { table_name, columns })
        } else {
            // No parentheses found, create empty table
            Ok(SqlStatement::CreateTable {
                table_name,
                columns: Vec::new()
            })
        }
    }

    fn parse_insert(&self, tokens: &[&str]) -> Result<SqlStatement, DatabaseError> {
        if tokens.len() < 4 || tokens[1] != "INTO" {
            return Err(DatabaseError::ParseError("Invalid INSERT syntax".to_string()));
        }

        let table_name = tokens[2].to_string();

        let sql_str = tokens.join(" ");
        let values_pos = sql_str.find("VALUES").ok_or_else(||
            DatabaseError::ParseError("Missing VALUES clause".to_string()))?;

        let columns_part = &sql_str[sql_str.find(table_name.as_str()).unwrap() + table_name.len()..values_pos];

        let columns = if let Some(start) = columns_part.find('(') {
            if let Some(end) = columns_part.find(')') {
                let columns_str = &columns_part[start + 1..end];
                columns_str.split(',')
                    .map(|s| s.trim().to_string())
                    .collect()
            } else {
                return Err(DatabaseError::ParseError("Missing closing parenthesis in column list".to_string()));
            }
        } else {
            vec!["*".to_string()]
        };

        let values_part = &sql_str[values_pos + 6..];
        let start_pos = values_part.find('(').ok_or_else(||
            DatabaseError::ParseError("Missing opening parenthesis in VALUES".to_string()))?;
        let end_pos = values_part.rfind(')').ok_or_else(||
            DatabaseError::ParseError("Missing closing parenthesis in VALUES".to_string()))?;

        let values_str = &values_part[start_pos + 1..end_pos];
        let value_strs: Vec<&str> = values_str.split(',').collect();

        let mut values = Vec::new();
        for value_str in value_strs {
            let value = self.parse_value(value_str.trim())?;
            values.push(value);
        }

        Ok(SqlStatement::Insert { table_name, columns, values })
    }

    fn parse_select(&self, tokens: &[&str]) -> Result<SqlStatement, DatabaseError> {
        let from_pos = tokens.iter().position(|&token| token == "FROM")
            .ok_or_else(|| DatabaseError::ParseError("Missing FROM clause".to_string()))?;

        if from_pos + 1 >= tokens.len() {
            return Err(DatabaseError::ParseError("Missing table name after FROM".to_string()));
        }

        let columns: Vec<String> = tokens[1..from_pos]
            .iter()
            .flat_map(|s| s.split(','))
            .map(|s| s.trim().to_string())
            .collect();

        let table_name = tokens[from_pos + 1].to_string();

        let where_clause = if let Some(where_pos) = tokens.iter().position(|&token| token == "WHERE") {
            Some(self.parse_where_clause(&tokens[where_pos + 1..])?)
        } else {
            None
        };

        Ok(SqlStatement::Select { table_name, columns, where_clause })
    }

    fn parse_update(&self, tokens: &[&str]) -> Result<SqlStatement, DatabaseError> {
        if tokens.len() < 4 {
            return Err(DatabaseError::ParseError("Invalid UPDATE syntax".to_string()));
        }

        let table_name = tokens[1].to_string();

        let set_pos = tokens.iter().position(|&token| token == "SET")
            .ok_or_else(|| DatabaseError::ParseError("Missing SET clause".to_string()))?;

        let where_pos = tokens.iter().position(|&token| token == "WHERE");

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

            let column_name = parts[0].trim().to_string();
            let value = self.parse_value(parts[1].trim())?;
            set_clauses.push((column_name, value));
        }

        let where_clause = if let Some(where_pos) = where_pos {
            Some(self.parse_where_clause(&tokens[where_pos + 1..])?)
        } else {
            None
        };

        Ok(SqlStatement::Update { table_name, set_clauses, where_clause })
    }

    fn parse_delete(&self, tokens: &[&str]) -> Result<SqlStatement, DatabaseError> {
        if tokens.len() < 3 || tokens[1] != "FROM" {
            return Err(DatabaseError::ParseError("Invalid DELETE syntax".to_string()));
        }

        let table_name = tokens[2].to_string();

        let where_clause = if let Some(where_pos) = tokens.iter().position(|&token| token == "WHERE") {
            Some(self.parse_where_clause(&tokens[where_pos + 1..])?)
        } else {
            None
        };

        Ok(SqlStatement::Delete { table_name, where_clause })
    }

    fn parse_where_clause(&self, tokens: &[&str]) -> Result<WhereClause, DatabaseError> {
        if tokens.len() < 3 {
            return Err(DatabaseError::ParseError("Invalid WHERE clause".to_string()));
        }

        let column = tokens[0].to_string();
        let operator = self.parse_comparison_operator(tokens[1])?;
        let value = self.parse_value(tokens[2])?;

        Ok(WhereClause { column, operator, value })
    }

    fn parse_comparison_operator(&self, op: &str) -> Result<ComparisonOperator, DatabaseError> {
        match op {
            "=" => Ok(ComparisonOperator::Equal),
            "!=" | "<>" => Ok(ComparisonOperator::NotEqual),
            ">" => Ok(ComparisonOperator::GreaterThan),
            "<" => Ok(ComparisonOperator::LessThan),
            ">=" => Ok(ComparisonOperator::GreaterThanOrEqual),
            "<=" => Ok(ComparisonOperator::LessThanOrEqual),
            _ => Err(DatabaseError::ParseError(format!("Unknown comparison operator: {}", op))),
        }
    }

    fn parse_data_type(&self, type_str: &str) -> Result<DataType, DatabaseError> {
        let type_upper = type_str.to_uppercase();
        match type_upper.as_str() {
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => Ok(DataType::Integer),
            "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" | "NUMERIC" => Ok(DataType::Float),
            "VARCHAR" | "TEXT" | "CHAR" | "NVARCHAR" | "STRING" => Ok(DataType::Text),
            "BOOL" | "BOOLEAN" | "BIT" => Ok(DataType::Boolean),
            _ => match self.dialect {
                SqlDialect::MsSql => self.parse_mssql_type(&type_upper),
                SqlDialect::MariaSql => self.parse_maria_type(&type_upper),
                SqlDialect::OracleSql => self.parse_oracle_type(&type_upper),
                SqlDialect::Standard => Err(DatabaseError::InvalidDataType(type_str.to_string())),
            }
        }
    }

    fn parse_mssql_type(&self, type_str: &str) -> Result<DataType, DatabaseError> {
        match type_str {
            "NTEXT" | "NCHAR" => Ok(DataType::Text),
            "MONEY" | "SMALLMONEY" => Ok(DataType::Float),
            "TINYINT" => Ok(DataType::Integer),
            _ => Err(DatabaseError::InvalidDataType(type_str.to_string())),
        }
    }

    fn parse_maria_type(&self, type_str: &str) -> Result<DataType, DatabaseError> {
        match type_str {
            "LONGTEXT" | "MEDIUMTEXT" | "TINYTEXT" => Ok(DataType::Text),
            "DECIMAL" => Ok(DataType::Float),
            "TINYINT" => Ok(DataType::Integer),
            _ => Err(DatabaseError::InvalidDataType(type_str.to_string())),
        }
    }

    fn parse_oracle_type(&self, type_str: &str) -> Result<DataType, DatabaseError> {
        match type_str {
            "VARCHAR2" | "NVARCHAR2" | "CLOB" | "NCLOB" => Ok(DataType::Text),
            "NUMBER" => Ok(DataType::Float),
            _ => Err(DatabaseError::InvalidDataType(type_str.to_string())),
        }
    }

    fn parse_value(&self, value_str: &str) -> Result<SqlValue, DatabaseError> {
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

        if value_str.starts_with('\'') && value_str.ends_with('\'') {
            let text = value_str[1..value_str.len()-1].to_string();
            return Ok(SqlValue::Text(text));
        }

        if value_str.starts_with('"') && value_str.ends_with('"') {
            let text = value_str[1..value_str.len()-1].to_string();
            return Ok(SqlValue::Text(text));
        }

        if value_str.contains('.') {
            if let Ok(float_val) = value_str.parse::<f64>() {
                return Ok(SqlValue::Float(float_val));
            }
        }

        if let Ok(int_val) = value_str.parse::<i64>() {
            return Ok(SqlValue::Integer(int_val));
        }

        Ok(SqlValue::Text(value_str.to_string()))
    }
}