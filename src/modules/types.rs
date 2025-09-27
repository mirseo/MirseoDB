use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum SqlValue {
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub columns: HashMap<String, SqlValue>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}

#[derive(Debug, Clone)]
pub enum SqlStatement {
    CreateDatabase {
        database_name: String,
    },
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDefinition>,
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<SqlValue>,
    },
    Select {
        table_name: String,
        columns: Vec<String>,
        where_clause: Option<WhereClause>,
    },
    Update {
        table_name: String,
        set_clauses: Vec<(String, SqlValue)>,
        where_clause: Option<WhereClause>,
    },
    Delete {
        table_name: String,
        where_clause: Option<WhereClause>,
    },
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub column: String,
    pub operator: ComparisonOperator,
    pub value: SqlValue,
}

#[derive(Debug, Clone)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

#[derive(Debug)]
pub enum DatabaseError {
    TableNotFound(String),
    ColumnNotFound(String),
    ParseError(String),
    IoError(String),
    InvalidDataType(String),
}