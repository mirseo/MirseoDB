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
    pub index_manager: super::indexing::IndexManager,
    pub next_row_id: usize,
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
        optimization_hint: Option<QueryOptimizationHint>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    ComplexSelect {
        table_name: String,
        columns: Vec<String>,
        complex_where: Option<ComplexWhereClause>,
        optimization_hint: Option<QueryOptimizationHint>,
        order_by: Option<Vec<OrderBy>>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    CreateCompositeIndex {
        index_name: String,
        table_name: String,
        column_names: Vec<String>,
        is_unique: bool,
    },
    DropIndex {
        index_name: String,
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
    DropTable {
        table_name: String,
    },
    DropDatabase {
        database_name: String,
    },
    AlterTable {
        table_name: String,
        action: AlterAction,
    },
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum AlterAction {
    AddColumn { column: ColumnDefinition },
    DropColumn { column_name: String },
    ModifyColumn { column: ColumnDefinition },
}

impl SqlStatement {
    /// 민감한 SQL 작업인지 확인 (2차 인증이 필요한 작업)
    pub fn requires_2fa(&self) -> bool {
        match self {
            SqlStatement::DropTable { .. } => true,
            SqlStatement::DropDatabase { .. } => true,
            SqlStatement::AlterTable { .. } => true,
            SqlStatement::Delete {
                where_clause: None, ..
            } => true, // WHERE 절이 없는 DELETE는 위험
            SqlStatement::Update {
                where_clause: None, ..
            } => true, // WHERE 절이 없는 UPDATE는 위험
            _ => false,
        }
    }

    pub fn get_operation_name(&self) -> &'static str {
        match self {
            SqlStatement::CreateDatabase { .. } => "CREATE DATABASE",
            SqlStatement::CreateTable { .. } => "CREATE TABLE",
            SqlStatement::Insert { .. } => "INSERT",
            SqlStatement::Select { .. } => "SELECT",
            SqlStatement::ComplexSelect { .. } => "COMPLEX SELECT",
            SqlStatement::CreateCompositeIndex { .. } => "CREATE COMPOSITE INDEX",
            SqlStatement::DropIndex { .. } => "DROP INDEX",
            SqlStatement::Update { .. } => "UPDATE",
            SqlStatement::Delete { .. } => "DELETE",
            SqlStatement::DropTable { .. } => "DROP TABLE",
            SqlStatement::DropDatabase { .. } => "DROP DATABASE",
            SqlStatement::AlterTable { .. } => "ALTER TABLE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub column: String,
    pub operator: ComparisonOperator,
    pub value: SqlValue,
}

#[derive(Debug, Clone)]
pub struct ComplexWhereClause {
    pub conditions: Vec<WhereCondition>,
    pub logical_operators: Vec<LogicalOperator>,
}

#[derive(Debug, Clone)]
pub enum WhereCondition {
    Simple(WhereClause),
    Nested(ComplexWhereClause),
    In { column: String, values: Vec<SqlValue> },
    Between { column: String, start: SqlValue, end: SqlValue },
    Like { column: String, pattern: String },
    IsNull { column: String },
    IsNotNull { column: String },
}

#[derive(Debug, Clone)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone)]
pub struct IndexHint {
    pub hint_type: IndexHintType,
    pub index_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum IndexHintType {
    Use,
    Force,
    Ignore,
}

#[derive(Debug, Clone)]
pub struct QueryOptimizationHint {
    pub index_hints: Option<IndexHint>,
    pub force_index_scan: bool,
    pub disable_optimization: bool,
    pub max_rows: Option<usize>,
    pub enable_bloom_filter: bool,
    pub chunk_size: Option<usize>,
    pub early_termination: bool,
}

#[derive(Debug, Clone)]
pub struct TableScanOptions {
    pub use_bloom_filter: bool,
    pub chunk_size: usize,
    pub max_memory_mb: usize,
    pub enable_early_termination: bool,
    pub collect_statistics: bool,
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
    UniqueConstraintViolation(String),
    PrimaryKeyViolation(String),
    IndexAlreadyExists(String),
    InvalidDataType(String),
    PermissionDenied(String),
    IndexNotFound(String),
    InvalidCredentials(String),
    TwoFactorAuthRequired(String),
    NetworkError(String),
    HttpError(String),
    InvalidSqlSyntax(String),
    SqlInjectionDetected,
    QueryTooComplex,
    InvalidIndexHint(String),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            DatabaseError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
            DatabaseError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            DatabaseError::IoError(msg) => write!(f, "IO error: {}", msg),
            DatabaseError::UniqueConstraintViolation(msg) => {
                write!(f, "Unique constraint violation: {}", msg)
            }
            DatabaseError::PrimaryKeyViolation(msg) => {
                write!(f, "Primary key violation: {}", msg)
            }
            DatabaseError::IndexAlreadyExists(name) => write!(f, "Index '{}' already exists", name),
            DatabaseError::InvalidDataType(msg) => write!(f, "Invalid data type: {}", msg),
            DatabaseError::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
            DatabaseError::IndexNotFound(name) => write!(f, "Index '{}' not found", name),
            DatabaseError::InvalidCredentials(msg) => write!(f, "Invalid credentials: {}", msg),
            DatabaseError::TwoFactorAuthRequired(msg) => {
                write!(f, "Two-factor authentication required: {}", msg)
            }
            DatabaseError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            DatabaseError::HttpError(msg) => write!(f, "HTTP error: {}", msg),
            DatabaseError::InvalidSqlSyntax(msg) => write!(f, "Invalid SQL syntax: {}", msg),
            DatabaseError::SqlInjectionDetected => write!(f, "SQL injection attempt detected"),
            DatabaseError::QueryTooComplex => write!(f, "Query too complex"),
            DatabaseError::InvalidIndexHint(msg) => write!(f, "Invalid index hint: {}", msg),
        }
    }
}

impl std::error::Error for DatabaseError {}