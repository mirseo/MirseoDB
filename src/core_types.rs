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
    UniqueConstraintViolation(String),
    IndexAlreadyExists(String),
    IndexNotFound(String),
    PrimaryKeyViolation(String),
}
