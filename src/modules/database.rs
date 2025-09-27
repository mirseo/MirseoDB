use super::types::{DatabaseError, Row, SqlStatement, SqlValue, Table, WhereClause, ComparisonOperator};
use super::storage::StorageEngine;
use std::collections::HashMap;

pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
    storage: StorageEngine,
}

impl Database {
    pub fn new(name: String) -> Self {
        Self {
            name: name.clone(),
            tables: HashMap::new(),
            storage: StorageEngine::new(name),
        }
    }

    pub fn create_database(name: String) -> Result<Self, DatabaseError> {
        use std::fs;
        use std::path::Path;

        // Create .mirseoDB directory if it doesn't exist
        let mirseo_db_dir = Path::new(".mirseoDB");
        if !mirseo_db_dir.exists() {
            fs::create_dir_all(mirseo_db_dir)
                .map_err(|e| DatabaseError::IoError(format!("Failed to create .mirseoDB directory: {}", e)))?;
        }

        // Create the database file
        let db_file_path = mirseo_db_dir.join(format!("{}.mdb", name));

        // Create empty database file
        fs::File::create(&db_file_path)
            .map_err(|e| DatabaseError::IoError(format!("Failed to create database file: {}", e)))?;

        println!("Database '{}' created at {}", name, db_file_path.display());

        // Create and return new database instance
        Ok(Self::new(name))
    }

    pub fn load(name: String) -> Result<Self, DatabaseError> {
        let storage = StorageEngine::new(name.clone());
        let tables = storage.load_tables()?;

        Ok(Self {
            name,
            tables,
            storage,
        })
    }

    pub fn execute(&mut self, statement: SqlStatement) -> Result<Vec<Row>, DatabaseError> {
        match statement {
            SqlStatement::CreateDatabase { database_name } => {
                // Create the database file in .mirseoDB directory
                Self::create_database(database_name)?;
                Ok(vec![])
            },
            SqlStatement::CreateTable { table_name, columns } => {
                let table = Table {
                    name: table_name.clone(),
                    columns,
                    rows: Vec::new(),
                };
                self.tables.insert(table_name, table);
                self.storage.save_tables(&self.tables)?;
                Ok(vec![])
            },
            SqlStatement::Insert { table_name, columns, values } => {
                let table = self.tables.get_mut(&table_name)
                    .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                let mut row_columns = HashMap::new();
                for (i, column_name) in columns.iter().enumerate() {
                    if let Some(value) = values.get(i) {
                        row_columns.insert(column_name.clone(), value.clone());
                    }
                }

                let row = Row { columns: row_columns };
                table.rows.push(row);
                self.storage.save_tables(&self.tables)?;
                Ok(vec![])
            },
            SqlStatement::Select { table_name, columns, where_clause } => {
                let table = self.tables.get(&table_name)
                    .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                let mut result_rows = Vec::new();

                for row in &table.rows {
                    if let Some(ref where_clause) = where_clause {
                        if !self.evaluate_where_clause(row, where_clause)? {
                            continue;
                        }
                    }

                    let mut result_row = HashMap::new();

                    if columns.len() == 1 && columns[0] == "*" {
                        result_row = row.columns.clone();
                    } else {
                        for column_name in &columns {
                            if let Some(value) = row.columns.get(column_name) {
                                result_row.insert(column_name.clone(), value.clone());
                            }
                        }
                    }

                    result_rows.push(Row { columns: result_row });
                }

                Ok(result_rows)
            },
            SqlStatement::Update { table_name, set_clauses, where_clause } => {
                let indices_to_update: Vec<usize> = if let Some(ref where_clause) = where_clause {
                    let table = self.tables.get(&table_name)
                        .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                    table.rows.iter().enumerate()
                        .filter_map(|(i, row)| {
                            if self.evaluate_where_clause(row, where_clause).unwrap_or(false) {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    (0..self.tables.get(&table_name).unwrap().rows.len()).collect()
                };

                let table = self.tables.get_mut(&table_name)
                    .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                for index in indices_to_update {
                    let row = &mut table.rows[index];
                    for (column_name, new_value) in &set_clauses {
                        row.columns.insert(column_name.clone(), new_value.clone());
                    }
                }

                self.storage.save_tables(&self.tables)?;
                Ok(vec![])
            },
            SqlStatement::Delete { table_name, where_clause } => {
                let indices_to_delete: Vec<usize> = if let Some(ref where_clause) = where_clause {
                    let table = self.tables.get(&table_name)
                        .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                    table.rows.iter().enumerate()
                        .filter_map(|(i, row)| {
                            if self.evaluate_where_clause(row, where_clause).unwrap_or(false) {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                let table = self.tables.get_mut(&table_name)
                    .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                if where_clause.is_none() {
                    table.rows.clear();
                } else {
                    for index in indices_to_delete.into_iter().rev() {
                        table.rows.remove(index);
                    }
                }

                self.storage.save_tables(&self.tables)?;
                Ok(vec![])
            },
        }
    }

    fn evaluate_where_clause(&self, row: &Row, where_clause: &WhereClause) -> Result<bool, DatabaseError> {
        let row_value = row.columns.get(&where_clause.column)
            .ok_or_else(|| DatabaseError::ColumnNotFound(where_clause.column.clone()))?;

        Ok(match &where_clause.operator {
            ComparisonOperator::Equal => self.compare_values(row_value, &where_clause.value) == std::cmp::Ordering::Equal,
            ComparisonOperator::NotEqual => self.compare_values(row_value, &where_clause.value) != std::cmp::Ordering::Equal,
            ComparisonOperator::GreaterThan => self.compare_values(row_value, &where_clause.value) == std::cmp::Ordering::Greater,
            ComparisonOperator::LessThan => self.compare_values(row_value, &where_clause.value) == std::cmp::Ordering::Less,
            ComparisonOperator::GreaterThanOrEqual => {
                let cmp = self.compare_values(row_value, &where_clause.value);
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            },
            ComparisonOperator::LessThanOrEqual => {
                let cmp = self.compare_values(row_value, &where_clause.value);
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            },
        })
    }

    fn compare_values(&self, a: &SqlValue, b: &SqlValue) -> std::cmp::Ordering {
        match (a, b) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
            (SqlValue::Float(a), SqlValue::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (SqlValue::Text(a), SqlValue::Text(b)) => a.cmp(b),
            (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
            (SqlValue::Null, SqlValue::Null) => std::cmp::Ordering::Equal,
            _ => std::cmp::Ordering::Equal,
        }
    }
}
