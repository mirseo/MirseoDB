use super::configuration::ConfigManager;
use super::core_types::{
    ColumnDefinition, ComparisonOperator, DatabaseError, Row, SqlStatement, SqlValue, Table,
    WhereClause,
};
use super::indexing::{IndexKey, IndexManager};
use super::persistence::StorageEngine;
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
            fs::create_dir_all(mirseo_db_dir).map_err(|e| {
                DatabaseError::IoError(format!("Failed to create .mirseoDB directory: {}", e))
            })?;
        }

        // Create the database file
        let db_file_path = mirseo_db_dir.join(format!("{}.mdb", name));

        // Create empty database file
        fs::File::create(&db_file_path).map_err(|e| {
            DatabaseError::IoError(format!("Failed to create database file: {}", e))
        })?;

        // Create route.cfg file in .mirseoDB directory
        let route_cfg_path = mirseo_db_dir.join("route.cfg");
        let route_cfg_content = format!(
            "# Route Configuration for MirseoDB Database: {}\n# Format: route_name=server_url\n# Example: backup_server=http://192.168.1.100:3306\n# Example: analytics_server=http://analytics.company.com:3306\n\n# Default fallback server (uncomment and configure as needed)\n# fallback=http://localhost:3307\n\n# Additional route examples:\n# primary_db=http://primary.database.local:3306\n# secondary_db=http://secondary.database.local:3306\n# analytics=http://analytics.server.local:3306\n",
            name
        );

        fs::write(&route_cfg_path, route_cfg_content).map_err(|e| {
            DatabaseError::IoError(format!("Failed to create route.cfg file: {}", e))
        })?;

        println!("Database '{}' created at {}", name, db_file_path.display());
        println!(
            "Route configuration created at {}",
            route_cfg_path.display()
        );

        // Ensure config exists with defaults on first creation
        ConfigManager::ensure_exists()?;

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
            }
            SqlStatement::CreateTable {
                table_name,
                columns,
            } => {
                self.create_table_with_indexes(table_name, columns)?;
                Ok(vec![])
            }
            SqlStatement::Insert {
                table_name,
                columns,
                values,
            } => {
                self.insert_row_with_indexes(table_name, columns, values)?;
                Ok(vec![])
            }
            SqlStatement::Select {
                table_name,
                columns,
                where_clause,
            } => self.select_with_indexes(table_name, columns, where_clause),
            SqlStatement::Update {
                table_name,
                set_clauses,
                where_clause,
            } => {
                let indices_to_update: Vec<usize> = if let Some(ref where_clause) = where_clause {
                    let table = self
                        .tables
                        .get(&table_name)
                        .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                    table
                        .rows
                        .iter()
                        .enumerate()
                        .filter_map(|(i, row)| {
                            if self
                                .evaluate_where_clause(row, where_clause)
                                .unwrap_or(false)
                            {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    (0..self.tables.get(&table_name).unwrap().rows.len()).collect()
                };

                let table = self
                    .tables
                    .get_mut(&table_name)
                    .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                for index in indices_to_update {
                    let row = &mut table.rows[index];
                    for (column_name, new_value) in &set_clauses {
                        row.columns.insert(column_name.clone(), new_value.clone());
                    }
                }

                self.storage.save_tables(&self.tables)?;
                Ok(vec![])
            }
            SqlStatement::Delete {
                table_name,
                where_clause,
            } => {
                let indices_to_delete: Vec<usize> = if let Some(ref where_clause) = where_clause {
                    let table = self
                        .tables
                        .get(&table_name)
                        .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                    table
                        .rows
                        .iter()
                        .enumerate()
                        .filter_map(|(i, row)| {
                            if self
                                .evaluate_where_clause(row, where_clause)
                                .unwrap_or(false)
                            {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                let table = self
                    .tables
                    .get_mut(&table_name)
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
            }
            SqlStatement::DropTable { table_name } => {
                self.tables.remove(&table_name);
                self.storage.save_tables(&self.tables)?;
                Ok(vec![])
            }
            SqlStatement::DropDatabase { database_name } => {
                // Drop database is a dangerous operation - clear all tables
                self.tables.clear();
                self.storage.save_tables(&self.tables)?;
                Ok(vec![])
            }
            SqlStatement::AlterTable { table_name, action } => {
                use super::core_types::AlterAction;

                let table = self
                    .tables
                    .get_mut(&table_name)
                    .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

                match action {
                    AlterAction::AddColumn { column } => {
                        // Check if column already exists
                        if table.columns.iter().any(|c| c.name == column.name) {
                            return Err(DatabaseError::ParseError(format!(
                                "Column '{}' already exists",
                                column.name
                            )));
                        }

                        // Add column definition
                        table.columns.push(column.clone());

                        // Add default value to all existing rows
                        let default_value = match column.data_type {
                            super::core_types::DataType::Integer => SqlValue::Integer(0),
                            super::core_types::DataType::Float => SqlValue::Float(0.0),
                            super::core_types::DataType::Text => SqlValue::Text("".to_string()),
                            super::core_types::DataType::Boolean => SqlValue::Boolean(false),
                        };

                        for row in &mut table.rows {
                            row.columns
                                .insert(column.name.clone(), default_value.clone());
                        }
                    }
                    AlterAction::DropColumn { column_name } => {
                        // Remove column definition
                        table.columns.retain(|c| c.name != *column_name);

                        // Remove column data from all rows
                        for row in &mut table.rows {
                            row.columns.remove(&column_name);
                        }
                    }
                    AlterAction::ModifyColumn { column } => {
                        // Find and update column definition
                        if let Some(existing_column) =
                            table.columns.iter_mut().find(|c| c.name == column.name)
                        {
                            existing_column.data_type = column.data_type.clone();
                            existing_column.nullable = column.nullable;
                            existing_column.primary_key = column.primary_key;
                        } else {
                            return Err(DatabaseError::ColumnNotFound(column.name.clone()));
                        }
                    }
                }

                self.storage.save_tables(&self.tables)?;
                Ok(vec![])
            }
        }
    }

    fn evaluate_where_clause(
        &self,
        row: &Row,
        where_clause: &WhereClause,
    ) -> Result<bool, DatabaseError> {
        let row_value = row
            .columns
            .get(&where_clause.column)
            .ok_or_else(|| DatabaseError::ColumnNotFound(where_clause.column.clone()))?;

        Ok(match &where_clause.operator {
            ComparisonOperator::Equal => {
                self.compare_values(row_value, &where_clause.value) == std::cmp::Ordering::Equal
            }
            ComparisonOperator::NotEqual => {
                self.compare_values(row_value, &where_clause.value) != std::cmp::Ordering::Equal
            }
            ComparisonOperator::GreaterThan => {
                self.compare_values(row_value, &where_clause.value) == std::cmp::Ordering::Greater
            }
            ComparisonOperator::LessThan => {
                self.compare_values(row_value, &where_clause.value) == std::cmp::Ordering::Less
            }
            ComparisonOperator::GreaterThanOrEqual => {
                let cmp = self.compare_values(row_value, &where_clause.value);
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            ComparisonOperator::LessThanOrEqual => {
                let cmp = self.compare_values(row_value, &where_clause.value);
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
        })
    }

    fn compare_values(&self, a: &SqlValue, b: &SqlValue) -> std::cmp::Ordering {
        match (a, b) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
            (SqlValue::Float(a), SqlValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (SqlValue::Text(a), SqlValue::Text(b)) => a.cmp(b),
            (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
            (SqlValue::Null, SqlValue::Null) => std::cmp::Ordering::Equal,
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn create_table_with_indexes(
        &mut self,
        table_name: String,
        columns: Vec<ColumnDefinition>,
    ) -> Result<(), DatabaseError> {
        let mut index_manager = IndexManager::new();

        for column in &columns {
            if column.primary_key {
                let index_name = format!("pk_{}", column.name);
                index_manager.create_index(index_name, column.name.clone(), true, true)?;
                println!(
                    "[MirseoDB] Auto-created primary key index for column '{}'",
                    column.name
                );
            } else if !column.nullable {
                let index_name = format!("idx_{}_{}", table_name, column.name);
                index_manager.create_index(index_name, column.name.clone(), false, false)?;
                println!(
                    "[MirseoDB] Auto-created index for NOT NULL column '{}'",
                    column.name
                );
            }
        }

        let table = Table {
            name: table_name.clone(),
            columns,
            rows: Vec::new(),
            index_manager,
            next_row_id: 0,
        };

        self.tables.insert(table_name.clone(), table);
        self.storage.save_tables(&self.tables)?;

        println!(
            "[MirseoDB] Created table '{}' with auto-indexing enabled",
            table_name
        );
        Ok(())
    }

    fn insert_row_with_indexes(
        &mut self,
        table_name: String,
        columns: Vec<String>,
        values: Vec<SqlValue>,
    ) -> Result<(), DatabaseError> {
        let table = self
            .tables
            .get_mut(&table_name)
            .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

        let mut row_columns = HashMap::new();

        for table_column in &table.columns {
            if let Some(pos) = columns.iter().position(|c| c == &table_column.name) {
                if let Some(value) = values.get(pos) {
                    row_columns.insert(table_column.name.clone(), value.clone());
                }
            } else if !table_column.nullable && !table_column.primary_key {
                return Err(DatabaseError::ColumnNotFound(format!(
                    "Non-nullable column '{}' requires a value",
                    table_column.name
                )));
            }
        }

        if let Some(pk_index) = table.index_manager.get_primary_key_index() {
            if let Some(pk_value) = row_columns.get(&pk_index.column_name) {
                if !pk_index.find_exact(pk_value).is_empty() {
                    return Err(DatabaseError::PrimaryKeyViolation(format!(
                        "Primary key value {:?} already exists",
                        pk_value
                    )));
                }
            }
        }

        let row_id = table.next_row_id;
        table.next_row_id += 1;

        table
            .index_manager
            .insert_into_indexes(&row_columns, row_id)?;

        let row = Row {
            columns: row_columns,
        };
        table.rows.push(row);

        self.storage.save_tables(&self.tables)?;

        println!(
            "[MirseoDB] Inserted row with ID {} into table '{}'",
            row_id, table_name
        );
        Ok(())
    }

    fn select_with_indexes(
        &self,
        table_name: String,
        columns: Vec<String>,
        where_clause: Option<WhereClause>,
    ) -> Result<Vec<Row>, DatabaseError> {
        let table = self
            .tables
            .get(&table_name)
            .ok_or_else(|| DatabaseError::TableNotFound(table_name.clone()))?;

        let mut candidate_row_ids: Option<Vec<usize>> = None;

        if let Some(ref where_clause) = where_clause {
            if let Some(index) = table
                .index_manager
                .find_best_index_for_query(&where_clause.column)
            {
                println!(
                    "[MirseoDB] Using index '{}' for query optimization",
                    index.name
                );

                candidate_row_ids = Some(match where_clause.operator {
                    ComparisonOperator::Equal => index.find_exact(&where_clause.value),
                    ComparisonOperator::GreaterThan => index.find_greater_than(&where_clause.value),
                    ComparisonOperator::LessThan => index.find_less_than(&where_clause.value),
                    ComparisonOperator::GreaterThanOrEqual => {
                        let mut result = index.find_exact(&where_clause.value);
                        result.extend(index.find_greater_than(&where_clause.value));
                        result
                    }
                    ComparisonOperator::LessThanOrEqual => {
                        let mut result = index.find_exact(&where_clause.value);
                        result.extend(index.find_less_than(&where_clause.value));
                        result
                    }
                    ComparisonOperator::NotEqual => {
                        let all_keys = index.get_all_keys();
                        let mut result = Vec::new();
                        for key in all_keys {
                            if key != IndexKey::from(&where_clause.value) {
                                if let Ok(sql_value) = self.index_key_to_sql_value(&key) {
                                    result.extend(index.find_exact(&sql_value));
                                }
                            }
                        }
                        result
                    }
                });
            }
        }

        let mut result_rows = Vec::new();

        match candidate_row_ids {
            Some(row_ids) => {
                println!(
                    "[MirseoDB] Index scan returned {} candidate rows",
                    row_ids.len()
                );
                for &row_id in &row_ids {
                    if let Some(row) = table.rows.get(row_id) {
                        if let Some(ref where_clause) = where_clause {
                            if !self.evaluate_where_clause(row, where_clause)? {
                                continue;
                            }
                        }

                        let selected_row = self.project_columns(row, &columns);
                        result_rows.push(selected_row);
                    }
                }
            }
            None => {
                println!("[MirseoDB] Full table scan on {} rows", table.rows.len());
                for row in &table.rows {
                    if let Some(ref where_clause) = where_clause {
                        if !self.evaluate_where_clause(row, where_clause)? {
                            continue;
                        }
                    }

                    let selected_row = self.project_columns(row, &columns);
                    result_rows.push(selected_row);
                }
            }
        }

        println!("[MirseoDB] Query returned {} rows", result_rows.len());
        Ok(result_rows)
    }

    fn project_columns(&self, row: &Row, columns: &[String]) -> Row {
        let mut result_row = HashMap::new();

        if columns.len() == 1 && columns[0] == "*" {
            result_row = row.columns.clone();
        } else {
            for column_name in columns {
                if let Some(value) = row.columns.get(column_name) {
                    result_row.insert(column_name.clone(), value.clone());
                }
            }
        }

        Row {
            columns: result_row,
        }
    }

    fn index_key_to_sql_value(&self, key: &IndexKey) -> Result<SqlValue, DatabaseError> {
        match key {
            IndexKey::Integer(i) => Ok(SqlValue::Integer(*i)),
            IndexKey::Float(f) => Ok(SqlValue::Float(f.value())),
            IndexKey::Text(s) => Ok(SqlValue::Text(s.clone())),
            IndexKey::Boolean(b) => Ok(SqlValue::Boolean(*b)),
            IndexKey::Null => Ok(SqlValue::Null),
        }
    }
}
