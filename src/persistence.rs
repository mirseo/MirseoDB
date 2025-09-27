use super::indexing::IndexManager;
use super::core_types::{ColumnDefinition, DataType, DatabaseError, Row, SqlValue, Table};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct StorageEngine {
    db_name: String,
}

impl StorageEngine {
    pub fn new(db_name: String) -> Self {
        Self { db_name }
    }

    pub fn save_tables(&self, tables: &HashMap<String, Table>) -> Result<(), DatabaseError> {
        let filepath = self.db_file_path()?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&filepath)
            .map_err(|e| DatabaseError::IoError(e.to_string()))?;

        let serialized = self.serialize_tables(tables)?;
        file.write_all(&serialized)
            .map_err(|e| DatabaseError::IoError(e.to_string()))?;

        Ok(())
    }

    pub fn load_tables(&self) -> Result<HashMap<String, Table>, DatabaseError> {
        let filepath = self.db_file_path()?;

        if !filepath.exists() {
            return Ok(HashMap::new());
        }

        let mut file = File::open(&filepath).map_err(|e| DatabaseError::IoError(e.to_string()))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .map_err(|e| DatabaseError::IoError(e.to_string()))?;

        self.deserialize_tables(&buffer)
    }

    fn db_file_path(&self) -> Result<PathBuf, DatabaseError> {
        let dir = Path::new(".mirseoDB");
        fs::create_dir_all(dir).map_err(|e| DatabaseError::IoError(e.to_string()))?;

        Ok(dir.join(format!("{}.mdb", self.db_name)))
    }

    fn serialize_tables(&self, tables: &HashMap<String, Table>) -> Result<Vec<u8>, DatabaseError> {
        let mut buffer = Vec::new();

        let table_count = tables.len() as u32;
        buffer.extend_from_slice(&table_count.to_le_bytes());

        for table in tables.values() {
            self.serialize_table(table, &mut buffer)?;
        }

        Ok(buffer)
    }

    fn serialize_table(&self, table: &Table, buffer: &mut Vec<u8>) -> Result<(), DatabaseError> {
        let name_bytes = table.name.as_bytes();
        buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(name_bytes);

        buffer.extend_from_slice(&(table.columns.len() as u32).to_le_bytes());
        for column in &table.columns {
            self.serialize_column_definition(column, buffer)?;
        }

        buffer.extend_from_slice(&(table.rows.len() as u32).to_le_bytes());
        for row in &table.rows {
            self.serialize_row(row, buffer)?;
        }

        Ok(())
    }

    fn serialize_column_definition(
        &self,
        column: &ColumnDefinition,
        buffer: &mut Vec<u8>,
    ) -> Result<(), DatabaseError> {
        let name_bytes = column.name.as_bytes();
        buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(name_bytes);

        let data_type_id = match column.data_type {
            DataType::Integer => 0u8,
            DataType::Float => 1u8,
            DataType::Text => 2u8,
            DataType::Boolean => 3u8,
        };
        buffer.push(data_type_id);

        buffer.push(if column.nullable { 1 } else { 0 });
        buffer.push(if column.primary_key { 1 } else { 0 });

        Ok(())
    }

    fn serialize_row(&self, row: &Row, buffer: &mut Vec<u8>) -> Result<(), DatabaseError> {
        buffer.extend_from_slice(&(row.columns.len() as u32).to_le_bytes());

        for (column_name, value) in &row.columns {
            let name_bytes = column_name.as_bytes();
            buffer.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(name_bytes);

            self.serialize_sql_value(value, buffer)?;
        }

        Ok(())
    }

    fn serialize_sql_value(
        &self,
        value: &SqlValue,
        buffer: &mut Vec<u8>,
    ) -> Result<(), DatabaseError> {
        match value {
            SqlValue::Integer(i) => {
                buffer.push(0);
                buffer.extend_from_slice(&i.to_le_bytes());
            }
            SqlValue::Float(f) => {
                buffer.push(1);
                buffer.extend_from_slice(&f.to_le_bytes());
            }
            SqlValue::Text(s) => {
                buffer.push(2);
                let text_bytes = s.as_bytes();
                buffer.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
                buffer.extend_from_slice(text_bytes);
            }
            SqlValue::Boolean(b) => {
                buffer.push(3);
                buffer.push(if *b { 1 } else { 0 });
            }
            SqlValue::Null => {
                buffer.push(4);
            }
        }
        Ok(())
    }

    fn deserialize_tables(&self, buffer: &[u8]) -> Result<HashMap<String, Table>, DatabaseError> {
        let mut cursor = 0;
        let mut tables = HashMap::new();

        if buffer.len() < 4 {
            return Ok(tables);
        }

        let table_count = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        cursor += 4;

        for _ in 0..table_count {
            let (table, new_cursor) = self.deserialize_table(buffer, cursor)?;
            cursor = new_cursor;
            tables.insert(table.name.clone(), table);
        }

        Ok(tables)
    }

    fn deserialize_table(
        &self,
        buffer: &[u8],
        mut cursor: usize,
    ) -> Result<(Table, usize), DatabaseError> {
        if cursor + 4 > buffer.len() {
            return Err(DatabaseError::IoError("Invalid table data".to_string()));
        }

        let name_len = u32::from_le_bytes([
            buffer[cursor],
            buffer[cursor + 1],
            buffer[cursor + 2],
            buffer[cursor + 3],
        ]) as usize;
        cursor += 4;

        if cursor + name_len > buffer.len() {
            return Err(DatabaseError::IoError(
                "Invalid table name data".to_string(),
            ));
        }

        let name = String::from_utf8(buffer[cursor..cursor + name_len].to_vec())
            .map_err(|_| DatabaseError::IoError("Invalid UTF-8 in table name".to_string()))?;
        cursor += name_len;

        if cursor + 4 > buffer.len() {
            return Err(DatabaseError::IoError(
                "Invalid column count data".to_string(),
            ));
        }

        let column_count = u32::from_le_bytes([
            buffer[cursor],
            buffer[cursor + 1],
            buffer[cursor + 2],
            buffer[cursor + 3],
        ]);
        cursor += 4;

        let mut columns = Vec::new();
        for _ in 0..column_count {
            let (column, new_cursor) = self.deserialize_column_definition(buffer, cursor)?;
            cursor = new_cursor;
            columns.push(column);
        }

        if cursor + 4 > buffer.len() {
            return Err(DatabaseError::IoError("Invalid row count data".to_string()));
        }

        let row_count = u32::from_le_bytes([
            buffer[cursor],
            buffer[cursor + 1],
            buffer[cursor + 2],
            buffer[cursor + 3],
        ]);
        cursor += 4;

        let mut rows = Vec::new();
        for _ in 0..row_count {
            let (row, new_cursor) = self.deserialize_row(buffer, cursor)?;
            cursor = new_cursor;
            rows.push(row);
        }

        let mut index_manager = IndexManager::new();

        for column in &columns {
            if column.primary_key {
                let index_name = format!("pk_{}", column.name);
                index_manager.create_index(index_name, column.name.clone(), true, true)?;
            } else if !column.nullable {
                let index_name = format!("idx_{}_{}", name, column.name);
                index_manager.create_index(index_name, column.name.clone(), false, false)?;
            }
        }

        let mut table = Table {
            name,
            columns,
            rows,
            index_manager,
            next_row_id: row_count as usize,
        };

        let table_snapshot: Vec<(HashMap<String, SqlValue>, usize)> = table
            .rows
            .iter()
            .enumerate()
            .map(|(row_id, row)| (row.columns.clone(), row_id))
            .collect();

        table.index_manager.rebuild_all_indexes(&table_snapshot)?;

        table.next_row_id = table.rows.len();

        Ok((table, cursor))
    }

    fn deserialize_column_definition(
        &self,
        buffer: &[u8],
        mut cursor: usize,
    ) -> Result<(ColumnDefinition, usize), DatabaseError> {
        if cursor + 4 > buffer.len() {
            return Err(DatabaseError::IoError(
                "Invalid column definition data".to_string(),
            ));
        }

        let name_len = u32::from_le_bytes([
            buffer[cursor],
            buffer[cursor + 1],
            buffer[cursor + 2],
            buffer[cursor + 3],
        ]) as usize;
        cursor += 4;

        if cursor + name_len + 3 > buffer.len() {
            return Err(DatabaseError::IoError(
                "Invalid column definition data".to_string(),
            ));
        }

        let name = String::from_utf8(buffer[cursor..cursor + name_len].to_vec())
            .map_err(|_| DatabaseError::IoError("Invalid UTF-8 in column name".to_string()))?;
        cursor += name_len;

        let data_type = match buffer[cursor] {
            0 => DataType::Integer,
            1 => DataType::Float,
            2 => DataType::Text,
            3 => DataType::Boolean,
            _ => return Err(DatabaseError::IoError("Invalid data type".to_string())),
        };
        cursor += 1;

        let nullable = buffer[cursor] == 1;
        cursor += 1;

        let primary_key = buffer[cursor] == 1;
        cursor += 1;

        let column = ColumnDefinition {
            name,
            data_type,
            nullable,
            primary_key,
        };

        Ok((column, cursor))
    }

    fn deserialize_row(
        &self,
        buffer: &[u8],
        mut cursor: usize,
    ) -> Result<(Row, usize), DatabaseError> {
        if cursor + 4 > buffer.len() {
            return Err(DatabaseError::IoError("Invalid row data".to_string()));
        }

        let column_count = u32::from_le_bytes([
            buffer[cursor],
            buffer[cursor + 1],
            buffer[cursor + 2],
            buffer[cursor + 3],
        ]);
        cursor += 4;

        let mut columns = HashMap::new();

        for _ in 0..column_count {
            if cursor + 4 > buffer.len() {
                return Err(DatabaseError::IoError(
                    "Invalid row column data".to_string(),
                ));
            }

            let name_len = u32::from_le_bytes([
                buffer[cursor],
                buffer[cursor + 1],
                buffer[cursor + 2],
                buffer[cursor + 3],
            ]) as usize;
            cursor += 4;

            if cursor + name_len > buffer.len() {
                return Err(DatabaseError::IoError(
                    "Invalid row column name data".to_string(),
                ));
            }

            let column_name = String::from_utf8(buffer[cursor..cursor + name_len].to_vec())
                .map_err(|_| DatabaseError::IoError("Invalid UTF-8 in column name".to_string()))?;
            cursor += name_len;

            let (value, new_cursor) = self.deserialize_sql_value(buffer, cursor)?;
            cursor = new_cursor;

            columns.insert(column_name, value);
        }

        let row = Row { columns };
        Ok((row, cursor))
    }

    fn deserialize_sql_value(
        &self,
        buffer: &[u8],
        mut cursor: usize,
    ) -> Result<(SqlValue, usize), DatabaseError> {
        if cursor >= buffer.len() {
            return Err(DatabaseError::IoError("Invalid SQL value data".to_string()));
        }

        let value_type = buffer[cursor];
        cursor += 1;

        let value = match value_type {
            0 => {
                if cursor + 8 > buffer.len() {
                    return Err(DatabaseError::IoError("Invalid integer data".to_string()));
                }
                let int_val = i64::from_le_bytes([
                    buffer[cursor],
                    buffer[cursor + 1],
                    buffer[cursor + 2],
                    buffer[cursor + 3],
                    buffer[cursor + 4],
                    buffer[cursor + 5],
                    buffer[cursor + 6],
                    buffer[cursor + 7],
                ]);
                cursor += 8;
                SqlValue::Integer(int_val)
            }
            1 => {
                if cursor + 8 > buffer.len() {
                    return Err(DatabaseError::IoError("Invalid float data".to_string()));
                }
                let float_val = f64::from_le_bytes([
                    buffer[cursor],
                    buffer[cursor + 1],
                    buffer[cursor + 2],
                    buffer[cursor + 3],
                    buffer[cursor + 4],
                    buffer[cursor + 5],
                    buffer[cursor + 6],
                    buffer[cursor + 7],
                ]);
                cursor += 8;
                SqlValue::Float(float_val)
            }
            2 => {
                if cursor + 4 > buffer.len() {
                    return Err(DatabaseError::IoError(
                        "Invalid text length data".to_string(),
                    ));
                }
                let text_len = u32::from_le_bytes([
                    buffer[cursor],
                    buffer[cursor + 1],
                    buffer[cursor + 2],
                    buffer[cursor + 3],
                ]) as usize;
                cursor += 4;

                if cursor + text_len > buffer.len() {
                    return Err(DatabaseError::IoError("Invalid text data".to_string()));
                }

                let text_val = String::from_utf8(buffer[cursor..cursor + text_len].to_vec())
                    .map_err(|_| {
                        DatabaseError::IoError("Invalid UTF-8 in text value".to_string())
                    })?;
                cursor += text_len;
                SqlValue::Text(text_val)
            }
            3 => {
                if cursor >= buffer.len() {
                    return Err(DatabaseError::IoError("Invalid boolean data".to_string()));
                }
                let bool_val = buffer[cursor] == 1;
                cursor += 1;
                SqlValue::Boolean(bool_val)
            }
            4 => SqlValue::Null,
            _ => return Err(DatabaseError::IoError("Unknown SQL value type".to_string())),
        };

        Ok((value, cursor))
    }
}
