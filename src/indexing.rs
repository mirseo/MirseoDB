use super::core_types::{DatabaseError, SqlValue};
use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct BTreeIndex {
    pub name: String,
    pub column_name: String,
    pub is_unique: bool,
    pub is_primary: bool,
    tree: BTreeMap<IndexKey, Vec<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Integer(i64),
    Float(OrderedFloat),
    Text(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone)]
pub struct OrderedFloat(f64);

impl OrderedFloat {
    pub fn value(&self) -> f64 {
        self.0
    }
}

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl From<f64> for OrderedFloat {
    fn from(value: f64) -> Self {
        OrderedFloat(value)
    }
}

impl From<&SqlValue> for IndexKey {
    fn from(value: &SqlValue) -> Self {
        match value {
            SqlValue::Integer(i) => IndexKey::Integer(*i),
            SqlValue::Float(f) => IndexKey::Float(OrderedFloat(*f)),
            SqlValue::Text(s) => IndexKey::Text(s.clone()),
            SqlValue::Boolean(b) => IndexKey::Boolean(*b),
            SqlValue::Null => IndexKey::Null,
        }
    }
}

impl BTreeIndex {
    pub fn new(name: String, column_name: String, is_unique: bool, is_primary: bool) -> Self {
        Self {
            name,
            column_name,
            is_unique,
            is_primary,
            tree: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: &SqlValue, row_id: usize) -> Result<(), DatabaseError> {
        let index_key = IndexKey::from(key);

        if self.is_unique {
            if let Some(existing_rows) = self.tree.get(&index_key) {
                if !existing_rows.is_empty() {
                    return Err(DatabaseError::UniqueConstraintViolation(format!(
                        "Duplicate value for unique index '{}': {:?}",
                        self.name, key
                    )));
                }
            }
        }

        self.tree
            .entry(index_key)
            .or_insert_with(Vec::new)
            .push(row_id);
        Ok(())
    }

    pub fn remove(&mut self, key: &SqlValue, row_id: usize) {
        let index_key = IndexKey::from(key);
        if let Some(row_ids) = self.tree.get_mut(&index_key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                self.tree.remove(&index_key);
            }
        }
    }

    pub fn find_exact(&self, key: &SqlValue) -> Vec<usize> {
        let index_key = IndexKey::from(key);
        self.tree.get(&index_key).cloned().unwrap_or_default()
    }

    pub fn find_range(&self, start: Option<&SqlValue>, end: Option<&SqlValue>) -> Vec<usize> {
        let mut result = Vec::new();

        let start_key = start.map(IndexKey::from);
        let end_key = end.map(IndexKey::from);

        let iter = match (start_key.as_ref(), end_key.as_ref()) {
            (Some(start), Some(end)) => self.tree.range(start..=end),
            (Some(start), None) => self.tree.range(start..),
            (None, Some(end)) => self.tree.range(..=end),
            (None, None) => self.tree.range(..),
        };

        for (_, row_ids) in iter {
            result.extend(row_ids);
        }

        result
    }

    pub fn find_greater_than(&self, key: &SqlValue) -> Vec<usize> {
        let index_key = IndexKey::from(key);
        let mut result = Vec::new();

        for (_, row_ids) in self.tree.range((
            std::ops::Bound::Excluded(&index_key),
            std::ops::Bound::Unbounded,
        )) {
            result.extend(row_ids);
        }

        result
    }

    pub fn find_less_than(&self, key: &SqlValue) -> Vec<usize> {
        let index_key = IndexKey::from(key);
        let mut result = Vec::new();

        for (_, row_ids) in self.tree.range(..&index_key) {
            result.extend(row_ids);
        }

        result
    }

    pub fn get_all_keys(&self) -> Vec<IndexKey> {
        self.tree.keys().cloned().collect()
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }

    pub fn rebuild(&mut self, data: Vec<(&SqlValue, usize)>) -> Result<(), DatabaseError> {
        self.tree.clear();

        for (value, row_id) in data {
            self.insert(value, row_id)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct IndexManager {
    indexes: Vec<BTreeIndex>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: Vec::new(),
        }
    }

    pub fn create_index(
        &mut self,
        name: String,
        column_name: String,
        is_unique: bool,
        is_primary: bool,
    ) -> Result<(), DatabaseError> {
        if self.indexes.iter().any(|idx| idx.name == name) {
            return Err(DatabaseError::IndexAlreadyExists(name));
        }

        let index = BTreeIndex::new(name, column_name, is_unique, is_primary);
        self.indexes.push(index);
        Ok(())
    }

    pub fn drop_index(&mut self, name: &str) -> Result<(), DatabaseError> {
        let pos = self
            .indexes
            .iter()
            .position(|idx| idx.name == name)
            .ok_or_else(|| DatabaseError::IndexNotFound(name.to_string()))?;

        self.indexes.remove(pos);
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<&BTreeIndex> {
        self.indexes.iter().find(|idx| idx.name == name)
    }

    pub fn get_index_mut(&mut self, name: &str) -> Option<&mut BTreeIndex> {
        self.indexes.iter_mut().find(|idx| idx.name == name)
    }

    pub fn get_indexes_for_column(&self, column_name: &str) -> Vec<&BTreeIndex> {
        self.indexes
            .iter()
            .filter(|idx| idx.column_name == column_name)
            .collect()
    }

    pub fn get_primary_key_index(&self) -> Option<&BTreeIndex> {
        self.indexes.iter().find(|idx| idx.is_primary)
    }

    pub fn get_primary_key_index_mut(&mut self) -> Option<&mut BTreeIndex> {
        self.indexes.iter_mut().find(|idx| idx.is_primary)
    }

    pub fn insert_into_indexes(
        &mut self,
        column_values: &std::collections::HashMap<String, SqlValue>,
        row_id: usize,
    ) -> Result<(), DatabaseError> {
        for index in &mut self.indexes {
            if let Some(value) = column_values.get(&index.column_name) {
                index.insert(value, row_id)?;
            }
        }
        Ok(())
    }

    pub fn remove_from_indexes(
        &mut self,
        column_values: &std::collections::HashMap<String, SqlValue>,
        row_id: usize,
    ) {
        for index in &mut self.indexes {
            if let Some(value) = column_values.get(&index.column_name) {
                index.remove(value, row_id);
            }
        }
    }

    pub fn update_indexes(
        &mut self,
        old_values: &std::collections::HashMap<String, SqlValue>,
        new_values: &std::collections::HashMap<String, SqlValue>,
        row_id: usize,
    ) -> Result<(), DatabaseError> {
        self.remove_from_indexes(old_values, row_id);
        self.insert_into_indexes(new_values, row_id)?;
        Ok(())
    }

    pub fn rebuild_all_indexes(
        &mut self,
        table_data: &[(std::collections::HashMap<String, SqlValue>, usize)],
    ) -> Result<(), DatabaseError> {
        for index in &mut self.indexes {
            let index_data: Vec<(&SqlValue, usize)> = table_data
                .iter()
                .filter_map(|(row, row_id)| {
                    row.get(&index.column_name).map(|value| (value, *row_id))
                })
                .collect();

            index.rebuild(index_data)?;
        }
        Ok(())
    }

    pub fn find_best_index_for_query(&self, column_name: &str) -> Option<&BTreeIndex> {
        let mut candidates: Vec<&BTreeIndex> = self.get_indexes_for_column(column_name);

        candidates.sort_by(|a, b| match (a.is_primary, b.is_primary) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => match (a.is_unique, b.is_unique) {
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                _ => Ordering::Equal,
            },
        });

        candidates.first().copied()
    }

    pub fn list_indexes(&self) -> Vec<&BTreeIndex> {
        self.indexes.iter().collect()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}
