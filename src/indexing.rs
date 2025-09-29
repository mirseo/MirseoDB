use super::core_types::{DatabaseError, SqlValue, ComparisonOperator, WhereClause};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct BTreeIndex {
    pub name: String,
    pub column_name: String,
    pub is_unique: bool,
    pub is_primary: bool,
    tree: BTreeMap<IndexKey, Vec<usize>>,
}

#[derive(Debug, Clone)]
pub struct CompositeIndex {
    pub name: String,
    pub column_names: Vec<String>,
    pub is_unique: bool,
    tree: BTreeMap<CompositeKey, Vec<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompositeKey {
    keys: Vec<IndexKey>,
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
pub struct QueryPlan {
    pub selected_indexes: Vec<String>,
    pub estimated_cost: f64,
    pub scan_type: ScanType,
}

#[derive(Debug, Clone)]
pub enum ScanType {
    IndexScan,
    CompositeIndexScan,
    FullTableScan,
    IndexIntersection,
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

impl CompositeKey {
    pub fn new(keys: Vec<IndexKey>) -> Self {
        Self { keys }
    }

    pub fn from_values(values: &[&SqlValue]) -> Self {
        let keys = values.iter().map(|v| IndexKey::from(*v)).collect();
        Self::new(keys)
    }

    pub fn prefix_match(&self, prefix: &[IndexKey]) -> bool {
        if prefix.len() > self.keys.len() {
            return false;
        }
        self.keys[..prefix.len()] == *prefix
    }
}

impl CompositeIndex {
    pub fn new(name: String, column_names: Vec<String>, is_unique: bool) -> Self {
        Self {
            name,
            column_names,
            is_unique,
            tree: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, values: &[&SqlValue], row_id: usize) -> Result<(), DatabaseError> {
        if values.len() != self.column_names.len() {
            return Err(DatabaseError::InvalidDataType(
                "Column count mismatch for composite index".to_string(),
            ));
        }

        let composite_key = CompositeKey::from_values(values);

        if self.is_unique {
            if let Some(existing_rows) = self.tree.get(&composite_key) {
                if !existing_rows.is_empty() {
                    return Err(DatabaseError::UniqueConstraintViolation(format!(
                        "Duplicate value for unique composite index '{}'",
                        self.name
                    )));
                }
            }
        }

        self.tree
            .entry(composite_key)
            .or_insert_with(Vec::new)
            .push(row_id);
        Ok(())
    }

    pub fn find_exact(&self, values: &[&SqlValue]) -> Vec<usize> {
        let composite_key = CompositeKey::from_values(values);
        self.tree.get(&composite_key).cloned().unwrap_or_default()
    }

    pub fn find_prefix(&self, prefix_values: &[&SqlValue]) -> Vec<usize> {
        let prefix_keys: Vec<IndexKey> = prefix_values.iter().map(|v| IndexKey::from(*v)).collect();
        let mut result = Vec::new();

        for (key, row_ids) in &self.tree {
            if key.prefix_match(&prefix_keys) {
                result.extend(row_ids);
            }
        }

        result
    }

    pub fn find_range_composite(
        &self,
        start_values: Option<&[&SqlValue]>,
        end_values: Option<&[&SqlValue]>,
    ) -> Vec<usize> {
        let start_key = start_values.map(CompositeKey::from_values);
        let end_key = end_values.map(CompositeKey::from_values);
        let mut result = Vec::new();

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

    pub fn remove(&mut self, values: &[&SqlValue], row_id: usize) {
        let composite_key = CompositeKey::from_values(values);
        if let Some(row_ids) = self.tree.get_mut(&composite_key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                self.tree.remove(&composite_key);
            }
        }
    }

    pub fn size(&self) -> usize {
        self.tree.len()
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
    composite_indexes: Vec<CompositeIndex>,
    query_optimizer: QueryOptimizer,
}

#[derive(Debug, Clone)]
pub struct QueryOptimizer {
    index_usage_stats: HashMap<String, IndexUsageStats>,
    cost_model: CostModel,
}

#[derive(Debug, Clone)]
pub struct IndexUsageStats {
    pub access_count: u64,
    pub selectivity: f64,
    pub last_used: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct CostModel {
    pub index_scan_cost: f64,
    pub table_scan_cost: f64,
    pub composite_index_cost: f64,
    pub intersection_cost: f64,
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            index_usage_stats: HashMap::new(),
            cost_model: CostModel {
                index_scan_cost: 1.0,
                table_scan_cost: 10.0,
                composite_index_cost: 0.8,
                intersection_cost: 5.0,
            },
        }
    }

    pub fn analyze_where_clauses(&self, where_clauses: &[WhereClause]) -> QueryPlan {
        let mut best_plan = QueryPlan {
            selected_indexes: Vec::new(),
            estimated_cost: f64::MAX,
            scan_type: ScanType::FullTableScan,
        };

        let columns: HashSet<String> = where_clauses.iter().map(|w| w.column.clone()).collect();

        for (index_name, stats) in &self.index_usage_stats {
            let cost = self.calculate_index_cost(stats, where_clauses.len());
            if cost < best_plan.estimated_cost {
                best_plan = QueryPlan {
                    selected_indexes: vec![index_name.clone()],
                    estimated_cost: cost,
                    scan_type: ScanType::IndexScan,
                };
            }
        }

        if columns.len() > 1 {
            let intersection_cost = self.cost_model.intersection_cost * columns.len() as f64;
            if intersection_cost < best_plan.estimated_cost {
                best_plan = QueryPlan {
                    selected_indexes: columns.into_iter().collect(),
                    estimated_cost: intersection_cost,
                    scan_type: ScanType::IndexIntersection,
                };
            }
        }

        best_plan
    }

    fn calculate_index_cost(&self, stats: &IndexUsageStats, clause_count: usize) -> f64 {
        let base_cost = self.cost_model.index_scan_cost;
        let selectivity_factor = 1.0 - stats.selectivity;
        let usage_factor = (stats.access_count as f64).log10().max(1.0);

        base_cost * selectivity_factor * clause_count as f64 / usage_factor
    }

    pub fn update_index_stats(&mut self, index_name: &str, selectivity: f64) {
        let stats = self.index_usage_stats.entry(index_name.to_string()).or_insert(
            IndexUsageStats {
                access_count: 0,
                selectivity: 1.0,
                last_used: std::time::Instant::now(),
            }
        );

        stats.access_count += 1;
        stats.selectivity = (stats.selectivity + selectivity) / 2.0;
        stats.last_used = std::time::Instant::now();
    }
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: Vec::new(),
            composite_indexes: Vec::new(),
            query_optimizer: QueryOptimizer::new(),
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
        column_values: &HashMap<String, SqlValue>,
        row_id: usize,
    ) -> Result<(), DatabaseError> {
        for index in &mut self.indexes {
            if let Some(value) = column_values.get(&index.column_name) {
                index.insert(value, row_id)?;
            }
        }

        self.insert_into_composite_indexes(column_values, row_id)?;
        Ok(())
    }

    pub fn remove_from_indexes(
        &mut self,
        column_values: &HashMap<String, SqlValue>,
        row_id: usize,
    ) {
        for index in &mut self.indexes {
            if let Some(value) = column_values.get(&index.column_name) {
                index.remove(value, row_id);
            }
        }

        for composite_idx in &mut self.composite_indexes {
            let values: Vec<&SqlValue> = composite_idx
                .column_names
                .iter()
                .filter_map(|col| column_values.get(col))
                .collect();

            if values.len() == composite_idx.column_names.len() {
                composite_idx.remove(&values, row_id);
            }
        }
    }

    pub fn update_indexes(
        &mut self,
        old_values: &HashMap<String, SqlValue>,
        new_values: &HashMap<String, SqlValue>,
        row_id: usize,
    ) -> Result<(), DatabaseError> {
        self.remove_from_indexes(old_values, row_id);
        self.insert_into_indexes(new_values, row_id)?;
        Ok(())
    }

    pub fn rebuild_all_indexes(
        &mut self,
        table_data: &[(HashMap<String, SqlValue>, usize)],
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

        for composite_idx in &mut self.composite_indexes {
            composite_idx.tree.clear();
            for (row, row_id) in table_data {
                let values: Vec<&SqlValue> = composite_idx
                    .column_names
                    .iter()
                    .filter_map(|col| row.get(col))
                    .collect();

                if values.len() == composite_idx.column_names.len() {
                    composite_idx.insert(&values, *row_id)?;
                }
            }
        }
        Ok(())
    }

    pub fn create_composite_index(
        &mut self,
        name: String,
        column_names: Vec<String>,
        is_unique: bool,
    ) -> Result<(), DatabaseError> {
        if self.composite_indexes.iter().any(|idx| idx.name == name) {
            return Err(DatabaseError::IndexAlreadyExists(name));
        }

        let index = CompositeIndex::new(name, column_names, is_unique);
        self.composite_indexes.push(index);
        Ok(())
    }

    pub fn get_composite_index(&self, name: &str) -> Option<&CompositeIndex> {
        self.composite_indexes.iter().find(|idx| idx.name == name)
    }

    pub fn get_composite_index_mut(&mut self, name: &str) -> Option<&mut CompositeIndex> {
        self.composite_indexes.iter_mut().find(|idx| idx.name == name)
    }

    pub fn find_best_composite_index(&self, column_names: &[String]) -> Option<&CompositeIndex> {
        let mut best_match: Option<&CompositeIndex> = None;
        let mut best_score = 0;

        for composite_idx in &self.composite_indexes {
            let mut score = 0;
            for (i, col) in column_names.iter().enumerate() {
                if i < composite_idx.column_names.len() && composite_idx.column_names[i] == *col {
                    score += 10 - i;
                } else if composite_idx.column_names.contains(col) {
                    score += 1;
                }
            }

            if score > best_score {
                best_score = score;
                best_match = Some(composite_idx);
            }
        }

        best_match
    }

    pub fn find_best_index_for_query(&self, column_name: &str) -> Option<&BTreeIndex> {
        let mut candidates: Vec<&BTreeIndex> = self.get_indexes_for_column(column_name);

        candidates.sort_by(|a, b| {
            let a_stats = self.query_optimizer.index_usage_stats.get(&a.name);
            let b_stats = self.query_optimizer.index_usage_stats.get(&b.name);

            match (a_stats, b_stats) {
                (Some(a_stat), Some(b_stat)) => {
                    let a_score = a_stat.access_count as f64 * (1.0 - a_stat.selectivity);
                    let b_score = b_stat.access_count as f64 * (1.0 - b_stat.selectivity);
                    b_score.partial_cmp(&a_score).unwrap_or(Ordering::Equal)
                }
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                _ => match (a.is_primary, b.is_primary) {
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    _ => match (a.is_unique, b.is_unique) {
                        (true, false) => Ordering::Less,
                        (false, true) => Ordering::Greater,
                        _ => Ordering::Equal,
                    },
                },
            }
        });

        candidates.first().copied()
    }

    pub fn optimize_multi_column_query(
        &mut self,
        where_clauses: &[WhereClause],
        hint: Option<&IndexHint>,
    ) -> QueryPlan {
        if let Some(hint) = hint {
            return self.apply_index_hint(hint, where_clauses);
        }

        let columns: Vec<String> = where_clauses.iter().map(|w| w.column.clone()).collect();

        if let Some(composite_idx) = self.find_best_composite_index(&columns) {
            let index_name = composite_idx.name.clone();
            let selectivity = self.estimate_composite_selectivity(composite_idx, where_clauses);
            self.query_optimizer.update_index_stats(&index_name, selectivity);

            return QueryPlan {
                selected_indexes: vec![index_name],
                estimated_cost: self.query_optimizer.cost_model.composite_index_cost * selectivity,
                scan_type: ScanType::CompositeIndexScan,
            };
        }

        self.query_optimizer.analyze_where_clauses(where_clauses)
    }

    fn apply_index_hint(&self, hint: &IndexHint, where_clauses: &[WhereClause]) -> QueryPlan {
        match hint.hint_type {
            IndexHintType::Force => QueryPlan {
                selected_indexes: hint.index_names.clone(),
                estimated_cost: 1.0,
                scan_type: ScanType::IndexScan,
            },
            IndexHintType::Use => {
                let mut plan = self.query_optimizer.analyze_where_clauses(where_clauses);
                plan.selected_indexes.retain(|idx| hint.index_names.contains(idx));
                plan
            },
            IndexHintType::Ignore => {
                let mut plan = self.query_optimizer.analyze_where_clauses(where_clauses);
                plan.selected_indexes.retain(|idx| !hint.index_names.contains(idx));
                if plan.selected_indexes.is_empty() {
                    plan.scan_type = ScanType::FullTableScan;
                    plan.estimated_cost = self.query_optimizer.cost_model.table_scan_cost;
                }
                plan
            },
        }
    }

    fn estimate_composite_selectivity(&self, _composite_idx: &CompositeIndex, where_clauses: &[WhereClause]) -> f64 {
        let base_selectivity = 0.1;
        let clause_factor = 1.0 / (where_clauses.len() as f64).sqrt();
        (base_selectivity * clause_factor).min(1.0)
    }

    pub fn insert_into_composite_indexes(
        &mut self,
        column_values: &HashMap<String, SqlValue>,
        row_id: usize,
    ) -> Result<(), DatabaseError> {
        for composite_idx in &mut self.composite_indexes {
            let values: Vec<&SqlValue> = composite_idx
                .column_names
                .iter()
                .filter_map(|col| column_values.get(col))
                .collect();

            if values.len() == composite_idx.column_names.len() {
                composite_idx.insert(&values, row_id)?;
            }
        }
        Ok(())
    }

    pub fn get_query_optimizer_stats(&self) -> &HashMap<String, IndexUsageStats> {
        &self.query_optimizer.index_usage_stats
    }

    pub fn reset_optimizer_stats(&mut self) {
        self.query_optimizer.index_usage_stats.clear();
    }

    pub fn list_indexes(&self) -> Vec<&BTreeIndex> {
        self.indexes.iter().collect()
    }

    pub fn list_composite_indexes(&self) -> Vec<&CompositeIndex> {
        self.composite_indexes.iter().collect()
    }

    pub fn list_all_indexes(&self) -> (Vec<&BTreeIndex>, Vec<&CompositeIndex>) {
        (self.list_indexes(), self.list_composite_indexes())
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}
