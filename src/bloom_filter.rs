use super::core_types::{SqlValue, DatabaseError};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bit_array: Vec<bool>,
    size: usize,
    hash_functions: usize,
    element_count: usize,
}

impl BloomFilter {
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        let size = Self::optimal_size(expected_elements, false_positive_rate);
        let hash_functions = Self::optimal_hash_functions(size, expected_elements);

        Self {
            bit_array: vec![false; size],
            size,
            hash_functions,
            element_count: 0,
        }
    }

    pub fn new_with_params(size: usize, hash_functions: usize) -> Self {
        Self {
            bit_array: vec![false; size],
            size,
            hash_functions,
            element_count: 0,
        }
    }

    fn optimal_size(expected_elements: usize, false_positive_rate: f64) -> usize {
        let m = -(expected_elements as f64 * false_positive_rate.ln()) / (2.0_f64.ln().powi(2));
        m.ceil() as usize
    }

    fn optimal_hash_functions(size: usize, expected_elements: usize) -> usize {
        let k = (size as f64 / expected_elements as f64) * 2.0_f64.ln();
        k.ceil() as usize
    }

    pub fn insert(&mut self, value: &SqlValue) {
        for i in 0..self.hash_functions {
            let hash = self.hash_value(value, i);
            let index = hash % self.size;
            self.bit_array[index] = true;
        }
        self.element_count += 1;
    }

    pub fn contains(&self, value: &SqlValue) -> bool {
        for i in 0..self.hash_functions {
            let hash = self.hash_value(value, i);
            let index = hash % self.size;
            if !self.bit_array[index] {
                return false;
            }
        }
        true
    }

    fn hash_value(&self, value: &SqlValue, seed: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);

        match value {
            SqlValue::Integer(i) => i.hash(&mut hasher),
            SqlValue::Float(f) => f.to_bits().hash(&mut hasher),
            SqlValue::Text(s) => s.hash(&mut hasher),
            SqlValue::Boolean(b) => b.hash(&mut hasher),
            SqlValue::Null => 0u8.hash(&mut hasher),
        }

        hasher.finish() as usize
    }

    pub fn false_positive_probability(&self) -> f64 {
        let k = self.hash_functions as f64;
        let m = self.size as f64;
        let n = self.element_count as f64;

        (1.0 - (-k * n / m).exp()).powf(k)
    }

    pub fn is_empty(&self) -> bool {
        self.element_count == 0
    }

    pub fn clear(&mut self) {
        self.bit_array.fill(false);
        self.element_count = 0;
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn element_count(&self) -> usize {
        self.element_count
    }
}

#[derive(Debug, Clone)]
pub struct ColumnBloomFilter {
    filters: std::collections::HashMap<String, BloomFilter>,
    row_count: usize,
}

impl ColumnBloomFilter {
    pub fn new() -> Self {
        Self {
            filters: std::collections::HashMap::new(),
            row_count: 0,
        }
    }

    pub fn build_from_table(&mut self, table_data: &[(std::collections::HashMap<String, SqlValue>, usize)]) {
        self.clear();
        self.row_count = table_data.len();

        let expected_elements = self.row_count.max(1000);
        let false_positive_rate = 0.01;

        for (row, _) in table_data {
            for (column_name, value) in row {
                let filter = self.filters.entry(column_name.clone()).or_insert_with(|| {
                    BloomFilter::new(expected_elements, false_positive_rate)
                });
                filter.insert(value);
            }
        }
    }

    pub fn might_contain(&self, column: &str, value: &SqlValue) -> bool {
        match self.filters.get(column) {
            Some(filter) => filter.contains(value),
            None => false,
        }
    }

    pub fn can_skip_scan(&self, column: &str, value: &SqlValue) -> bool {
        match self.filters.get(column) {
            Some(filter) => !filter.contains(value),
            None => false,
        }
    }

    pub fn clear(&mut self) {
        self.filters.clear();
        self.row_count = 0;
    }

    pub fn get_column_stats(&self) -> Vec<(String, f64, usize)> {
        self.filters.iter().map(|(name, filter)| {
            (name.clone(), filter.false_positive_probability(), filter.element_count())
        }).collect()
    }

    pub fn rebuild_for_column(&mut self, column: &str, values: &[&SqlValue]) {
        let expected_elements = values.len().max(100);
        let false_positive_rate = 0.01;

        let mut filter = BloomFilter::new(expected_elements, false_positive_rate);
        for value in values {
            filter.insert(value);
        }

        self.filters.insert(column.to_string(), filter);
    }
}

#[derive(Debug, Clone)]
pub struct ChunkedTableScanner {
    chunk_size: usize,
    max_memory_mb: usize,
    early_termination_enabled: bool,
}

impl ChunkedTableScanner {
    pub fn new(chunk_size: usize, max_memory_mb: usize) -> Self {
        Self {
            chunk_size,
            max_memory_mb,
            early_termination_enabled: true,
        }
    }

    pub fn with_early_termination(mut self, enabled: bool) -> Self {
        self.early_termination_enabled = enabled;
        self
    }

    pub fn scan_with_bloom_filter<F, R>(
        &self,
        table_rows: &[super::core_types::Row],
        bloom_filter: &ColumnBloomFilter,
        where_clause: Option<&super::core_types::WhereClause>,
        limit: Option<usize>,
        mut processor: F,
    ) -> Result<Vec<R>, DatabaseError>
    where
        F: FnMut(&super::core_types::Row) -> Result<Option<R>, DatabaseError>,
    {
        let mut results = Vec::new();
        let mut processed_rows = 0;
        let effective_limit = limit.unwrap_or(usize::MAX);

        if let Some(where_clause) = where_clause {
            if bloom_filter.can_skip_scan(&where_clause.column, &where_clause.value) {
                return Ok(results);
            }
        }

        let total_chunks = (table_rows.len() + self.chunk_size - 1) / self.chunk_size;

        for (chunk_idx, chunk) in table_rows.chunks(self.chunk_size).enumerate() {
            if self.early_termination_enabled && results.len() >= effective_limit {
                break;
            }

            let memory_usage = self.estimate_chunk_memory_usage(chunk);
            if memory_usage > self.max_memory_mb * 1024 * 1024 {
                return Err(DatabaseError::QueryTooComplex);
            }

            let mut chunk_results = Vec::new();

            for row in chunk {
                if let Some(where_clause) = where_clause {
                    if !bloom_filter.might_contain(&where_clause.column, &where_clause.value) {
                        continue;
                    }
                }

                match processor(row)? {
                    Some(result) => {
                        chunk_results.push(result);
                        if self.early_termination_enabled && chunk_results.len() + results.len() >= effective_limit {
                            break;
                        }
                    }
                    None => {}
                }

                processed_rows += 1;
            }

            results.extend(chunk_results);

            if chunk_idx % 10 == 0 {
                println!(
                    "[MirseoDB] Processed chunk {}/{}, found {} results",
                    chunk_idx + 1, total_chunks, results.len()
                );
            }

            if self.early_termination_enabled && results.len() >= effective_limit {
                break;
            }
        }

        if self.early_termination_enabled && results.len() > effective_limit {
            results.truncate(effective_limit);
        }

        println!(
            "[MirseoDB] Scan completed: {} rows processed, {} results returned",
            processed_rows, results.len()
        );

        Ok(results)
    }

    fn estimate_chunk_memory_usage(&self, chunk: &[super::core_types::Row]) -> usize {
        let avg_row_size = if chunk.is_empty() {
            1024
        } else {
            chunk.iter().map(|row| self.estimate_row_size(row)).sum::<usize>() / chunk.len()
        };

        chunk.len() * avg_row_size
    }

    fn estimate_row_size(&self, row: &super::core_types::Row) -> usize {
        let mut size = std::mem::size_of::<super::core_types::Row>();

        for (key, value) in &row.columns {
            size += key.len() + match value {
                SqlValue::Integer(_) => 8,
                SqlValue::Float(_) => 8,
                SqlValue::Text(s) => s.len(),
                SqlValue::Boolean(_) => 1,
                SqlValue::Null => 0,
            };
        }

        size
    }

    pub fn adaptive_chunk_size(&self, total_rows: usize, available_memory_mb: usize) -> usize {
        let estimated_row_size = 1024;
        let max_chunk_size = (available_memory_mb * 1024 * 1024) / estimated_row_size;

        let adaptive_size = if total_rows < 10000 {
            self.chunk_size
        } else if total_rows < 100000 {
            self.chunk_size * 2
        } else {
            self.chunk_size * 4
        };

        adaptive_size.min(max_chunk_size).max(100)
    }
}

#[derive(Debug, Clone)]
pub struct ScanStatistics {
    pub total_rows_scanned: usize,
    pub rows_skipped_by_bloom: usize,
    pub chunks_processed: usize,
    pub early_termination_triggered: bool,
    pub scan_time_ms: u64,
    pub bloom_filter_hits: usize,
    pub bloom_filter_misses: usize,
}

impl ScanStatistics {
    pub fn new() -> Self {
        Self {
            total_rows_scanned: 0,
            rows_skipped_by_bloom: 0,
            chunks_processed: 0,
            early_termination_triggered: false,
            scan_time_ms: 0,
            bloom_filter_hits: 0,
            bloom_filter_misses: 0,
        }
    }

    pub fn bloom_filter_effectiveness(&self) -> f64 {
        if self.bloom_filter_hits + self.bloom_filter_misses == 0 {
            0.0
        } else {
            self.rows_skipped_by_bloom as f64 / (self.bloom_filter_hits + self.bloom_filter_misses) as f64
        }
    }

    pub fn print_summary(&self) {
        println!("[MirseoDB] Scan Statistics:");
        println!("  Total rows scanned: {}", self.total_rows_scanned);
        println!("  Rows skipped by Bloom filter: {}", self.rows_skipped_by_bloom);
        println!("  Chunks processed: {}", self.chunks_processed);
        println!("  Early termination: {}", self.early_termination_triggered);
        println!("  Scan time: {}ms", self.scan_time_ms);
        println!("  Bloom filter effectiveness: {:.2}%", self.bloom_filter_effectiveness() * 100.0);
    }
}