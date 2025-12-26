use std::collections::BTreeMap;

/// Represents a value in the memtable
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// An actual value
    Some(Vec<u8>),
    /// A tombstone marking a deletion
    Tombstone,
}

/// In-memory write buffer using a BTreeMap for sorted storage
pub struct Memtable {
    /// Sorted map of keys to values
    data: BTreeMap<Vec<u8>, Value>,
    /// Approximate size in bytes
    size_bytes: usize,
}

impl Memtable {
    /// Creates a new empty memtable
    pub fn new() -> Self {
        Memtable {
            data: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Insert a KV-pair
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if let Some(old_value) = self.data.get(&key) {
            match old_value {
                Value::Some(old_value) => self.size_bytes -= old_value.len(),
                Value::Tombstone => {}
            }

            self.size_bytes += value.len();
        } else {
            // no replacement -> add full kv-pair-size
            self.size_bytes += key.len() + value.len()
        }

        self.data.insert(key, Value::Some(value));
    }

    /// Get value of a key
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.data.get(key) {
            Some(Value::Some(value)) => Some(value.clone()),
            Some(Value::Tombstone) | None => None,
        }
    }

    /// Delete an entry by key
    pub fn delete(&mut self, key: Vec<u8>) {
        if let Some(old_value) = self.data.get(&key) {
            match old_value {
                Value::Some(old_value) => self.size_bytes -= old_value.len(),
                Value::Tombstone => {
                    return;
                }
            }
        } else {
            self.size_bytes += key.len();
        }

        self.data.insert(key, Value::Tombstone);
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if memtable is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the number of bytes
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}
