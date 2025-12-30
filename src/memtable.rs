use std::collections::BTreeMap;
use std::collections::btree_map;

/// Represents a value in the memtable
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// An actual value
    Some(Vec<u8>),
    /// A tombstone marking a deletion
    Tombstone,
}

/// In-memory write buffer using a `BTreeMap` for sorted storage
pub struct Memtable {
    /// Sorted map of keys to values
    data: BTreeMap<Vec<u8>, Value>,
    /// Approximate size in bytes
    size_bytes: usize,
}

impl Memtable {
    /// Creates a new empty memtable
    pub const fn new() -> Self {
        Self {
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
            self.size_bytes += key.len() + value.len();
        }

        self.data.insert(key, Value::Some(value));
    }

    /// Get value of a key
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.data.get(key)
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

    /// Returns iterator over the memtalbe
    pub fn iter(&self) -> btree_map::Iter<Vec<u8>, Value> {
        self.data.iter()
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
    pub const fn size_bytes(&self) -> usize {
        self.size_bytes
    }
}

impl<'a> IntoIterator for &'a Memtable {
    type Item = (&'a Vec<u8>, &'a Value);
    type IntoIter = btree_map::Iter<'a, Vec<u8>, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_get() {
        let mut memtable = Memtable::new();
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();

        memtable.put(key.clone(), value.clone());
        let result = memtable.get(&key);

        assert_eq!(result, Some(&Value::Some(value)));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let memtable = Memtable::new();
        let key = b"nonexistent".to_vec();

        let result = memtable.get(&key);

        assert_eq!(result, None);
    }

    #[test]
    fn test_delete_creates_tombstone() {
        let mut memtable = Memtable::new();
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();

        // insert, then delete
        memtable.put(key.clone(), value);
        memtable.delete(key.clone());

        // should be a tombstone internally
        assert_eq!(memtable.get(&key), Some(&Value::Tombstone));
    }

    #[test]
    fn test_get_after_delete() {
        let mut memtable = Memtable::new();
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();

        // insert, then delete
        memtable.put(key.clone(), value);
        memtable.delete(key.clone());

        // should return tombstone
        let result = memtable.get(&key);
        assert_eq!(result, Some(&Value::Tombstone));
    }

    #[test]
    fn test_overwrite() {
        let mut memtable = Memtable::new();
        let key = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        // insert, then overwrite
        memtable.put(key.clone(), value1);
        memtable.put(key.clone(), value2.clone());

        // should be the second value
        let result = memtable.get(&key);
        assert_eq!(result, Some(&Value::Some(value2)));

        // should be exactly 1 entry
        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn test_ordering() {
        let mut memtable = Memtable::new();

        // insert unsorted keys
        memtable.put(b"key3".to_vec(), b"value3".to_vec());
        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        memtable.put(b"key2".to_vec(), b"value2".to_vec());

        // should be sorted internally
        let keys: Vec<_> = memtable.iter().map(|(k, _v)| k.clone()).collect();

        assert_eq!(
            keys,
            vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]
        );
    }

    #[test]
    fn test_size_bytes() {
        let mut memtable = Memtable::new();

        // should be initially empty
        assert_eq!(memtable.size_bytes(), 0);

        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        memtable.put(key1.clone(), value1.clone());
        assert_eq!(memtable.size_bytes(), key1.len() + value1.len());

        // overwrite with larger value
        let value2 = b"larger_value".to_vec();
        let expected_size = key1.len() + value2.len();
        memtable.put(key1.clone(), value2);
        assert_eq!(memtable.size_bytes(), expected_size);

        let key2 = b"key2".to_vec();
        let value3 = b"value3".to_vec();
        memtable.put(key2.clone(), value3.clone());
        assert_eq!(
            memtable.size_bytes(),
            expected_size + key2.len() + value3.len()
        );

        // delete should update size
        memtable.delete(key1.clone());
        assert_eq!(
            memtable.size_bytes(),
            key1.len() + key2.len() + value3.len()
        );
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut memtable = Memtable::new();

        assert!(memtable.is_empty());
        assert_eq!(memtable.len(), 0);

        memtable.put(b"key1".to_vec(), b"value1".to_vec());
        assert!(!memtable.is_empty());
        assert_eq!(memtable.len(), 1);

        memtable.put(b"key2".to_vec(), b"value2".to_vec());
        assert_eq!(memtable.len(), 2);

        // delete should still count as entry (tombstone
        memtable.delete(b"key1".to_vec());
        assert_eq!(memtable.len(), 2);
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut memtable = Memtable::new();
        let key = b"nonexistent".to_vec();

        memtable.delete(key.clone());

        // deleting a nonexistent key should create a tombstone
        assert_eq!(memtable.get(&key), Some(&Value::Tombstone));
        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn test_delete_twice() {
        let mut memtable = Memtable::new();
        let key = b"key1".to_vec();

        memtable.put(key.clone(), b"value1".to_vec());
        memtable.delete(key.clone());

        memtable.delete(key.clone());

        // should still be a tombstone
        assert_eq!(memtable.get(&key), Some(&Value::Tombstone));
    }
}
