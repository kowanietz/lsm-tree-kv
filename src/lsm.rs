use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Result;
use crate::memtable::{Memtable, Value};
use crate::sstable::{SSTable, SSTableBuilder};

const MEMTABLE_SIZE_THRESHOLD: usize = 4096; // 4KB

/// The main LSM-Tree structure
pub struct LSMTree {
    /// Active in-memory table
    memtable: Memtable,
    /// Queue of flushed SSTables (L0)
    l0_sstables: VecDeque<SSTable>,
    /// Path to the data directory
    data_dir: PathBuf,
    /// A counter to generate unique sstable file names
    sst_counter: AtomicUsize,
}

impl LSMTree {
    /// Opens LSM-Tree at the given path.
    ///
    /// creates the directory if it doesn't exist and
    /// recovers the state from any existing SSTable files.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data_dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        // find all existing sstbales
        let mut sst_paths: Vec<PathBuf> = fs::read_dir(&data_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "sst"))
            .collect();

        // sort -> creation order
        sst_paths.sort();

        // Deque is used here since pushing elements to the front has a O(1) complexity while Vec has O(n)
        let mut l0_sstables = VecDeque::new();
        let mut max_sst_num = 0;

        for path in sst_paths {
            let sst = SSTable::open(path)?;
            if let Some(num_str) = sst.path().file_stem().and_then(|s| s.to_str()) {
                if let Ok(num) = num_str.parse::<usize>() {
                    if num > max_sst_num {
                        max_sst_num = num;
                    }
                }
            }
            // O(1)
            l0_sstables.push_front(sst);
        }

        Ok(Self {
            memtable: Memtable::new(),
            l0_sstables,
            data_dir,
            sst_counter: AtomicUsize::new(max_sst_num + 1),
        })
    }

    /// Retrieves a value for a given key.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. check active memtable
        if let Some(value) = self.memtable.get(key) {
            return match value {
                Value::Some(v) => Ok(Some(v.clone())),
                Value::Tombstone => Ok(None),
            };
        }

        // 2. check L0 SSTables from newest to oldest
        for sstable in &mut self.l0_sstables {
            match sstable.get(key)? {
                Some(Value::Some(value)) => return Ok(Some(value)),
                Some(Value::Tombstone) => return Ok(None),
                None => continue,
            }
        }

        // 3. nothing found :(
        Ok(None)
    }

    /// Inserts a key-value pair.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.memtable.put(key, value);

        if self.memtable.size_bytes() >= MEMTABLE_SIZE_THRESHOLD {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Deletes a key.
    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.memtable.delete(key);

        if self.memtable.size_bytes() >= MEMTABLE_SIZE_THRESHOLD {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Flushes the current memtable to a new L0 SSTable.
    fn flush_memtable(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let sst_num = self.sst_counter.fetch_add(1, Ordering::SeqCst);
        let sst_path = self.data_dir.join(format!("{:08}.sst", sst_num));

        // flush memtable to new SSTable
        let mut builder = SSTableBuilder::new(sst_path.clone())?;
        for (key, value) in self.memtable.iter() {
            builder.add(key, value)?;
        }
        builder.finish()?;

        // add new SSTable to L0 list
        let new_sstable = SSTable::open(sst_path)?;
        self.l0_sstables.push_front(new_sstable);

        // clear memtable
        self.memtable = Memtable::new();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("lsm-tree-kv-test").join(name);

        // clean up before test
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }

        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_open_creates_dir() {
        let path = temp_dir("open_creates_dir");
        LSMTree::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_put_and_get_memtable_only() {
        let path = temp_dir("put_get_memtable");
        let mut tree = LSMTree::open(path).unwrap();

        tree.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        let val = tree.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_memtable_flush() {
        let path = temp_dir("memtable_flush");
        let mut tree = LSMTree::open(path.clone()).unwrap();

        // small put, does not trigger a flush
        tree.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(tree.l0_sstables.len(), 0);

        // large put to trigger a flush
        let big_value = vec![0u8; MEMTABLE_SIZE_THRESHOLD];
        tree.put(b"key2".to_vec(), big_value).unwrap();

        // Memtable should be flushed and a new one created
        assert_eq!(tree.l0_sstables.len(), 1);
        assert!(tree.memtable.is_empty());

        // SSTable file should exist
        let sst_files: Vec<_> = fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "sst"))
            .collect();
        assert_eq!(sst_files.len(), 1);
    }

    #[test]
    fn test_get_after_flush() {
        let path = temp_dir("get_after_flush");
        let mut tree = LSMTree::open(path).unwrap();

        tree.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        // flush
        tree.put(b"key2".to_vec(), vec![0u8; MEMTABLE_SIZE_THRESHOLD])
            .unwrap();

        // key1 should now be in an SSTable
        assert!(tree.memtable.is_empty());
        let val = tree.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_multiple_flushes() {
        let path = temp_dir("multiple_flushes");
        let mut tree = LSMTree::open(path).unwrap();

        // flush
        tree.put(b"key1".to_vec(), vec![0u8; MEMTABLE_SIZE_THRESHOLD])
            .unwrap();
        assert_eq!(tree.l0_sstables.len(), 1);

        // flushq again
        tree.put(b"key2".to_vec(), vec![0u8; MEMTABLE_SIZE_THRESHOLD])
            .unwrap();
        assert_eq!(tree.l0_sstables.len(), 2);

        // check values from both SSTables
        let val1 = tree.get(b"key1").unwrap();
        assert_eq!(val1, Some(vec![0u8; MEMTABLE_SIZE_THRESHOLD]));
        let val2 = tree.get(b"key2").unwrap();
        assert_eq!(val2, Some(vec![0u8; MEMTABLE_SIZE_THRESHOLD]));
    }

    #[test]
    fn test_read_priority_memtable_over_sstable() {
        let path = temp_dir("read_priority");
        let mut tree = LSMTree::open(path).unwrap();

        // put initial value and flush it
        tree.put(b"key1".to_vec(), b"old_value".to_vec()).unwrap();
        tree.put(b"filler".to_vec(), vec![0u8; MEMTABLE_SIZE_THRESHOLD])
            .unwrap();
        assert_eq!(tree.l0_sstables.len(), 1);

        // put new value in memtable
        tree.put(b"key1".to_vec(), b"new_value".to_vec()).unwrap();

        // should return new value from memtable
        let val = tree.get(b"key1").unwrap();
        assert_eq!(val, Some(b"new_value".to_vec()));
    }

    #[test]
    fn test_tombstone_in_memtable_masks_sstable() {
        let path = temp_dir("tombstone_mask");
        let mut tree = LSMTree::open(path).unwrap();

        // put value and flush it
        tree.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        tree.put(b"filler".to_vec(), vec![0u8; MEMTABLE_SIZE_THRESHOLD])
            .unwrap();
        assert_eq!(tree.l0_sstables.len(), 1);

        // delete it (places tombstone in memtable)
        tree.delete(b"key1".to_vec()).unwrap();

        // get should return None because of the tombstone
        let val = tree.get(b"key1").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_restart_recovers_sstables() {
        let path = temp_dir("restart_recovery");

        // create LSMT, write some data, flush
        {
            let mut tree = LSMTree::open(&path).unwrap();
            tree.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
            tree.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
            tree.put(b"filler".to_vec(), vec![0u8; MEMTABLE_SIZE_THRESHOLD])
                .unwrap();
            assert_eq!(tree.l0_sstables.len(), 1);

            // tree is dropped here since it goes out of scope
        }

        // re-open LSMT, should recover SSTables
        {
            let mut tree = LSMTree::open(&path).unwrap();
            assert_eq!(tree.l0_sstables.len(), 1);
            assert!(tree.memtable.is_empty());

            // data should be accessible
            let val1 = tree.get(b"key1").unwrap();
            assert_eq!(val1, Some(b"value1".to_vec()));
            let val2 = tree.get(b"key2").unwrap();
            assert_eq!(val2, Some(b"value2".to_vec()));
        }
    }
}
