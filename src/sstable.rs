//! SSTable implementation
//!
//! # File Format Specification
//!
//! An SSTable file consists of three main sections:
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │          Data Block                     │
//! │  (variable length, sorted entries)      │
//! ├─────────────────────────────────────────┤
//! │          Index Block                    │
//! │  (sparse index: key → offset)           │
//! ├─────────────────────────────────────────┤
//! │          Footer                         │
//! │  (metadata, 32 bytes fixed)             │
//! └─────────────────────────────────────────┘
//! ```
//!
//! ## Data Block Format
//!
//! The data block contains sorted key-value pairs:
//!
//! ```text
//! For each entry:
//!   key_len:    u32 (4 bytes)
//!   key:        [u8; key_len]
//!   value_len:  u32 (4 bytes)
//!   value:      [u8; value_len]
//!   tombstone:  u8 (1 byte)    // 0 = value, 1 = tombstone
//! ```
//!
//! ## Index Block Format
//!
//! The index block contains a sparse index mapping keys to offsets:
//!
//! ```text
//! For each index entry:
//!   key_len:    u32 (4 bytes)
//!   key:        [u8; key_len]
//!   offset:     u64 (8 bytes)  // offset into data block
//! ```
//!
//! ## Footer Format (32 bytes fixed)
//!
//! ```text
//! index_offset:   u64 (8 bytes)  // offset to index block
//! index_len:      u32 (4 bytes)  // length of index block
//! num_entries:    u32 (4 bytes)  // total number of entries
//! magic_number:   u64 (8 bytes)  // 0x5353544142454c31 ("SSTABLE1")
//! _reserved:      u64 (8 bytes)  // reserved for future use
//! ```

use crate::{Error, Result, Value};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Magic number for SSTable files: "SSTABLE1" in ASCII
const MAGIC_NUMBER: u64 = 0x5353544142454c31;

/// Size of the footer in bytes
const FOOTER_SIZE: u64 = 32;

/// SSTable builder class
pub struct SSTableBuilder {
    /// Buffered writer
    writer: BufWriter<File>,
    /// In-memory index: key → offset in data block
    index: Vec<(Vec<u8>, u64)>,
    /// Current offset in the data block
    current_offset: u64,
    /// Number of entries written
    num_entries: u32,
}

impl SSTableBuilder {
    /// Instantiates new  SSTable builder
    pub fn new(path: PathBuf) -> Result<Self> {
        let file = File::create(&path).expect("Error creating file");
        let writer = BufWriter::new(file);

        Ok(SSTableBuilder {
            writer,
            index: Vec::new(),
            current_offset: 0,
            num_entries: 0,
        })
    }

    /// Add a key-value pair to the SSTable
    //  !! must be added in sorted order
    pub fn add(&mut self, key: &[u8], value: &Value) -> Result<()> {
        let offset = self.current_offset;

        // write key
        let key_len = key.len() as u32;
        self.writer.write_all(&key_len.to_le_bytes())?;
        self.current_offset += 4;

        self.writer.write_all(key)?;
        self.current_offset += key.len() as u64;

        // write value
        match value {
            // write actual value
            Value::Some(val) => {
                let value_len = val.len() as u32;
                self.writer.write_all(&value_len.to_le_bytes())?;
                self.current_offset += 4;

                self.writer.write_all(val)?;
                self.current_offset += val.len() as u64;

                // tombstone flag (0 = not a tombstone)
                self.writer.write_all(&[0u8])?;
                self.current_offset += 1;
            }
            // write tombstone
            Value::Tombstone => {
                // valu e length is 0 for tombstones
                self.writer.write_all(&0u32.to_le_bytes())?;
                self.current_offset += 4;

                // tombstone flag (1 = tombstone)
                self.writer.write_all(&[1u8])?;
                self.current_offset += 1;
            }
        }

        // add entry to index
        self.index.push((key.to_vec(), offset));
        self.num_entries += 1;

        Ok(())
    }

    /// Finish writing the SSTable and flush to disk
    pub fn finish(mut self) -> Result<()> {
        let index_offset = self.current_offset;

        // writee index block
        let mut index_len = 0u64;
        for (key, offset) in &self.index {
            let key_len = key.len() as u32;
            self.writer.write_all(&key_len.to_le_bytes())?;
            index_len += 4;

            self.writer.write_all(key)?;
            index_len += key.len() as u64;

            self.writer.write_all(&offset.to_le_bytes())?;
            index_len += 8;
        }

        // write the footer
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(&(index_len as u32).to_le_bytes())?;
        self.writer.write_all(&self.num_entries.to_le_bytes())?;
        self.writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;
        self.writer.write_all(&0u64.to_le_bytes())?; // reserved

        // flush to disk
        self.writer.flush()?;

        Ok(())
    }
}

/// SSTable reader
pub struct SSTable {
    /// File path
    path: PathBuf,
    /// File handle
    file: File,
    /// In-memory index: key → offset in data block
    index: BTreeMap<Vec<u8>, u64>,
    /// Number of entries in the SSTable
    num_entries: u32,
}

impl SSTable {
    /// Open an existing SSTable
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut file = File::open(&path)?;

        // read footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        let mut footer_buf = [0u8; FOOTER_SIZE as usize];
        file.read_exact(&mut footer_buf)?;

        // parse footer
        let index_offset = u64::from_le_bytes(footer_buf[0..8].try_into().unwrap());
        let index_len = u32::from_le_bytes(footer_buf[8..12].try_into().unwrap());
        let num_entries = u32::from_le_bytes(footer_buf[12..16].try_into().unwrap());
        let magic = u64::from_le_bytes(footer_buf[16..24].try_into().unwrap());

        // validate magic number
        if magic != MAGIC_NUMBER {
            return Err(Error::Corruption(format!(
                "Invalid magic number: expected 0x{:x}, got 0x{:x}",
                MAGIC_NUMBER, magic
            )));
        }

        // read index block
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_buf = vec![0u8; index_len as usize];
        file.read_exact(&mut index_buf)?;

        // pars eindex
        let mut index = BTreeMap::new();
        let mut pos = 0;

        while pos < index_len as usize {
            let key_len = u32::from_le_bytes(index_buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            let key = index_buf[pos..pos + key_len].to_vec();
            pos += key_len;

            let offset = u64::from_le_bytes(index_buf[pos..pos + 8].try_into().unwrap());
            pos += 8;

            index.insert(key, offset);
        }

        Ok(SSTable {
            path,
            file,
            index,
            num_entries,
        })
    }

    /// Get a value by key
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Value>> {
        // binary search the index
        let offset = match self.index.get(key) {
            Some(offset) => *offset,
            None => return Ok(None),
        };

        // seek to the data block entry
        self.file.seek(SeekFrom::Start(offset))?;

        // read key_len
        let mut key_len_buf = [0u8; 4];
        self.file.read_exact(&mut key_len_buf)?;
        let key_len = u32::from_le_bytes(key_len_buf) as usize;

        // read key
        let mut key_buf = vec![0u8; key_len];
        self.file.read_exact(&mut key_buf)?;

        if key_buf != key {
            return Err(Error::Corruption(
                "Key mismatch at indexed offset".to_string(),
            ));
        }

        // read value_len
        let mut value_len_buf = [0u8; 4];
        self.file.read_exact(&mut value_len_buf)?;
        let value_len = u32::from_le_bytes(value_len_buf) as usize;

        // read value if not a tombstone
        let value = if value_len > 0 {
            let mut value_buf = vec![0u8; value_len];
            self.file.read_exact(&mut value_buf)?;
            value_buf
        } else {
            Vec::new() // TODO: save memory by not instantiating vector if tombstone
        };

        // read tombstone flag
        let mut tombstone_buf = [0u8; 1];
        self.file.read_exact(&mut tombstone_buf)?;
        let is_tombstone = tombstone_buf[0] == 1;

        if is_tombstone {
            Ok(Some(Value::Tombstone))
        } else {
            Ok(Some(Value::Some(value)))
        }
    }

    /// Get the number of entries in the SSTable
    pub fn num_entries(&self) -> u32 {
        self.num_entries
    }

    /// Get the file path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}
