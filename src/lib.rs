mod memtable;

pub use memtable::{Memtable, Value};

use std::io;

/// Error types for LSM-tree operations
#[derive(Debug)]
pub enum Error {
    // file operations, etc.
    Io(io::Error),
    // data corruption
    Corruption(String),
    // invalid operation or argument
    InvalidArgument(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(err) => write!(f, "I/O error: {}", err),
            Error::Corruption(msg) => write!(f, "Corruption: {}", msg),
            Error::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
