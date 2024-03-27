mod assert;
mod from;

use std::{fmt::Display, path::PathBuf};

use thiserror::Error;

use crate::hash::Hash;

pub use assert::{assert_block_level_eq, assert_hash_eq, assert_size_multiple_of_hash};

pub type Result<T> = std::result::Result<T, Error>;

pub const OK: Result<()> = Ok(());

#[derive(Error, Debug, PartialEq)]
pub enum Error {
    #[error("no item found for key `{0}`")]
    ItemNotFound(String),

    #[error("storage URL `{0}` is invalid")]
    InvalidStorageUrl(String),

    #[error("key `{0}` is invalid")]
    InvalidKey(String),

    #[error("hash `{0}` is invalid")]
    InvalidHash(String),

    #[error("no item found for prefix `{0}`")]
    NoItemForPrefix(String),

    #[error("multiple items found for prefix `{0}`")]
    MultipleItemsForPrefix(String),

    #[error("`{0}` is not a directory")]
    FileIsNotDirectory(PathBuf),

    #[error("`{0}` does not exist")]
    FileDoesNotExist(PathBuf),

    #[error("inode {0} does not exist")]
    InodeDoesNotExist(u64),

    #[error("path is empty")]
    EmptyPath,

    #[error("`{0}` is already in archive")]
    PathAlreadyArchived(PathBuf),

    #[error("`{0}` already exists")]
    FileAlreadyExists(PathBuf),

    #[error("no block record found for {0}")]
    BlockRecordNotFound(Hash),

    #[error("block {hash} has ref count {actual}, expected at least {expected}")]
    WrongRefCount {
        hash: Hash,
        actual: u64,
        expected: u64,
    },

    #[error("block has hash {actual}, expected {expected}")]
    WrongBlockHash { actual: Hash, expected: Hash },

    #[error("block has level {actual}, expected {expected}")]
    WrongBlockLevel {
        hash: Hash,
        actual: u8,
        expected: u8,
    },

    #[error("block has invalid size {0}")]
    InvalidBlockSize(u64),

    #[error("branch block level is 0")]
    BranchLevelZero,

    #[error("too many block levels")]
    TooManyBlockLevels,

    #[error("block is empty")]
    EmptyBlock,

    #[error(transparent)]
    Other(AnyError),
}

#[derive(Error, Debug)]
pub struct AnyError(anyhow::Error);

impl Display for AnyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq for AnyError {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Error {
    pub fn other<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error::Other(AnyError(error.into()))
    }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Error::Other(AnyError(error))
    }
}
