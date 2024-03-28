mod assert;
mod from;

use std::path::PathBuf;

use thiserror::Error;

use crate::hash::Hash;

pub use assert::{assert_block_level_eq, assert_hash_eq, assert_size_multiple_of_hash};

pub type Result<T> = std::result::Result<T, Error>;

pub const OK: Result<()> = Ok(());

#[derive(Error, Debug)]
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

    #[error("archive `{hash}` does not have tag `{tag}`")]
    NoTagForArchive { hash: String, tag: String },

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
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(anyhow::Error),
}

impl Error {
    pub fn other<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error::Other(error.into())
    }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Error::Other(error)
    }
}

#[allow(clippy::match_same_arms)]
impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        #[allow(clippy::enum_glob_use)]
        use Error::*;

        match (self, other) {
            (ItemNotFound(key_l), ItemNotFound(key_r)) => key_l == key_r,
            (InvalidStorageUrl(url_l), InvalidStorageUrl(url_r)) => url_l == url_r,
            (InvalidKey(key_l), InvalidKey(key_r)) => key_l == key_r,
            (InvalidHash(str_l), InvalidHash(str_r)) => str_l == str_r,
            (NoItemForPrefix(prefix_l), NoItemForPrefix(prefix_r)) => prefix_l == prefix_r,
            (MultipleItemsForPrefix(prefix_l), MultipleItemsForPrefix(prefix_r)) => {
                prefix_l == prefix_r
            }
            (FileIsNotDirectory(path_l), FileIsNotDirectory(path_r)) => path_l == path_r,
            (FileDoesNotExist(path_l), FileDoesNotExist(path_r)) => path_l == path_r,
            (InodeDoesNotExist(inode_l), InodeDoesNotExist(inode_r)) => inode_l == inode_r,
            (EmptyPath, EmptyPath) => false,
            (PathAlreadyArchived(path_l), PathAlreadyArchived(path_r)) => path_l == path_r,
            (FileAlreadyExists(path_l), FileAlreadyExists(path_r)) => path_l == path_r,
            (
                NoTagForArchive {
                    hash: hash_l,
                    tag: tag_l,
                },
                NoTagForArchive {
                    hash: hash_r,
                    tag: tag_r,
                },
            ) => hash_l == hash_r && tag_l == tag_r,
            (BlockRecordNotFound(hash_l), BlockRecordNotFound(hash_r)) => hash_l == hash_r,
            (
                WrongRefCount {
                    hash: hash_l,
                    actual: actual_l,
                    expected: expected_l,
                },
                WrongRefCount {
                    hash: hash_r,
                    actual: actual_r,
                    expected: expected_r,
                },
            ) => hash_l == hash_r && actual_l == actual_r && expected_l == expected_r,
            (
                WrongBlockHash {
                    actual: actual_l,
                    expected: expected_l,
                },
                WrongBlockHash {
                    actual: actual_r,
                    expected: expected_r,
                },
            ) => actual_l == actual_r && expected_l == expected_r,
            (
                WrongBlockLevel {
                    hash: hash_l,
                    actual: actual_l,
                    expected: expected_l,
                },
                WrongBlockLevel {
                    hash: hash_r,
                    actual: actual_r,
                    expected: expected_r,
                },
            ) => hash_l == hash_r && actual_l == actual_r && expected_l == expected_r,
            (InvalidBlockSize(size_l), InvalidBlockSize(size_r)) => size_l == size_r,
            (BranchLevelZero, BranchLevelZero) => true,
            (TooManyBlockLevels, TooManyBlockLevels) => true,
            (EmptyBlock, EmptyBlock) => true,
            _ => false,
        }
    }
}
