mod from;

use std::{path::PathBuf, process::ExitCode};

use log::error;
use thiserror::Error;

use crate::{archive::Archive, block::Block, hash::Hash};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("no item found for key `{0}`")]
    ItemNotFound(String),

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

    #[error("no archive record found for {0}")]
    ArchiveRecordNotFound(Hash<Archive>),

    #[error("no block record found for {0}")]
    BlockRecordNotFound(Hash<Block>),

    #[error("block {hash} has ref count {actual}, expected at least {expected}")]
    WrongRefCount {
        hash: Hash<Block>,
        actual: u64,
        expected: u64,
    },

    #[error("block has hash {actual}, expected {expected}")]
    WrongBlockHash {
        actual: Hash<Block>,
        expected: Hash<Block>,
    },

    #[error("block has level {actual}, expected {expected}")]
    WrongBlockLevel {
        hash: Hash<Block>,
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

    #[error("`{0}` must be set")]
    MissingEnvVar(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    WalkDir(#[from] async_walkdir::Error),

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
            (InvalidKey(key_l), InvalidKey(key_r)) => key_l == key_r,
            (InvalidHash(hash_l), InvalidHash(hash_r)) => hash_l == hash_r,
            (NoItemForPrefix(prefix_l), NoItemForPrefix(prefix_r)) => prefix_l == prefix_r,
            (MultipleItemsForPrefix(prefix_l), MultipleItemsForPrefix(prefix_r)) => {
                prefix_l == prefix_r
            }
            (FileIsNotDirectory(path_l), FileIsNotDirectory(path_r)) => path_l == path_r,
            (FileDoesNotExist(path_l), FileDoesNotExist(path_r)) => path_l == path_r,
            (InodeDoesNotExist(inode_l), InodeDoesNotExist(inode_r)) => inode_l == inode_r,
            (EmptyPath, EmptyPath) => true,
            (PathAlreadyArchived(path_l), PathAlreadyArchived(path_r)) => path_l == path_r,
            (FileAlreadyExists(path_l), FileAlreadyExists(path_r)) => path_l == path_r,
            (ArchiveRecordNotFound(hash_l), ArchiveRecordNotFound(hash_r)) => hash_l == hash_r,
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
            (MissingEnvVar(var_l), MissingEnvVar(var_r)) => var_l == var_r,
            _ => false,
        }
    }
}

pub fn handle_error<T>(result: Result<T>) -> ExitCode {
    if let Err(err) = result {
        error!("{err}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
