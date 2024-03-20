use std::{
    env::VarError,
    fmt::Debug,
    path::{PathBuf, StripPrefixError},
};

use async_channel::SendError;
use aws_sdk_s3::{
    error::{BuildError, SdkError},
    primitives::ByteStreamError,
};
use humantime::DurationError;
use thiserror::Error;
use tokio::{sync::AcquireError, task::JoinError};

use crate::hash::{self, Hash};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("no item found for key `{0}`")]
    ItemNotFound(String),

    #[error("storage URL `{0}` is invalid")]
    InvalidStorageUrl(String),

    #[error("key `{0}` is invalid")]
    InvalidKey(String),

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

    #[error("too many block levels")]
    TooManyBlockLevels,

    #[error("{source}")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("{source}")]
    EnvVar {
        #[from]
        source: VarError,
    },

    #[error("{source}")]
    Bincode {
        #[from]
        source: bincode::Error,
    },

    #[error("{source}")]
    Join {
        #[from]
        source: JoinError,
    },

    #[error("{source}")]
    Acquire {
        #[from]
        source: AcquireError,
    },

    #[error("{source}")]
    Chunker {
        #[from]
        source: fastcdc::v2020::Error,
    },

    #[error("{source}")]
    ByteStream {
        #[from]
        source: ByteStreamError,
    },

    #[error("{source}")]
    Prefix {
        #[from]
        source: StripPrefixError,
    },

    #[error("{source}")]
    Duration {
        #[from]
        source: DurationError,
    },

    #[error("{0}")]
    Cli(String),

    #[error("{0}")]
    Sdk(String),

    #[error("{0}")]
    Channel(String),
}

impl From<clap::Error> for Error {
    fn from(error: clap::Error) -> Self {
        Error::Cli(error.to_string())
    }
}

impl<E, R> From<SdkError<E, R>> for Error {
    fn from(error: SdkError<E, R>) -> Self {
        Error::Sdk(error.to_string())
    }
}

impl From<BuildError> for Error {
    fn from(error: BuildError) -> Self {
        Error::Sdk(error.to_string())
    }
}

impl<T> From<SendError<T>> for Error {
    fn from(error: SendError<T>) -> Self {
        Error::Channel(error.to_string())
    }
}

pub fn assert_block_level_eq(hash: Hash, actual: u8, expected: Option<u8>) -> Result<()> {
    if let Some(expected) = expected {
        if expected != actual {
            return Err(Error::WrongBlockLevel {
                hash,
                actual,
                expected,
            });
        }
    }

    Ok(())
}

pub fn assert_hash_eq(actual: &Hash, expected: &Hash) -> Result<()> {
    if expected != actual {
        return Err(Error::WrongBlockHash {
            actual: *actual,
            expected: *expected,
        });
    }

    Ok(())
}

pub fn assert_size_multiple_of_hash(size: u64) -> Result<()> {
    if size % hash::SIZE as u64 != 0 {
        return Err(Error::InvalidBlockSize(size));
    }

    Ok(())
}
