use std::{
    fmt::Debug,
    path::{PathBuf, StripPrefixError},
    string::FromUtf8Error,
};

use async_channel::SendError;
use aws_sdk_s3::{error::SdkError, primitives::ByteStreamError};
use thiserror::Error;
use tokio::task::JoinError;

use crate::hash::Hash;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("`{0}` is not a directory")]
    FileIsNotDirectory(PathBuf),

    #[error("directory `{0}` does not exist")]
    DirectoryDoesNotExist(PathBuf),

    #[error("inode {0} does not exist")]
    InodeDoesNotExist(u64),

    #[error("`{0}` is already in archive")]
    PathAlreadyArchived(PathBuf),

    #[error("`{0}` already exists")]
    FileAlreadyExists(PathBuf),

    #[error("path is empty")]
    EmptyPath,

    #[error("invalid timestamp `{0}`")]
    InvalidTimestamp(i64),

    #[error("block has hash `{actual}`, expected `{expected}`")]
    WrongBlockHash { actual: Hash, expected: Hash },

    #[error("block has level `{actual}`, expected `{expected}`")]
    WrongBlockLevel { actual: u8, expected: u8 },

    #[error("block has invalid size {0}")]
    InvalidBlockSize(usize),

    #[error("too many block levels")]
    TooManyBlockLevels,

    #[error("{source}")]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error("{source}")]
    Join {
        #[from]
        source: JoinError,
    },

    #[error("{source}")]
    Walk {
        #[from]
        source: walkdir::Error,
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
    Utf {
        #[from]
        source: FromUtf8Error,
    },

    #[error("{source}")]
    Prefix {
        #[from]
        source: StripPrefixError,
    },

    #[error("{0}")]
    Sdk(String),

    #[error("{0}")]
    Channel(String),

    #[error("{0}")]
    Deserializer(String),

    #[error("{0}")]
    Serializer(String),
}

impl<E, R> From<SdkError<E, R>> for Error {
    fn from(error: SdkError<E, R>) -> Self {
        Error::Sdk(error.to_string())
    }
}
impl<T> From<SendError<T>> for Error {
    fn from(error: SendError<T>) -> Self {
        Error::Channel(error.to_string())
    }
}

impl<E: Debug> From<ciborium::de::Error<E>> for Error {
    fn from(error: ciborium::de::Error<E>) -> Self {
        Error::Deserializer(error.to_string())
    }
}

impl<E: Debug> From<ciborium::ser::Error<E>> for Error {
    fn from(error: ciborium::ser::Error<E>) -> Self {
        Error::Serializer(error.to_string())
    }
}
