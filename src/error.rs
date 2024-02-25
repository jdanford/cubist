use std::{fmt::Debug, path::PathBuf, string::FromUtf8Error};

use aws_sdk_s3::{error::SdkError, primitives::ByteStreamError};
use thiserror::Error;

use crate::block::BlockHash;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid path `{0}`")]
    InvalidPath(PathBuf),

    #[error("no path for inode {0}")]
    NoPathForInode(u64),

    #[error("path `{0}` is already in archive")]
    PathAlreadyArchived(PathBuf),

    #[error("file `{0}` already exists")]
    FileAlreadyExists(PathBuf),

    // TODO: lol
    #[error("file `{0}` is weird")]
    WeirdFile(PathBuf),

    #[error("invalid timestamp `{0}`")]
    InvalidTimestamp(i64),

    #[error("block has hash `{actual}`, expected `{expected}`")]
    WrongBlockHash {
        actual: BlockHash,
        expected: BlockHash,
    },

    #[error("{source}")]
    Io {
        #[from]
        source: std::io::Error,
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

    #[error("{0}")]
    Sdk(String),

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

impl Error {
    pub fn invalid_path<P: Into<PathBuf>>(path: P) -> Error {
        Error::InvalidPath(path.into())
    }

    pub fn path_already_archived<P: Into<PathBuf>>(path: P) -> Error {
        Error::PathAlreadyArchived(path.into())
    }

    pub fn file_already_exists<P: Into<PathBuf>>(path: P) -> Error {
        Error::FileAlreadyExists(path.into())
    }

    pub fn weird_file<P: Into<PathBuf>>(path: P) -> Error {
        Error::WeirdFile(path.into())
    }
}
