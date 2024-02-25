use aws_sdk_s3::{error::SdkError, primitives::ByteStreamError};
use std::path::PathBuf;

use thiserror::Error;

use crate::block::BlockHash;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid path `{0}`")]
    InvalidPath(PathBuf),

    #[error("no path for inode {0}")]
    NoPathForInode(u64),

    #[error("path `{0}` already exists")]
    PathAlreadyExists(PathBuf),

    // TODO: lol
    #[error("weird file `{0}`")]
    WeirdFile(PathBuf),

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

    #[error("{0}")]
    Sdk(String),
}

impl<E, R> From<SdkError<E, R>> for Error {
    fn from(error: SdkError<E, R>) -> Self {
        Error::Sdk(error.to_string())
    }
}

impl Error {
    pub fn invalid_path<P: Into<PathBuf>>(path: P) -> Error {
        Error::InvalidPath(path.into())
    }

    pub fn path_already_exists<P: Into<PathBuf>>(path: P) -> Error {
        Error::PathAlreadyExists(path.into())
    }

    pub fn weird_file<P: Into<PathBuf>>(path: P) -> Error {
        Error::WeirdFile(path.into())
    }
}
