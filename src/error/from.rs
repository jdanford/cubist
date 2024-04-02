use std::{env::VarError, fmt::Debug, path::StripPrefixError};

use async_channel::SendError;
use aws_sdk_s3::{
    error::{BuildError, SdkError},
    primitives::ByteStreamError,
};
use humantime::DurationError;
use tokio::{sync::AcquireError, task::JoinError};

use super::Error;

impl From<VarError> for Error {
    fn from(error: VarError) -> Self {
        Error::other(error)
    }
}

impl From<bincode::Error> for Error {
    fn from(error: bincode::Error) -> Self {
        Error::other(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::other(error)
    }
}

impl From<JoinError> for Error {
    fn from(error: JoinError) -> Self {
        Error::other(error)
    }
}

impl From<AcquireError> for Error {
    fn from(error: AcquireError) -> Self {
        Error::other(error)
    }
}

impl From<fastcdc::v2020::Error> for Error {
    fn from(error: fastcdc::v2020::Error) -> Self {
        Error::other(error)
    }
}

impl From<ByteStreamError> for Error {
    fn from(error: ByteStreamError) -> Self {
        Error::other(error)
    }
}

impl From<StripPrefixError> for Error {
    fn from(error: StripPrefixError) -> Self {
        Error::other(error)
    }
}

impl From<DurationError> for Error {
    fn from(error: DurationError) -> Self {
        Error::other(error)
    }
}

impl From<clap::Error> for Error {
    fn from(error: clap::Error) -> Self {
        Error::other(error)
    }
}

impl<E: std::error::Error + Send + Sync + 'static, R: Debug + Send + Sync + 'static>
    From<SdkError<E, R>> for Error
{
    fn from(error: SdkError<E, R>) -> Self {
        Error::other(error)
    }
}

impl From<BuildError> for Error {
    fn from(error: BuildError) -> Self {
        Error::other(error)
    }
}

impl<T: Send + Sync + 'static> From<SendError<T>> for Error {
    fn from(error: SendError<T>) -> Self {
        Error::other(error)
    }
}
