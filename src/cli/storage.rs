use std::{
    env::{self, VarError},
    ffi::OsStr,
    str::FromStr,
    time::Duration,
};

use humantime::parse_duration;

use crate::{
    error::{Error, Result},
    storage::{BoxedStorage, LocalStorage, S3Storage, StorageUrl},
};

use super::GlobalArgs;

const ENV_VAR_STORAGE: &str = "CUBIST_STORAGE";
const ENV_VAR_LATENCY: &str = "CUBIST_LATENCY";

pub async fn create_storage(args: &GlobalArgs) -> Result<BoxedStorage> {
    let storage_url = if let Some(storage_url) = &args.storage {
        storage_url.to_owned()
    } else {
        get_env_storage_url()?
            .ok_or_else(|| Error::Cli(format!("`{ENV_VAR_STORAGE}` must be set")))?
    };

    let latency = get_env_latency()?.or(args.latency);

    match storage_url {
        StorageUrl::S3(bucket) => {
            let s3_storage = S3Storage::new(bucket).await;
            Ok(Box::new(s3_storage))
        }
        StorageUrl::Local(path) => {
            let local_storage = LocalStorage::new(path, latency);
            Ok(Box::new(local_storage))
        }
    }
}

fn get_env_storage_url() -> Result<Option<StorageUrl>> {
    get_env_var(ENV_VAR_STORAGE)?
        .as_deref()
        .map(FromStr::from_str)
        .transpose()
}

fn get_env_latency() -> Result<Option<Duration>> {
    get_env_var(ENV_VAR_LATENCY)?
        .as_deref()
        .map(parse_duration)
        .transpose()
        .map_err(Error::from)
}

fn get_env_var<T: AsRef<OsStr>>(name: T) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}
