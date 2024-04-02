use std::env::{self};

use anyhow::anyhow;

use crate::{error::Result, storage::Storage};

use super::GlobalArgs;

const ENV_VAR_STORAGE: &str = "CUBIST_BUCKET";

pub async fn create_storage(args: &GlobalArgs) -> Result<Storage> {
    let bucket = if let Some(bucket) = &args.bucket {
        bucket.to_owned()
    } else {
        env::var(ENV_VAR_STORAGE).map_err(|_| anyhow!("`{ENV_VAR_STORAGE}` must be set"))?
    };

    Ok(Storage::new(bucket).await)
}
