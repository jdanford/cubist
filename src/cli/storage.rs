use std::env::{self};

use crate::{
    error::{Error, Result},
    storage::Storage,
};

use super::GlobalArgs;

const ENV_VAR_STORAGE: &str = "CUBIST_BUCKET";

pub async fn create_storage(args: &GlobalArgs) -> Result<Storage> {
    let bucket = if let Some(bucket) = &args.bucket {
        bucket.clone()
    } else {
        env::var(ENV_VAR_STORAGE).map_err(|_| Error::MissingEnvVar(ENV_VAR_STORAGE.to_owned()))?
    };

    Ok(Storage::new(bucket).await)
}
