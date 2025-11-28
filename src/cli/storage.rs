use crate::{env, error::Result, storage::Storage};

use super::GlobalArgs;

const ENV_VAR_STORAGE: &str = "CUBIST_BUCKET";

pub async fn create_storage(args: &GlobalArgs) -> Result<Storage> {
    let bucket = if let Some(bucket) = &args.bucket {
        bucket.clone()
    } else {
        env::var(ENV_VAR_STORAGE)?
    };

    Ok(Storage::new(bucket).await)
}
