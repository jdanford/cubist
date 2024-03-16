use std::{
    env::{self, VarError},
    ffi::OsStr,
    path::PathBuf,
    sync::Arc,
};

use humantime::parse_duration;
use tokio::{
    spawn,
    sync::{RwLock, Semaphore},
    task::spawn_blocking,
};

use crate::{
    archive::Archive,
    error::{Error, Result},
    hash::Hash,
    refs::RefCounts,
    serde::{deserialize, serialize},
    stats::Stats,
    storage::{self, BoxedStorage, LocalStorage, S3Storage},
};

use super::StorageArgs;

const ENV_VAR_BUCKET: &str = "CUBIST_BUCKET";
const ENV_VAR_LOCAL: &str = "CUBIST_LOCAL";
const ENV_VAR_LATENCY: &str = "CUBIST_LATENCY";

pub async fn create_storage(args: StorageArgs) -> Result<BoxedStorage> {
    let StorageArgs {
        mut bucket,
        mut local,
        latency,
    } = args;

    if bucket.is_none() && local.is_none() {
        bucket = get_env_var(ENV_VAR_BUCKET)?;
        local = get_env_var(ENV_VAR_LOCAL)?.map(PathBuf::from);
    }

    let latency = get_env_var(ENV_VAR_LATENCY)?
        .as_deref()
        .map(parse_duration)
        .transpose()?
        .or(latency);

    match (bucket, local) {
        (Some(bucket), None) => {
            let s3_storage = S3Storage::new(bucket).await;
            Ok(Box::new(s3_storage))
        }
        (None, Some(path)) => {
            let local_storage = LocalStorage::new(path, latency);
            Ok(Box::new(local_storage))
        }
        (None, None) => Err(Error::Cli(format!(
            "Either `{ENV_VAR_BUCKET}` or `{ENV_VAR_LOCAL}` must be set"
        ))),
        _ => Err(Error::Cli(format!(
            "`{ENV_VAR_BUCKET}` and `{ENV_VAR_LOCAL}` can't both be set"
        ))),
    }
}

pub async fn download_archive(
    storage: Arc<RwLock<BoxedStorage>>,
    archive_name: &str,
) -> Result<Archive> {
    let key = storage::archive_key(archive_name);
    let archive_bytes = storage.write().await.get(&key).await?;
    let archive = spawn_blocking(move || deserialize(&archive_bytes)).await??;
    Ok(archive)
}

pub async fn download_archives<S: ToString, I: IntoIterator<Item = S>>(
    storage: Arc<RwLock<BoxedStorage>>,
    names: I,
    max_concurrency: u32,
) -> Result<Vec<Archive>> {
    let semaphore = Arc::new(Semaphore::new(max_concurrency as usize));
    let archives = Arc::new(RwLock::new(vec![]));

    for name in names {
        let storage = storage.clone();
        let archives = archives.clone();
        let name = name.to_string();
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        spawn(async move {
            let archive = download_archive(storage.clone(), &name).await.unwrap();
            archives.write().await.push(archive);
            drop(permit);
        });
    }

    let _ = semaphore.acquire_many(max_concurrency).await.unwrap();
    let archives = Arc::try_unwrap(archives).unwrap().into_inner();
    Ok(archives)
}

pub async fn upload_archive(
    storage: Arc<RwLock<BoxedStorage>>,
    archive: Arc<Archive>,
    stats: &Stats,
) -> Result<()> {
    let timestamp = stats.start_time.format("%Y%m%d%H%M%S").to_string();
    let key = storage::archive_key(&timestamp);
    let archive_bytes = spawn_blocking(move || serialize(archive.as_ref())).await??;
    storage.write().await.put(&key, archive_bytes).await?;
    Ok(())
}

pub async fn delete_archive(storage: Arc<RwLock<BoxedStorage>>, name: &str) -> Result<()> {
    let key = storage::archive_key(name);
    storage.write().await.delete(&key).await?;
    Ok(())
}

pub async fn delete_archives<S: ToString, I: IntoIterator<Item = S>>(
    storage: Arc<RwLock<BoxedStorage>>,
    names: I,
    max_concurrency: u32,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(max_concurrency as usize));

    for name in names {
        let storage = storage.clone();
        let name = name.to_string();
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        spawn(async move {
            delete_archive(storage.clone(), &name).await.unwrap();
            drop(permit);
        });
    }

    let _ = semaphore.acquire_many(max_concurrency).await.unwrap();
    Ok(())
}

pub async fn download_ref_counts(storage: Arc<RwLock<BoxedStorage>>) -> Result<RefCounts> {
    let maybe_bytes = storage
        .write()
        .await
        .try_get(storage::REF_COUNTS_KEY)
        .await?;
    let ref_counts = if let Some(bytes) = maybe_bytes {
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        RefCounts::new()
    };

    Ok(ref_counts)
}

pub async fn upload_ref_counts(
    storage: Arc<RwLock<BoxedStorage>>,
    ref_counts: RefCounts,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&ref_counts)).await??;
    storage
        .write()
        .await
        .put(storage::REF_COUNTS_KEY, bytes)
        .await?;
    Ok(())
}

pub async fn update_ref_counts(
    storage: Arc<RwLock<BoxedStorage>>,
    mut ref_counts: RefCounts,
    archive_ref_counts: &RefCounts,
) -> Result<()> {
    ref_counts.add(archive_ref_counts);
    upload_ref_counts(storage, ref_counts).await
}

pub async fn delete_blocks<'a, I: IntoIterator<Item = &'a Hash>>(
    storage: Arc<RwLock<BoxedStorage>>,
    hashes: I,
) -> Result<()> {
    let keys = hashes.into_iter().map(storage::block_key).collect();
    storage.write().await.delete_many(keys).await?;
    Ok(())
}

fn get_env_var<T: AsRef<OsStr>>(name: T) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}
