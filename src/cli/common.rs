use std::sync::Arc;

use tokio::{sync::RwLock, task::spawn_blocking};

use crate::{
    archive::Archive,
    error::Result,
    hash::Hash,
    refs::RefCounts,
    serde::{deserialize, serialize},
    stats::Stats,
    storage::{self, BoxedStorage},
};

pub async fn download_archive(
    archive_name: &str,
    storage: Arc<RwLock<BoxedStorage>>,
) -> Result<Archive> {
    let key = storage::archive_key(archive_name);
    let archive_bytes = storage.write().await.get(&key).await?;
    let archive = spawn_blocking(move || deserialize(&archive_bytes)).await??;
    Ok(archive)
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
    storage
        .write()
        .await
        .put(storage::ARCHIVE_KEY_LATEST, timestamp.into())
        .await?;
    Ok(())
}

pub async fn delete_archive(storage: Arc<RwLock<BoxedStorage>>, name: String) -> Result<()> {
    let key = storage::archive_key(&name);
    storage.write().await.delete(&key).await?;
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

pub async fn remove_blocks<'a, I: IntoIterator<Item = &'a Hash>>(
    storage: Arc<RwLock<BoxedStorage>>,
    hashes: I,
) -> Result<()> {
    let keys = hashes.into_iter().map(storage::block_key).collect();
    storage.write().await.delete_many(keys).await?;
    Ok(())
}
