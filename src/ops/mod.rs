mod archive;
mod download;
mod records;
mod upload;

use std::sync::Arc;

use tokio::sync::RwLock;

pub use {
    archive::{
        delete_archive, delete_archives, download_archive, download_archives, upload_archive,
    },
    download::{download_pending_files, restore_recursive, DownloadArgs, DownloadState},
    records::{
        download_archive_records, download_block_records, upload_archive_records,
        upload_block_records,
    },
    upload::{backup_recursive, upload_pending_files, UploadArgs, UploadState},
};

use crate::{
    error::Result,
    hash::{Hash, ShortHash},
    keys::{self, hash_from_key},
    storage::BoxedStorage,
};

pub async fn delete_blocks<'a, I: IntoIterator<Item = &'a Hash>>(
    storage: Arc<RwLock<BoxedStorage>>,
    hashes: I,
) -> Result<()> {
    let keys = hashes.into_iter().map(keys::block).collect();
    storage.read().await.delete_many(keys).await?;
    Ok(())
}

pub async fn find_archive_hash(
    storage: Arc<RwLock<BoxedStorage>>,
    short_hash: &ShortHash,
) -> Result<Hash> {
    let partial_key = keys::archive(short_hash);
    let full_key = storage.read().await.expand_key(&partial_key).await?;
    hash_from_key(keys::ARCHIVE_NAMESPACE, &full_key)
}

pub async fn find_archive_hashes(
    storage: Arc<RwLock<BoxedStorage>>,
    short_hashes: &[&ShortHash],
) -> Result<Vec<Hash>> {
    let partial_keys_owned = short_hashes.iter().map(keys::archive).collect::<Vec<_>>();
    let partial_keys = partial_keys_owned
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let full_keys = storage.read().await.expand_keys(&partial_keys).await?;
    full_keys
        .into_iter()
        .map(|key| hash_from_key(keys::ARCHIVE_NAMESPACE, key.as_str()))
        .collect()
}
