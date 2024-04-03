mod archive;
mod backup;
mod cleanup;
mod records;
mod restore;

use std::sync::Arc;

use crate::{
    error::Result,
    hash::{Hash, ShortHash},
    keys::{self, hash_from_key},
    storage::Storage,
};

pub use self::{
    archive::{delete_archives, download_archive, download_archives, upload_archive},
    backup::{backup_recursive, upload_pending_files, BackupState},
    cleanup::{cleanup_archives, cleanup_blocks, CleanupState},
    records::{
        download_archive_records, download_block_records, upload_archive_records,
        upload_block_records,
    },
    restore::{download_pending_files, restore_recursive, RestoreState},
};

pub async fn delete_blocks<'a, I: IntoIterator<Item = &'a Hash>>(
    storage: Arc<Storage>,
    hashes: I,
) -> Result<()> {
    let keys = hashes.into_iter().map(keys::block);
    storage.delete_many(keys).await?;
    Ok(())
}

pub async fn expand_hash(
    storage: Arc<Storage>,
    namespace: &str,
    short_hash: &ShortHash,
) -> Result<Hash> {
    let partial_key = keys::archive(short_hash);
    let full_key = storage.expand_key(&partial_key).await?;
    hash_from_key(namespace, &full_key)
}

pub async fn expand_hashes(
    storage: Arc<Storage>,
    namespace: &str,
    short_hashes: &[&ShortHash],
) -> Result<Vec<Hash>> {
    match short_hashes {
        [short_hash] => {
            let hash = expand_hash(storage, namespace, short_hash).await?;
            return Ok(vec![hash]);
        }
        [] => return Ok(vec![]),
        _ => {}
    };

    let partial_keys = short_hashes.iter().map(keys::archive);
    let full_keys = storage.expand_keys(partial_keys).await?;
    full_keys
        .into_iter()
        .map(|key| hash_from_key(namespace, key.as_str()))
        .collect()
}
