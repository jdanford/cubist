mod archive;
mod backup;
mod cleanup;
mod records;
mod restore;

use std::{borrow::Borrow, fmt::Display, sync::Arc};

use crate::{
    block::Block,
    entity::Entity,
    error::Result,
    hash::{Hash, ShortHash},
    storage::Storage,
};

pub use self::{
    archive::{download_archive, upload_archive},
    backup::{backup_all, upload_pending_files, BackupState},
    cleanup::{cleanup_archives, cleanup_blocks, delete_archives_and_garbage_blocks, CleanupState},
    records::{
        download_archive_records, download_block_records, upload_archive_records,
        upload_block_records,
    },
    restore::{download_pending_files, restore_all, RestoreState},
};

pub async fn delete_blocks<H: Borrow<Hash<Block>>, I: IntoIterator<Item = H>>(
    storage: Arc<Storage>,
    hashes: I,
) -> Result<()> {
    let keys = hashes.into_iter().map(|hash| hash.borrow().key());
    storage.delete_many(keys).await
}

pub async fn expand_hash<E: Entity>(
    storage: Arc<Storage>,
    short_hash: &ShortHash<E>,
) -> Result<Hash<E>> {
    let partial_key = short_hash.to_key_prefix();
    let full_key = storage.expand_key(&partial_key).await?;
    Hash::from_key(&full_key)
}

pub async fn expand_hashes<E: Entity, H: Borrow<ShortHash<E>> + Display>(
    storage: Arc<Storage>,
    short_hashes: &[H],
) -> Result<Vec<Hash<E>>> {
    match short_hashes {
        [short_hash] => {
            let hash = expand_hash(storage, short_hash.borrow()).await?;
            return Ok(vec![hash]);
        }
        [] => return Ok(vec![]),
        _ => {}
    };

    let partial_keys = short_hashes
        .iter()
        .map(|hash| hash.borrow().to_key_prefix());
    let full_keys = storage.expand_keys(partial_keys).await?;
    full_keys
        .into_iter()
        .map(|key| Hash::from_key(key.as_str()))
        .collect()
}
