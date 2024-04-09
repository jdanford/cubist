mod archive;
mod backup;
mod cleanup;
mod records;
mod restore;

use std::{borrow::Borrow, sync::Arc};

use itertools::Itertools;

use crate::{
    block::Block,
    entity::Entity,
    error::{handle_error, Result},
    hash::{Hash, ShortHash},
    storage::{Storage, MAX_KEYS_PER_REQUEST},
    task::BoundedJoinSet,
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

pub async fn try_delete_blocks<H, I>(
    storage: Arc<Storage>,
    hashes: I,
    task_count: usize,
) -> Result<()>
where
    H: Borrow<Hash<Block>>,
    I: IntoIterator<Item = Result<H>>,
{
    let mut tasks = BoundedJoinSet::new(task_count);
    let keys = hashes
        .into_iter()
        .map(|result| result.map(|hash| hash.borrow().key()));

    for chunk in &keys.chunks(MAX_KEYS_PER_REQUEST) {
        let keys = chunk.collect::<Result<Vec<_>>>()?;
        let storage = storage.clone();
        tasks
            .spawn(async move { storage.delete_chunk(keys).await })
            .await?;

        while let Some(result) = tasks.try_join_next() {
            handle_error(result?);
        }
    }

    while let Some(result) = tasks.join_next().await {
        handle_error(result?);
    }

    Ok(())
}

pub async fn expand_hash<E: Entity>(
    storage: Arc<Storage>,
    short_hash: &ShortHash<E>,
) -> Result<Hash<E>> {
    let partial_key = short_hash.key_prefix();
    let full_key = storage.expand_key(&partial_key).await?;
    Hash::from_key(&full_key)
}

pub async fn expand_hashes<E, H>(storage: Arc<Storage>, short_hashes: &[H]) -> Result<Vec<Hash<E>>>
where
    E: Entity,
    H: Borrow<ShortHash<E>>,
{
    let partial_keys = short_hashes.iter().map(|hash| hash.borrow().key_prefix());
    let full_keys = storage.expand_keys(partial_keys).await?;
    full_keys
        .into_iter()
        .map(|key| Hash::from_key(key.as_str()))
        .collect()
}
