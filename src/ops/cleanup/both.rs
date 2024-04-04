use std::sync::Arc;

use async_channel::Sender;
use tokio::{
    sync::Semaphore,
    task::{spawn_blocking, JoinSet},
    try_join,
};

use crate::{
    error::Result,
    hash::Hash,
    ops::{
        cleanup::{archives::delete_garbage_archives, blocks::delete_garbage_blocks},
        download_archive,
    },
    storage::MAX_KEYS_PER_REQUEST,
};

use super::{CleanupState, RemovedArchive, RemovedBlock};

pub async fn delete_archives_and_garbage_blocks<'a, I: IntoIterator<Item = &'a Hash>>(
    state: Arc<CleanupState>,
    hashes: I,
) -> Result<()> {
    let (archive_sender, archive_receiver) = async_channel::bounded(MAX_KEYS_PER_REQUEST);
    let (block_sender, block_receiver) = async_channel::bounded(MAX_KEYS_PER_REQUEST);
    try_join!(
        find_archives_and_garbage_blocks(state.clone(), hashes, archive_sender, block_sender),
        delete_garbage_archives(state.clone(), archive_receiver),
        delete_garbage_blocks(state.clone(), block_receiver),
    )?;
    Ok(())
}

async fn find_archives_and_garbage_blocks<'a, I: IntoIterator<Item = &'a Hash>>(
    state: Arc<CleanupState>,
    hashes: I,
    archive_sender: Sender<RemovedArchive>,
    block_sender: Sender<RemovedBlock>,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(state.task_count));
    let mut tasks = JoinSet::new();

    for hash in hashes {
        let state = state.clone();
        let storage = state.storage.clone();
        let hash = hash.to_owned();
        let archive_sender = archive_sender.clone();
        let block_sender = block_sender.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn(async move {
            let archive = download_archive(storage.clone(), &hash).await?;
            let record = state.archive_records.write().await.remove(&hash)?;
            let removed_archive = RemovedArchive {
                hash,
                record: Some(record),
            };
            archive_sender.send(removed_archive).await?;

            spawn_blocking(move || {
                let mut block_records = state.block_records.blocking_write();
                let garbage_blocks = block_records.remove_refs(&archive.block_refs)?;
                for (hash, record) in garbage_blocks {
                    let removed_block = RemovedBlock {
                        hash,
                        record: Some(record),
                    };
                    block_sender.send_blocking(removed_block)?;
                }

                Result::Ok(())
            })
            .await??;

            drop(permit);
            Result::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}
