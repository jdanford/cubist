use std::{pin::pin, sync::Arc};

use async_channel::Sender;
use tokio::{sync::Semaphore, task::JoinSet, try_join};
use tokio_stream::StreamExt;

use crate::{
    archive::Archive,
    entity::{Entity, EntityIndex},
    error::Result,
    hash::Hash,
    ops::cleanup::delete_entities,
    storage::MAX_KEYS_PER_REQUEST,
};

use super::{CleanupState, RemovedArchive};

pub async fn cleanup_archives(state: Arc<CleanupState>) -> Result<()> {
    let (sender, receiver) = async_channel::bounded(MAX_KEYS_PER_REQUEST);
    try_join!(
        find_garbage_archives(state.clone(), sender),
        delete_entities(state.clone(), receiver),
    )?;
    Ok(())
}

async fn find_garbage_archives(
    state: Arc<CleanupState>,
    sender: Sender<RemovedArchive>,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(state.task_count));
    let mut tasks = JoinSet::new();

    let mut key_chunks = pin!(state.storage.keys_paginated(Some(Archive::KEY_PREFIX)));
    while let Some(keys) = key_chunks.try_next().await? {
        let state = state.clone();
        let sender = sender.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn_blocking(move || {
            let archive_records = state.archive_records.blocking_read();

            for key in keys {
                let hash = Hash::from_key(&key)?;
                if !archive_records.contains(&hash) {
                    let removed_archive = RemovedArchive { hash, record: None };
                    sender.send_blocking(removed_archive)?;
                }
            }

            drop(permit);
            Result::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}
