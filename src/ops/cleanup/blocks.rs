use std::{pin::pin, sync::Arc};

use async_channel::{Receiver, Sender};
use clap::builder::styling::AnsiColor;
use log::debug;
use tokio::{sync::Semaphore, task::JoinSet, try_join};
use tokio_stream::StreamExt;

use crate::{
    error::Result,
    keys::{self, hash_from_key},
    storage::MAX_KEYS_PER_REQUEST,
};

use super::{CleanupState, RemovedBlock};

pub async fn cleanup_blocks(state: Arc<CleanupState>) -> Result<()> {
    let (sender, receiver) = async_channel::bounded(MAX_KEYS_PER_REQUEST);
    try_join!(
        find_garbage_blocks(state.clone(), sender),
        delete_garbage_blocks(state.clone(), receiver),
    )?;
    Ok(())
}

async fn find_garbage_blocks(state: Arc<CleanupState>, sender: Sender<RemovedBlock>) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(state.task_count));
    let mut tasks = JoinSet::new();

    let mut key_chunks = pin!(state.storage.keys_paginated(Some(keys::BLOCK_NAMESPACE)));
    while let Some(keys) = key_chunks.try_next().await? {
        let state = state.clone();
        let sender = sender.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        tasks.spawn_blocking(move || {
            let block_records = state.block_records.blocking_read();

            for key in keys {
                let hash = hash_from_key(keys::BLOCK_NAMESPACE, &key)?;
                if !block_records.contains(&hash) {
                    let removed_block = RemovedBlock { hash, record: None };
                    sender.send_blocking(removed_block)?;
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

pub async fn delete_garbage_blocks(
    state: Arc<CleanupState>,
    receiver: Receiver<RemovedBlock>,
) -> Result<()> {
    let chunk_size = MAX_KEYS_PER_REQUEST;
    let mut keys = vec![];
    let mut bytes = 0;

    while let Ok(removed_block) = receiver.recv().await {
        let key = keys::block(&removed_block.hash);
        keys.push(key);

        if let Some(record) = removed_block.record {
            bytes += record.size;
        }

        let count = keys.len();
        if count >= chunk_size {
            let deleted_keys = keys.drain(..chunk_size).collect::<Vec<_>>();

            if !state.dry_run {
                state.storage.delete_many(&deleted_keys).await?;
            }

            state.stats.write().await.blocks_deleted += count as u64;
            state.stats.write().await.bytes_deleted += bytes;
            bytes = 0;

            for key in deleted_keys {
                let style = AnsiColor::Yellow.on_default();
                debug!("{style}deleted{style:#} {key}");
            }
        }
    }

    let count = keys.len();
    if count > 0 {
        if !state.dry_run {
            state.storage.delete_many(&keys).await?;
        }

        state.stats.write().await.blocks_deleted += count as u64;
        state.stats.write().await.bytes_deleted += bytes;

        for key in keys {
            let style = AnsiColor::Yellow.on_default();
            debug!("{style}deleted{style:#} {key}");
        }
    }

    Ok(())
}
