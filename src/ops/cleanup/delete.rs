use std::{mem, sync::Arc};

use async_channel::Receiver;
use clap::builder::styling::AnsiColor;
use log::debug;

use crate::{
    entity::{Entity, EntityIndex, EntityRecord},
    error::Result,
    hash::Hash,
    stats::{CommandStats, EntityStats},
    storage::MAX_KEYS_PER_REQUEST,
};

use super::{CleanupState, RemovedEntity};

pub async fn delete_entities<E, I>(
    state: Arc<CleanupState>,
    receiver: Receiver<RemovedEntity<E, I>>,
) -> Result<()>
where
    E: Entity,
    I: EntityIndex<E>,
    CommandStats: EntityStats<E>,
{
    let chunk_size = MAX_KEYS_PER_REQUEST;
    let mut hashes = vec![];
    let mut bytes = 0;

    while let Ok(removed_block) = receiver.recv().await {
        hashes.push(removed_block.hash);

        if let Some(record) = removed_block.record {
            bytes += record.size();
        }

        if hashes.len() == chunk_size {
            delete_chunk(state.clone(), &mut hashes, bytes).await?;
            bytes = 0;
        }
    }

    if !hashes.is_empty() {
        delete_chunk(state.clone(), &mut hashes, bytes).await?;
    }

    Ok(())
}

async fn delete_chunk<E>(
    state: Arc<CleanupState>,
    hashes: &mut Vec<Hash<E>>,
    bytes: u64,
) -> Result<()>
where
    E: Entity,
    CommandStats: EntityStats<E>,
{
    let deleted_hashes = mem::take(hashes);
    let count = deleted_hashes.len() as u64;

    if !state.dry_run {
        let deleted_keys = deleted_hashes.iter().map(Hash::key);
        state.storage.delete_many(deleted_keys).await?;
        state.stats.write().await.bytes_deleted += bytes;
    }

    state.stats.write().await.add_entities_deleted(count as u64);

    for hash in deleted_hashes {
        let entity_name = E::NAME;
        let style = AnsiColor::Yellow.on_default();
        debug!("{style}deleted {entity_name}{style:#} {hash}");
    }

    Ok(())
}
