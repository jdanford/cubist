mod blocks;
mod files;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use humantime::format_duration;
use log::info;
use tokio::{spawn, sync::RwLock, task::spawn_blocking};

use crate::{
    archive::Archive,
    cli,
    error::Result,
    hash::Hash,
    serde::deserialize,
    stats::{format_size, Stats},
    storage::{self, BoxedStorage},
};

use self::{
    blocks::LocalBlock,
    files::{download_pending_files, restore_recursive},
};

#[derive(Debug)]
struct Args {
    max_concurrency: u32,
    paths: Vec<PathBuf>,
    archive: Archive,
}

#[derive(Debug)]
struct State {
    storage: BoxedStorage,
    local_blocks: HashMap<Hash, LocalBlock>,
    stats: Stats,
}

pub async fn main(args: cli::RestoreArgs) -> Result<()> {
    cli::init_logger(args.logger);
    let mut storage = cli::create_storage(args.storage).await;

    let local_blocks = HashMap::new();
    let stats = Stats::new();
    let archive = download_archive(&mut storage).await?;

    let args = Arc::new(Args {
        max_concurrency: args.max_concurrency,
        paths: args.paths,
        archive,
    });
    let state = Arc::new(RwLock::new(State {
        storage,
        local_blocks,
        stats,
    }));
    let (sender, receiver) = async_channel::bounded(args.max_concurrency as usize);

    let downloader_args = args.clone();
    let downloader_state = state.clone();
    let downloader_task = spawn(async move {
        download_pending_files(downloader_args, downloader_state, receiver).await;
    });

    restore_recursive(args, state.clone(), sender.clone()).await?;

    sender.close();
    downloader_task.await?;

    let State {
        storage, mut stats, ..
    } = Arc::try_unwrap(state).unwrap().into_inner();

    let elapsed_time = stats.end();
    let storage_stats = storage.stats();

    info!(
        "bytes downloaded: {}",
        format_size(storage_stats.bytes_downloaded)
    );
    info!("bytes written: {}", format_size(stats.bytes_written));
    info!("files created: {}", stats.files_created);
    info!("blocks downloaded: {}", stats.blocks_downloaded);
    info!("blocks used: {}", stats.blocks_used);
    info!("elapsed time: {}", format_duration(elapsed_time));
    Ok(())
}

pub async fn download_archive(storage: &mut BoxedStorage) -> Result<Archive> {
    let timestamp_bytes = storage.get(storage::ARCHIVE_KEY_LATEST).await?;
    let timestamp = String::from_utf8(timestamp_bytes)?;
    let key = storage::archive_key(&timestamp);

    let archive_bytes = storage.get(&key).await?;
    let archive = spawn_blocking(move || deserialize(&archive_bytes)).await??;
    Ok(archive)
}
