mod blocks;
mod files;
mod stats;

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use tokio::{spawn, task::spawn_blocking};

use crate::{
    archive::Archive,
    cli,
    error::Result,
    hash::Hash,
    serde::deserialize,
    storage::{self, BoxedStorage},
};

use self::{
    blocks::LocalBlock,
    files::{download_pending_files, restore_recursive},
    stats::Stats,
};

struct Args {
    storage: BoxedStorage,
    max_concurrency: usize,
    output_path: PathBuf,
    archive: Archive,
}

#[derive(Debug)]
struct State {
    local_blocks: HashMap<Hash, LocalBlock>,
    stats: Stats,
}

pub async fn main(args: cli::RestoreArgs) -> Result<()> {
    cli::init_logger(args.logger);
    let storage = cli::create_storage(args.storage).await;

    let local_blocks = HashMap::new();
    let mut stats = Stats::new();
    let archive = download_archive(&storage, &mut stats).await?;

    let args = Arc::new(Args {
        storage,
        max_concurrency: args.max_concurrency,
        output_path: args.path,
        archive,
    });
    let state = Arc::new(Mutex::new(State {
        local_blocks,
        stats,
    }));
    let (sender, receiver) = async_channel::bounded(args.max_concurrency);

    let downloader_args = args.clone();
    let downloader_state = state.clone();
    let downloader_task = spawn(async move {
        download_pending_files(downloader_args, downloader_state, receiver).await;
    });

    restore_recursive(args, state.clone(), sender.clone()).await?;

    sender.close();
    downloader_task.await?;

    let stats = &mut state.lock().unwrap().stats;
    stats.end();
    // TODO: print stats

    Ok(())
}

pub async fn download_archive(storage: &BoxedStorage, stats: &mut Stats) -> Result<Archive> {
    let timestamp_bytes = storage.get(storage::ARCHIVE_KEY_LATEST).await?;
    stats.bytes_downloaded += timestamp_bytes.len() as u64;
    let timestamp = String::from_utf8(timestamp_bytes)?;
    let key = storage::archive_key(&timestamp);

    let archive_bytes = storage.get(&key).await?;
    stats.bytes_downloaded += archive_bytes.len() as u64;
    let archive = spawn_blocking(move || deserialize(&archive_bytes)).await??;
    Ok(archive)
}
