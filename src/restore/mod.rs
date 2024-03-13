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

struct Args {
    max_concurrency: u32,
    output_path: PathBuf,
    archive: Archive,
}

struct State {
    storage: BoxedStorage,
    local_blocks: HashMap<Hash, LocalBlock>,
    stats: Stats,
}

pub async fn main(args: cli::RestoreArgs) -> Result<()> {
    cli::init_logger(args.logger);
    let mut storage = cli::create_storage(args.storage).await;

    let local_blocks = HashMap::new();
    let mut stats = Stats::new();
    let archive = download_archive(&mut storage, &mut stats).await?;

    let args = Arc::new(Args {
        max_concurrency: args.max_concurrency,
        output_path: args.path,
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

    let mut locked_state = state.write().await;
    let elapsed_time = locked_state.stats.end();
    let main_stats = &locked_state.stats;
    let storage_stats = locked_state.storage.stats();

    info!(
        "bytes downloaded: {}",
        format_size(storage_stats.bytes_downloaded)
    );
    info!("bytes written: {}", format_size(main_stats.bytes_written));
    info!("files created: {}", main_stats.files_created);
    info!("blocks downloaded: {}", main_stats.blocks_downloaded);
    info!("blocks used: {}", main_stats.blocks_used);
    info!("elapsed time: {}", format_duration(elapsed_time));

    Ok(())
}

pub async fn download_archive(storage: &mut BoxedStorage, _stats: &mut Stats) -> Result<Archive> {
    let timestamp_bytes = storage.get(storage::ARCHIVE_KEY_LATEST).await?;
    let timestamp = String::from_utf8(timestamp_bytes)?;
    let key = storage::archive_key(&timestamp);

    let archive_bytes = storage.get(&key).await?;
    let archive = spawn_blocking(move || deserialize(&archive_bytes)).await??;
    Ok(archive)
}
