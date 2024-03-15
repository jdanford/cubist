mod blocks;
mod files;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use humantime::format_duration;
use log::info;
use tokio::{spawn, sync::RwLock};

use crate::{
    archive::Archive,
    cli,
    error::Result,
    hash::Hash,
    stats::{format_size, Stats},
    storage::{self, BoxedStorage},
};

use super::common::download_archive;

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
    stats: Stats,
    storage: BoxedStorage,
    local_blocks: HashMap<Hash, LocalBlock>,
}

pub async fn main(cli: cli::RestoreArgs) -> Result<()> {
    let stats = Stats::new();
    let storage = cli::create_storage(cli.global.storage).await;
    let storage_arc = Arc::new(RwLock::new(storage));
    let timestamp_bytes = storage_arc
        .write()
        .await
        .get(storage::ARCHIVE_KEY_LATEST)
        .await?;
    let timestamp = String::from_utf8(timestamp_bytes)?;
    let archive = download_archive(&timestamp, storage_arc.clone()).await?;

    let args = Arc::new(Args {
        max_concurrency: cli.max_concurrency,
        paths: cli.paths,
        archive,
    });

    let storage = Arc::try_unwrap(storage_arc).unwrap().into_inner();
    let local_blocks = HashMap::new();
    let state = Arc::new(RwLock::new(State {
        stats,
        storage,
        local_blocks,
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

    if cli.global.stats {
        info!(
            "bytes downloaded: {}",
            format_size(storage_stats.bytes_downloaded)
        );
        info!("bytes written: {}", format_size(stats.bytes_written));
        info!("files created: {}", stats.files_created);
        info!("blocks downloaded: {}", stats.blocks_downloaded);
        info!("blocks used: {}", stats.blocks_used);
        info!("elapsed time: {}", format_duration(elapsed_time));
    }

    Ok(())
}
