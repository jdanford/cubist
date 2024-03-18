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
    stats::{format_size, CoreStats},
    storage::BoxedStorage,
};

use super::{common::download_archive, storage::create_storage};

use self::{
    blocks::LocalBlock,
    files::{download_pending_files, restore_recursive},
};

#[derive(Debug)]
struct Args {
    archive: Archive,
    paths: Vec<PathBuf>,
    max_concurrency: u32,
}

#[derive(Debug)]
struct State {
    stats: CoreStats,
    storage: BoxedStorage,
    local_blocks: HashMap<Hash, LocalBlock>,
}

pub async fn main(cli: cli::RestoreArgs) -> Result<()> {
    let stats = CoreStats::new();
    let storage = create_storage(&cli.global).await?;
    let storage_arc = Arc::new(RwLock::new(storage));
    let archive = download_archive(storage_arc.clone(), &cli.archive_name).await?;

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

    let State { stats, storage, .. } = Arc::try_unwrap(state).unwrap().into_inner();

    if cli.global.stats {
        let full_stats = stats.finalize(storage.stats());
        info!(
            "content downloaded: {}",
            format_size(full_stats.content_bytes_downloaded)
        );
        info!(
            "metadata downloaded: {}",
            format_size(full_stats.metadata_bytes_downloaded())
        );
        info!("bytes written: {}", format_size(full_stats.bytes_written));
        info!("files created: {}", full_stats.files_created);
        info!("blocks downloaded: {}", full_stats.blocks_downloaded);
        info!("blocks referenced: {}", full_stats.blocks_referenced);
        info!(
            "elapsed time: {}",
            format_duration(full_stats.elapsed_time())
        );
    }

    Ok(())
}
