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

use super::{
    arc::{rwarc, unarc, unrwarc},
    locks::BlockLocks,
    ops::download_archive,
    storage::create_storage,
};

use self::{
    blocks::LocalBlock,
    files::{download_pending_files, restore_recursive},
};

#[derive(Debug)]
struct Args {
    archive: Archive,
    paths: Vec<PathBuf>,
    jobs: u32,
}

#[derive(Debug)]
struct State {
    stats: Arc<RwLock<CoreStats>>,
    storage: Arc<RwLock<BoxedStorage>>,
    local_blocks: Arc<RwLock<HashMap<Hash, LocalBlock>>>,
    block_locks: Arc<RwLock<BlockLocks>>,
}

pub async fn main(cli: cli::RestoreArgs) -> Result<()> {
    let stats = rwarc(CoreStats::new());
    let storage = rwarc(create_storage(&cli.global).await?);
    let local_blocks = rwarc(HashMap::new());
    let block_locks = rwarc(BlockLocks::new());

    let archive = download_archive(storage.clone(), &cli.archive).await?;

    let args = Arc::new(Args {
        jobs: cli.jobs,
        paths: cli.paths,
        archive,
    });
    let state = Arc::new(State {
        stats,
        storage,
        local_blocks,
        block_locks,
    });
    let (sender, receiver) = async_channel::bounded(args.jobs as usize);

    let downloader_args = args.clone();
    let downloader_state = state.clone();
    let downloader_task = spawn(async move {
        download_pending_files(downloader_args, downloader_state, receiver).await;
    });

    restore_recursive(args, state.clone(), sender.clone()).await?;

    sender.close();
    downloader_task.await?;

    let State { stats, storage, .. } = unarc(state);
    let stats = unrwarc(stats);

    if cli.global.stats {
        let full_stats = stats.finalize(storage.read().await.stats());
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
