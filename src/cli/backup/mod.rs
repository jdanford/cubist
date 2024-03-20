mod blocks;
mod files;

use std::{fmt::Debug, path::PathBuf, sync::Arc};

use humantime::format_duration;
use log::info;
use tokio::{spawn, sync::RwLock, try_join};

use crate::{
    archive::Archive,
    block::BlockRecords,
    cli::{self, ops::upload_block_records},
    error::Result,
    stats::{format_size, CoreStats},
    storage::BoxedStorage,
};

use super::{
    arc::{rwarc, unarc, unrwarc},
    locks::BlockLocks,
    ops::{download_block_records, upload_archive},
    storage::create_storage,
};

use self::files::{backup_recursive, upload_pending_files};

#[derive(Debug)]
struct Args {
    paths: Vec<PathBuf>,
    compression_level: u8,
    target_block_size: u32,
    jobs: u32,
}

#[derive(Debug)]
struct State {
    stats: Arc<RwLock<CoreStats>>,
    storage: Arc<RwLock<BoxedStorage>>,
    archive: Arc<RwLock<Archive>>,
    block_records: Arc<RwLock<BlockRecords>>,
    block_locks: Arc<RwLock<BlockLocks>>,
}

pub async fn main(cli: cli::BackupArgs) -> Result<()> {
    let stats = rwarc(CoreStats::new());
    let storage = rwarc(create_storage(&cli.global).await?);
    let archive = rwarc(Archive::new());
    let block_locks = rwarc(BlockLocks::new());

    let block_records = rwarc(download_block_records(storage.clone()).await?);

    let args = Arc::new(Args {
        compression_level: cli.compression_level,
        target_block_size: cli.target_block_size,
        jobs: cli.jobs,
        paths: cli.paths,
    });
    let state = Arc::new(State {
        stats,
        storage,
        archive,
        block_records,
        block_locks,
    });
    let (sender, receiver) = async_channel::bounded(args.jobs as usize);

    let uploader_args = args.clone();
    let uploader_state = state.clone();
    let uploader_task =
        spawn(async move { upload_pending_files(uploader_args, uploader_state, receiver).await });

    for path in &args.paths {
        backup_recursive(args.clone(), state.clone(), sender.clone(), path).await?;
    }

    sender.close();
    uploader_task.await??;

    let State {
        stats,
        storage,
        archive,
        block_records,
        ..
    } = unarc(state);
    let stats = unrwarc(stats);

    try_join!(
        upload_archive(storage.clone(), archive.clone(), &stats),
        upload_block_records(storage.clone(), block_records),
    )?;

    if cli.global.stats {
        let full_stats = stats.finalize(storage.read().await.stats());
        info!(
            "metadata downloaded: {}",
            format_size(full_stats.metadata_bytes_downloaded())
        );
        info!(
            "content uploaded: {}",
            format_size(full_stats.content_bytes_uploaded)
        );
        info!(
            "metadata uploaded: {}",
            format_size(full_stats.metadata_bytes_uploaded())
        );
        info!("bytes read: {}", format_size(full_stats.bytes_read));
        info!("files read: {}", full_stats.files_read);
        info!("blocks uploaded: {}", full_stats.blocks_uploaded);
        info!("blocks referenced: {}", full_stats.blocks_referenced);
        info!(
            "elapsed time: {}",
            format_duration(full_stats.elapsed_time())
        );
    }

    Ok(())
}
