mod blocks;
mod files;

use std::{path::PathBuf, sync::Arc};

use humantime::format_duration;
use log::info;
use tokio::{spawn, sync::RwLock, try_join};

use crate::{
    archive::Archive,
    block::BlockRecords,
    cli::{self, common::upload_block_records},
    error::Result,
    stats::{format_size, CoreStats},
    storage::BoxedStorage,
};

use super::{
    common::{download_block_records, upload_archive},
    storage::create_storage,
};

use self::files::{backup_recursive, upload_pending_files};

#[derive(Debug)]
struct Args {
    paths: Vec<PathBuf>,
    compression_level: u8,
    target_block_size: u32,
    max_concurrency: u32,
}

#[derive(Debug)]
struct State {
    stats: CoreStats,
    storage: BoxedStorage,
    archive: Archive,
    block_records: BlockRecords,
}

pub async fn main(cli: cli::BackupArgs) -> Result<()> {
    let stats = CoreStats::new();
    let storage = create_storage(&cli.global).await?;
    let storage_arc = Arc::new(RwLock::new(storage));
    let block_records = download_block_records(storage_arc.clone()).await?;

    let args = Arc::new(Args {
        compression_level: cli.compression_level,
        target_block_size: cli.target_block_size,
        max_concurrency: cli.max_concurrency,
        paths: cli.paths,
    });

    let storage = Arc::try_unwrap(storage_arc).unwrap().into_inner();
    let archive = Archive::new();
    let state = Arc::new(RwLock::new(State {
        stats,
        storage,
        archive,
        block_records,
    }));

    let (sender, receiver) = async_channel::bounded(args.max_concurrency as usize);

    let uploader_args = args.clone();
    let uploader_state = state.clone();
    let uploader_task = spawn(async move {
        upload_pending_files(uploader_args, uploader_state, receiver).await;
    });

    for path in &args.paths {
        backup_recursive(args.clone(), state.clone(), sender.clone(), path).await?;
    }

    sender.close();
    uploader_task.await?;

    let State {
        stats,
        storage,
        archive,
        block_records,
    } = Arc::try_unwrap(state).unwrap().into_inner();
    let storage_arc = Arc::new(RwLock::new(storage));
    let archive = Arc::new(archive);

    try_join!(
        upload_archive(storage_arc.clone(), archive.clone(), &stats),
        upload_block_records(storage_arc.clone(), block_records),
    )?;

    if cli.global.stats {
        let full_stats = stats.finalize(storage_arc.read().await.stats());
        info!(
            "bytes downloaded: {}",
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
