mod blocks;
mod files;

use std::{path::PathBuf, sync::Arc};

use humantime::format_duration;
use log::info;
use tokio::{spawn, sync::RwLock, try_join};

use crate::{
    archive::Archive,
    cli,
    error::Result,
    refs::RefCounts,
    stats::{format_size, Stats},
    storage::BoxedStorage,
};

use super::common::{create_storage, download_ref_counts, update_ref_counts, upload_archive};

use self::files::{backup_recursive, upload_pending_files};

#[derive(Debug)]
struct Args {
    compression_level: u8,
    target_block_size: u32,
    max_concurrency: u32,
    paths: Vec<PathBuf>,
    ref_counts: RefCounts,
}

#[derive(Debug)]
struct State {
    stats: Stats,
    storage: BoxedStorage,
    archive: Archive,
}

pub async fn main(cli: cli::BackupArgs) -> Result<()> {
    let stats = Stats::new();
    let storage = create_storage(cli.global.storage).await?;
    let storage_arc = Arc::new(RwLock::new(storage));
    let ref_counts = download_ref_counts(storage_arc.clone()).await?;

    let args = Arc::new(Args {
        compression_level: cli.compression_level,
        target_block_size: cli.target_block_size,
        max_concurrency: cli.max_concurrency,
        paths: cli.paths,
        ref_counts,
    });

    let storage = Arc::try_unwrap(storage_arc).unwrap().into_inner();
    let archive = Archive::new();
    let state = Arc::new(RwLock::new(State {
        stats,
        storage,
        archive,
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

    let Args { ref_counts, .. } = Arc::try_unwrap(args).unwrap();
    let State {
        mut stats,
        storage,
        archive,
    } = Arc::try_unwrap(state).unwrap().into_inner();
    let storage = Arc::new(RwLock::new(storage));
    let archive = Arc::new(archive);

    try_join!(
        upload_archive(storage.clone(), archive.clone(), &stats),
        update_ref_counts(storage.clone(), ref_counts, &archive.ref_counts),
    )?;

    let elapsed_time = stats.end();
    let storage = storage.read().await;
    let storage_stats = storage.stats();

    if cli.global.stats {
        info!(
            "bytes downloaded: {}",
            format_size(storage_stats.bytes_downloaded)
        );
        info!(
            "bytes uploaded: {}",
            format_size(storage_stats.bytes_uploaded)
        );
        info!("bytes read: {}", format_size(stats.bytes_read));
        info!("files read: {}", stats.files_read);
        info!("blocks uploaded: {}", stats.blocks_uploaded);
        info!("blocks used: {}", stats.blocks_used);
        info!("elapsed time: {}", format_duration(elapsed_time));
    }

    Ok(())
}
