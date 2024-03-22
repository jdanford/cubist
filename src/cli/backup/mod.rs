mod blocks;
mod files;

use std::{fmt::Debug, path::PathBuf, sync::Arc};

use humantime::format_duration;
use tokio::{spawn, sync::RwLock, try_join};

use crate::{
    archive::Archive,
    block::BlockRecords,
    cli::{self, format::format_size, ops::upload_block_records},
    error::Result,
    stats::CoreStats,
    storage::BoxedStorage,
};

use super::{
    arc::{rwarc, unarc, unrwarc},
    locks::BlockLocks,
    ops::{download_block_records, upload_archive},
    print_stat,
    storage::create_storage,
};

use self::files::{backup_recursive, upload_pending_files};

#[derive(Debug)]
struct Args {
    paths: Vec<PathBuf>,
    compression_level: u8,
    target_block_size: u32,
    tasks: usize,
    dry_run: bool,
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
        paths: cli.paths,
        compression_level: cli.compression_level,
        target_block_size: cli.target_block_size,
        tasks: cli.tasks,
        dry_run: cli.dry_run,
    });
    let state = Arc::new(State {
        stats,
        storage,
        archive,
        block_records,
        block_locks,
    });
    let (sender, receiver) = async_channel::bounded(args.tasks);

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

    if !cli.dry_run {
        try_join!(
            upload_archive(storage.clone(), archive.clone(), &stats),
            upload_block_records(storage.clone(), block_records),
        )?;
    }

    if cli.global.stats {
        let full_stats = stats.finalize(storage.read().await.stats());
        print_stat(
            "metadata downloaded",
            format_size(full_stats.metadata_bytes_downloaded()),
        );
        print_stat(
            "content uploaded",
            format_size(full_stats.content_bytes_uploaded),
        );
        print_stat(
            "metadata uploaded",
            format_size(full_stats.metadata_bytes_uploaded()),
        );
        print_stat("bytes read", format_size(full_stats.bytes_read));
        print_stat("files read", full_stats.files_read);
        print_stat("blocks uploaded", full_stats.blocks_uploaded);
        print_stat("blocks referenced", full_stats.blocks_referenced);
        print_stat("elapsed time", format_duration(full_stats.elapsed_time()));
    }

    Ok(())
}
