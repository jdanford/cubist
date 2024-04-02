use std::{collections::HashMap, sync::Arc};

use humantime::format_duration;
use tokio::spawn;

use crate::{
    arc::{rwarc, unarc, unrwarc},
    error::Result,
    format::format_size,
    locks::BlockLocks,
    ops::{
        download_archive, download_pending_files, find_archive_hash, restore_recursive,
        DownloadArgs, DownloadState,
    },
    stats::CommandStats,
};

use super::{print_stat, storage::create_storage, RestoreArgs};

pub async fn main(cli: RestoreArgs) -> Result<()> {
    let stats = rwarc(CommandStats::new());
    let storage = rwarc(create_storage(&cli.global).await?);
    let local_blocks = rwarc(HashMap::new());
    let block_locks = rwarc(BlockLocks::new());

    let archive_hash = find_archive_hash(storage.clone(), &cli.archive).await?;
    let archive = download_archive(storage.clone(), &archive_hash).await?;

    let args = Arc::new(DownloadArgs {
        archive,
        paths: cli.paths,
        order: cli.order,
        tasks: cli.tasks,
        dry_run: cli.dry_run,
    });
    let state = Arc::new(DownloadState {
        stats,
        storage,
        local_blocks,
        block_locks,
    });
    let (sender, receiver) = async_channel::bounded(args.tasks);

    let downloader_args = args.clone();
    let downloader_state = state.clone();
    let downloader_task = spawn(async move {
        download_pending_files(downloader_args, downloader_state, receiver).await
    });

    restore_recursive(args, state.clone(), sender.clone()).await?;

    sender.close();
    downloader_task.await??;

    let DownloadState { stats, storage, .. } = unarc(state);
    let stats = unrwarc(stats);

    if cli.global.stats {
        let storage = unrwarc(storage);
        let full_stats = stats.finalize(storage.stats());
        print_stat(
            "content downloaded",
            format_size(full_stats.content_bytes_downloaded),
        );
        print_stat(
            "metadata downloaded",
            format_size(full_stats.metadata_bytes_downloaded()),
        );
        print_stat("bytes written", format_size(full_stats.bytes_written));
        print_stat("files created", full_stats.files_created);
        print_stat("blocks downloaded", full_stats.blocks_downloaded);
        print_stat("blocks referenced", full_stats.blocks_referenced);
        print_stat("elapsed time", format_duration(full_stats.elapsed_time()));
    }

    Ok(())
}
