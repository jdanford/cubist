use std::sync::Arc;

use humantime::format_duration;
use tokio::try_join;

use crate::{
    arc::{rwarc, unarc, unrwarc},
    error::Result,
    format::format_size,
    ops::{
        cleanup_archives, cleanup_blocks, download_archive_records, download_block_records,
        upload_archive_records, upload_block_records, CleanupArgs, CleanupState,
    },
    stats::CommandStats,
};

use super::{print_stat, storage::create_storage};

pub async fn main(cli: super::args::CleanupArgs) -> Result<()> {
    let stats = rwarc(CommandStats::new());
    let storage = Arc::new(create_storage(&cli.global).await?);

    let (archive_records, block_records) = try_join!(
        Box::pin(download_archive_records(storage.clone())),
        Box::pin(download_block_records(storage.clone())),
    )?;

    let archive_records = rwarc(archive_records);
    let block_records = rwarc(block_records);

    let args = Arc::new(CleanupArgs {
        tasks: cli.tasks,
        dry_run: cli.dry_run,
    });
    let state = Arc::new(CleanupState {
        stats,
        storage,
        archive_records,
        block_records,
    });

    try_join!(
        Box::pin(cleanup_archives(args.clone(), state.clone())),
        Box::pin(cleanup_blocks(args.clone(), state.clone())),
    )?;

    let CleanupState {
        stats,
        storage,
        archive_records,
        block_records,
    } = unarc(state);
    let stats = unrwarc(stats);

    if !cli.dry_run {
        try_join!(
            Box::pin(upload_archive_records(
                storage.clone(),
                archive_records.clone()
            )),
            Box::pin(upload_block_records(storage.clone(), block_records.clone())),
        )?;
    }

    if cli.global.stats {
        let storage = unarc(storage);
        let full_stats = stats.finalize(storage.stats());
        print_stat(
            "metadata downloaded",
            format_size(full_stats.metadata_bytes_downloaded()),
        );
        print_stat(
            "metadata uploaded",
            format_size(full_stats.metadata_bytes_uploaded()),
        );
        print_stat("archives deleted", full_stats.archives_deleted);
        print_stat("blocks deleted", full_stats.blocks_deleted);
        print_stat("elapsed time", format_duration(full_stats.elapsed_time()));
    }

    Ok(())
}
