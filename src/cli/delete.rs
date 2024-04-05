use std::sync::Arc;

use humantime::format_duration;
use tokio::try_join;

use crate::{
    arc::{rwarc, unarc, unrwarc},
    error::Result,
    format::format_size,
    ops::{
        delete_archives_and_garbage_blocks, download_archive_records, download_block_records,
        expand_hashes, upload_archive_records, upload_block_records, CleanupState,
    },
    stats::CommandStats,
};

use super::{
    args::{DeleteArgs, StatsType},
    print_stat, print_stats_json,
    storage::create_storage,
};

pub async fn main(cli: DeleteArgs) -> Result<()> {
    let stats = rwarc(CommandStats::new());
    let storage = Arc::new(create_storage(&cli.global).await?);

    let archive_hashes = expand_hashes(storage.clone(), &cli.archives).await?;

    let (archive_records, block_records) = try_join!(
        download_archive_records(storage.clone()),
        download_block_records(storage.clone()),
    )?;
    let archive_records = rwarc(archive_records);
    let block_records = rwarc(block_records);

    let state = Arc::new(CleanupState {
        task_count: cli.tasks,
        dry_run: cli.dry_run,
        stats,
        storage,
        archive_records,
        block_records,
    });

    delete_archives_and_garbage_blocks(state.clone(), &archive_hashes).await?;

    let CleanupState {
        stats,
        storage,
        archive_records,
        block_records,
        ..
    } = unarc(state);
    let stats = unrwarc(stats);

    if !cli.dry_run {
        try_join!(
            Box::pin(upload_archive_records(storage.clone(), archive_records)),
            Box::pin(upload_block_records(storage.clone(), block_records)),
        )?;
    }

    let storage = unarc(storage);
    let full_stats = stats.finalize(storage.stats());

    match cli.global.stats {
        Some(StatsType::Basic) => {
            print_stat(
                "metadata downloaded",
                format_size(full_stats.metadata_bytes_downloaded()),
            );
            print_stat(
                "metadata uploaded",
                format_size(full_stats.metadata_bytes_uploaded()),
            );
            print_stat("bytes deleted", format_size(full_stats.bytes_deleted));
            print_stat("archives deleted", full_stats.archives_deleted);
            print_stat("blocks deleted", full_stats.blocks_deleted);
            print_stat("elapsed time", format_duration(full_stats.elapsed_time()));
        }
        Some(StatsType::Json) => {
            print_stats_json(&full_stats)?;
        }
        None => {}
    }

    Ok(())
}
