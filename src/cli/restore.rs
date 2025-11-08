use std::{collections::HashMap, sync::Arc};

use humantime::format_duration;
use tokio::try_join;

use crate::{
    arc::{rwarc, unarc, unrwarc},
    error::Result,
    format::{format_size, format_speed},
    locks::BlockLocks,
    ops::{RestoreState, download_archive, download_pending_files, expand_hash, restore_all},
    stats::CommandStats,
};

use super::{
    args::{RestoreArgs, StatsType},
    print_stat, print_stats_json,
    storage::create_storage,
};

pub async fn main(cli: RestoreArgs) -> Result<()> {
    let stats = rwarc(CommandStats::new());
    let storage = Arc::new(create_storage(&cli.global).await?);
    let local_blocks = rwarc(HashMap::new());
    let block_locks = rwarc(BlockLocks::new());

    let archive_hash = expand_hash(storage.clone(), &cli.archive).await?;
    let archive = download_archive(storage.clone(), &archive_hash).await?;

    let state = Arc::new(RestoreState {
        order: cli.order,
        task_count: cli.tasks,
        dry_run: cli.dry_run,
        archive,
        stats,
        storage,
        local_blocks,
        block_locks,
    });
    let (sender, receiver) = async_channel::bounded(state.task_count);

    try_join!(
        restore_all(state.clone(), sender, &cli.paths),
        download_pending_files(state.clone(), receiver)
    )?;

    let RestoreState { stats, storage, .. } = unarc(state);
    let stats = unrwarc(stats);
    let storage = unarc(storage);
    let full_stats = stats.finalize(storage.stats());

    match cli.global.stats {
        Some(StatsType::Basic) => {
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
            print_stat("download speed", format_speed(full_stats.download_speed()));
        }
        Some(StatsType::Json) => {
            print_stats_json(&full_stats)?;
        }
        None => {}
    }

    Ok(())
}
