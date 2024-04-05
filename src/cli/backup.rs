use std::sync::Arc;

use clap::builder::styling::AnsiColor;
use humantime::format_duration;
use log::info;
use tokio::try_join;

use crate::{
    arc::{rwarc, unarc, unrwarc},
    archive::Archive,
    entity::EntityIndex,
    error::Result,
    format::format_size,
    locks::BlockLocks,
    ops::{
        backup_all, delete_blocks, download_archive_records, download_block_records,
        upload_archive, upload_archive_records, upload_block_records, upload_pending_files,
        BackupState,
    },
    stats::CommandStats,
};

use super::{
    args::{BackupArgs, StatsType},
    print_stat, print_stats_json,
    storage::create_storage,
};

pub async fn main(cli: BackupArgs) -> Result<()> {
    let stats = rwarc(CommandStats::new());
    let storage = Arc::new(create_storage(&cli.global).await?);
    let archive = rwarc(Archive::new());
    let block_locks = rwarc(BlockLocks::new());

    let (mut archive_records, block_records) = try_join!(
        download_archive_records(storage.clone()),
        download_block_records(storage.clone()),
    )?;
    let block_records = rwarc(block_records);

    let state = Arc::new(BackupState {
        compression_level: cli.compression_level,
        target_block_size: cli.target_block_size,
        task_count: cli.tasks,
        dry_run: cli.dry_run,
        stats,
        storage,
        archive,
        block_records,
        block_locks,
    });
    let (sender, receiver) = async_channel::bounded(state.task_count);

    try_join!(
        backup_all(state.clone(), sender, &cli.paths),
        upload_pending_files(state.clone(), receiver),
    )?;

    let BackupState {
        stats,
        storage,
        archive,
        block_records,
        ..
    } = unarc(state);
    let stats = unrwarc(stats);

    if cli.transient {
        let archive = unrwarc(archive);
        let mut block_records = unrwarc(block_records);
        let removed_blocks = block_records.remove_refs(&archive.block_refs)?;
        let removed_hashes = removed_blocks.iter().map(|(hash, _)| hash);
        delete_blocks(storage.clone(), removed_hashes).await?;
    } else {
        let (hash, record) = upload_archive(storage.clone(), stats.start_time, archive).await?;
        archive_records.insert(hash, record);

        if !cli.dry_run {
            try_join!(
                Box::pin(upload_block_records(storage.clone(), block_records.clone())),
                Box::pin(upload_archive_records(
                    storage.clone(),
                    rwarc(archive_records)
                )),
            )?;
        }

        let block_count = block_records.read().await.len();
        let short_hash = hash.format_short(block_count);
        let style = AnsiColor::Green.on_default();
        info!("{style}created archive{style:#} {short_hash}");
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
        Some(StatsType::Json) => {
            print_stats_json(&full_stats)?;
        }
        None => {}
    }

    Ok(())
}
