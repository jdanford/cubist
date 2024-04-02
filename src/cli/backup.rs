use std::{collections::HashSet, sync::Arc};

use clap::builder::styling::AnsiColor;
use humantime::format_duration;
use log::info;
use tokio::{spawn, try_join};

use crate::{
    arc::{rwarc, unarc, unrwarc},
    archive::{Archive, ArchiveRecord},
    error::Result,
    format::format_size,
    hash,
    locks::BlockLocks,
    ops::{
        backup_recursive, delete_blocks, download_archive_records, download_block_records,
        upload_archive, upload_archive_records, upload_block_records, upload_pending_files,
        UploadArgs, UploadState,
    },
    stats::CommandStats,
};

use super::{print_stat, storage::create_storage, BackupArgs};

pub async fn main(cli: BackupArgs) -> Result<()> {
    let stats = rwarc(CommandStats::new());
    let storage = rwarc(create_storage(&cli.global).await?);
    let archive = rwarc(Archive::new());
    let block_locks = rwarc(BlockLocks::new());

    let (mut archive_records, block_records) = try_join!(
        download_archive_records(storage.clone()),
        download_block_records(storage.clone()),
    )?;
    let block_records = rwarc(block_records);

    let args = Arc::new(UploadArgs {
        paths: cli.paths,
        compression_level: cli.compression_level,
        target_block_size: cli.target_block_size,
        tasks: cli.tasks,
        dry_run: cli.dry_run,
    });
    let state = Arc::new(UploadState {
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

    let UploadState {
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
        let archive_record = ArchiveRecord {
            created: stats.start_time,
            tags: HashSet::new(),
        };
        let archive_hash = hash::archive(&archive_record);
        archive_records.insert(archive_hash, archive_record);

        if !cli.dry_run {
            try_join!(
                Box::pin(upload_archive(
                    storage.clone(),
                    &archive_hash,
                    archive.clone()
                )),
                Box::pin(upload_block_records(storage.clone(), block_records.clone())),
                Box::pin(upload_archive_records(
                    storage.clone(),
                    rwarc(archive_records)
                )),
            )?;
        }

        let block_count = block_records.read().await.unique_count();
        let short_hash = hash::format_short(&archive_hash, block_count);
        let style = AnsiColor::Green.on_default();
        info!("{style}created archive{style:#} {short_hash}");
    }

    if cli.global.stats {
        let storage = unrwarc(storage);
        let full_stats = stats.finalize(storage.stats());
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
