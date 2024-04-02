use std::sync::Arc;

use clap::builder::styling::AnsiColor;
use humantime::format_duration;
use log::debug;
use tokio::try_join;

use crate::{
    arc::{rwarc, unarc},
    error::Result,
    format::format_size,
    ops::{
        delete_archives, delete_blocks, download_archive_records, download_archives,
        download_block_records, find_archive_hashes, upload_archive_records, upload_block_records,
    },
    stats::CommandStats,
};

use super::{print_stat, storage::create_storage, DeleteArgs};

pub async fn main(cli: DeleteArgs) -> Result<()> {
    let mut stats = CommandStats::new();
    let storage = Arc::new(create_storage(&cli.global).await?);
    let mut removed_blocks = vec![];

    let archive_prefixes = &cli.archives.iter().collect::<Vec<_>>();
    let archive_hashes = find_archive_hashes(storage.clone(), archive_prefixes).await?;

    let (archives, mut archive_records, mut block_records) = try_join!(
        Box::pin(download_archives(
            storage.clone(),
            &archive_hashes,
            cli.tasks
        )),
        Box::pin(download_archive_records(storage.clone())),
        Box::pin(download_block_records(storage.clone())),
    )?;

    for (hash, archive) in &archives {
        archive_records.remove(hash)?;

        let archive_garbage_blocks = block_records.remove_refs(&archive.block_refs)?;
        removed_blocks.extend(archive_garbage_blocks);
        stats.archives_deleted += 1;
    }

    let mut block_hashes = vec![];

    for (hash, record) in removed_blocks {
        block_hashes.push(hash);
        stats.bytes_deleted += record.size;
        stats.blocks_deleted += 1;
    }

    if !cli.dry_run {
        delete_blocks(storage.clone(), block_hashes.iter()).await?;
    }

    for hash in block_hashes {
        let style = AnsiColor::Yellow.on_default();
        debug!("{style}deleted block{style:#} {hash}");
    }

    if !cli.dry_run {
        try_join!(
            Box::pin(delete_archives(storage.clone(), &archive_hashes)),
            Box::pin(upload_archive_records(
                storage.clone(),
                rwarc(archive_records)
            )),
            Box::pin(upload_block_records(storage.clone(), rwarc(block_records))),
        )?;
    }

    for hash in archive_hashes {
        let style = AnsiColor::Yellow.on_default();
        debug!("{style}deleted archive{style:#} {hash}");
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
        print_stat("bytes deleted", format_size(full_stats.bytes_deleted));
        print_stat("archives deleted", full_stats.archives_deleted);
        print_stat("blocks deleted", full_stats.blocks_deleted);
        print_stat("elapsed time", format_duration(full_stats.elapsed_time()));
    }

    Ok(())
}
