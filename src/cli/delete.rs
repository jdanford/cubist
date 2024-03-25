use std::collections::HashSet;

use humantime::format_duration;
use log::debug;
use tokio::try_join;

use crate::{
    arc::rwarc,
    error::{Error, Result},
    ops::{
        delete_archives, delete_blocks, download_archive_records, download_archives,
        download_block_records, find_archive_hashes, upload_archive_records, upload_block_records,
    },
    stats::CoreStats,
};

use super::{format::format_size, print_stat, storage::create_storage, DeleteArgs};

pub async fn main(cli: DeleteArgs) -> Result<()> {
    let mut stats = CoreStats::new();
    let storage = rwarc(create_storage(&cli.global).await?);
    let mut removed_blocks = HashSet::new();

    let (mut archive_records, mut block_records) = try_join!(
        download_archive_records(storage.clone()),
        download_block_records(storage.clone()),
    )?;

    let archive_prefixes = &cli.archives.iter().collect::<Vec<_>>();
    let archive_hashes = find_archive_hashes(storage.clone(), archive_prefixes).await?;
    let archives = download_archives(storage.clone(), &archive_hashes, cli.tasks).await?;

    for archive in &archives {
        archive_records.remove(&archive.hash)?;

        let archive_garbage_blocks = block_records.remove_refs(&archive.block_refs)?;
        removed_blocks.extend(archive_garbage_blocks);
    }

    let mut bytes_deleted = 0;
    let mut blocks_deleted = 0;

    for hash in &removed_blocks {
        let record = block_records
            .get(hash)
            .ok_or_else(|| Error::BlockRecordNotFound(*hash))?;
        bytes_deleted += record.size;
        blocks_deleted += 1;
    }

    if !cli.dry_run {
        delete_blocks(storage.clone(), removed_blocks.iter().by_ref()).await?;
    }

    for hash in removed_blocks {
        debug!("deleted {hash}");
    }

    stats.bytes_deleted += bytes_deleted;
    stats.blocks_deleted += blocks_deleted;

    if !cli.dry_run {
        try_join!(
            delete_archives(storage.clone(), &archive_hashes, cli.tasks),
            upload_archive_records(storage.clone(), rwarc(archive_records)),
            upload_block_records(storage.clone(), rwarc(block_records)),
        )?;
    }

    if cli.global.stats {
        let full_stats = stats.finalize(storage.read().await.stats());
        print_stat(
            "metadata downloaded",
            format_size(full_stats.metadata_bytes_downloaded()),
        );
        print_stat(
            "metadata uploaded",
            format_size(full_stats.metadata_bytes_uploaded()),
        );
        print_stat("bytes deleted", format_size(full_stats.bytes_deleted));
        print_stat("blocks deleted", full_stats.blocks_deleted);
        print_stat("elapsed time", format_duration(full_stats.elapsed_time()));
    }

    Ok(())
}
