use std::{collections::HashSet, pin::pin};

use clap::builder::styling::AnsiColor;
use humantime::format_duration;
use log::debug;
use tokio::try_join;
use tokio_stream::StreamExt;

use crate::{
    arc::{rwarc, unrwarc},
    error::Result,
    format::format_size,
    keys::{self, hash_from_key},
    ops::{
        delete_archives, delete_blocks, download_archive_records, download_block_records,
        upload_archive_records, upload_block_records,
    },
    stats::CommandStats,
};

use super::{print_stat, storage::create_storage};

pub async fn main(cli: super::args::CleanupArgs) -> Result<()> {
    let mut stats = CommandStats::new();
    let storage = rwarc(create_storage(&cli.global).await?);

    let (mut archive_records, mut block_records) = try_join!(
        Box::pin(download_archive_records(storage.clone())),
        Box::pin(download_block_records(storage.clone())),
    )?;

    let mut archive_hashes = HashSet::new();
    let mut block_hashes = HashSet::new();

    {
        let storage_lock = storage.read().await;
        let mut archive_keys = pin!(storage_lock.keys(Some(keys::ARCHIVE_NAMESPACE)));
        let mut block_keys = pin!(storage_lock.keys(Some(keys::BLOCK_NAMESPACE)));

        while let Some(key) = archive_keys.try_next().await? {
            let hash = hash_from_key(keys::ARCHIVE_NAMESPACE, &key)?;
            archive_hashes.insert(hash);
        }

        while let Some(key) = block_keys.try_next().await? {
            let hash = hash_from_key(keys::BLOCK_NAMESPACE, &key)?;
            block_hashes.insert(hash);
        }
    }

    let mut removed_archives = vec![];
    let mut removed_blocks = vec![];

    for hash in archive_records.keys() {
        if !archive_hashes.contains(hash) {
            removed_archives.push(hash);
        }
    }

    for hash in block_records.keys() {
        if !block_hashes.contains(hash) {
            removed_blocks.push(hash);
        }
    }

    if !cli.dry_run {
        try_join!(
            Box::pin(delete_archives(storage.clone(), &archive_hashes)),
            Box::pin(delete_blocks(storage.clone(), &block_hashes)),
        )?;
    }

    for hash in block_hashes {
        block_records.remove(&hash)?;
        stats.blocks_deleted += 1;

        let style = AnsiColor::Yellow.on_default();
        debug!("{style}deleted block{style:#} {hash}");
    }

    for hash in archive_hashes {
        archive_records.remove(&hash)?;
        stats.archives_deleted += 1;

        let style = AnsiColor::Yellow.on_default();
        debug!("{style}deleted archive{style:#} {hash}");
    }

    if !cli.dry_run {
        try_join!(
            Box::pin(upload_block_records(storage.clone(), rwarc(block_records))),
            Box::pin(upload_archive_records(
                storage.clone(),
                rwarc(archive_records)
            )),
        )?;
    }

    if cli.global.stats {
        let storage = unrwarc(storage);
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
