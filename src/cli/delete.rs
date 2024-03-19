use std::collections::HashSet;

use humantime::format_duration;
use log::info;
use tokio::try_join;

use crate::{
    cli,
    error::{Error, Result},
    stats::{format_size, CoreStats},
};

use super::{
    arc::rwarc,
    ops::{
        delete_archives, delete_blocks, download_archives, download_block_records,
        upload_block_records,
    },
    storage::create_storage,
};

pub async fn main(cli: cli::DeleteArgs) -> Result<()> {
    let mut stats = CoreStats::new();
    let storage = rwarc(create_storage(&cli.global).await?);
    let mut removed_blocks = HashSet::new();
    let (archives, mut block_records) = try_join!(
        download_archives(storage.clone(), &cli.archives, cli.jobs),
        download_block_records(storage.clone()),
    )?;

    for archive in &archives {
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

    delete_blocks(storage.clone(), removed_blocks.iter().by_ref()).await?;
    stats.bytes_deleted += bytes_deleted;
    stats.blocks_deleted += blocks_deleted;

    try_join!(
        delete_archives(storage.clone(), &cli.archives, cli.jobs),
        upload_block_records(storage.clone(), rwarc(block_records)),
    )?;

    if cli.global.stats {
        let full_stats = stats.finalize(storage.read().await.stats());
        info!(
            "metadata downloaded: {}",
            format_size(full_stats.metadata_bytes_downloaded())
        );
        info!(
            "metadata uploaded: {}",
            format_size(full_stats.metadata_bytes_uploaded())
        );
        info!("bytes deleted: {}", format_size(full_stats.bytes_deleted));
        info!("blocks deleted: {}", full_stats.blocks_deleted);
        info!(
            "elapsed time: {}",
            format_duration(full_stats.elapsed_time())
        );
    }

    Ok(())
}
