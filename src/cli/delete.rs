use std::{collections::HashSet, sync::Arc};

use humantime::format_duration;
use log::info;
use tokio::{sync::RwLock, try_join};

use crate::{
    cli,
    error::{Error, Result},
    stats::{format_size, CoreStats},
};

use super::{
    common::{
        delete_archives, delete_blocks, download_archives, download_block_records,
        upload_block_records,
    },
    storage::create_storage,
};

pub async fn main(cli: cli::DeleteArgs) -> Result<()> {
    let mut stats = CoreStats::new();
    let storage = create_storage(&cli.global).await?;
    let storage_arc = Arc::new(RwLock::new(storage));

    let mut block_records = download_block_records(storage_arc.clone()).await?;
    let mut removed_blocks = HashSet::new();
    let archives =
        download_archives(storage_arc.clone(), &cli.archive_names, cli.max_concurrency).await?;

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

    delete_blocks(storage_arc.clone(), removed_blocks.iter().by_ref()).await?;
    stats.bytes_deleted += bytes_deleted;
    stats.blocks_deleted += blocks_deleted;

    try_join!(
        delete_archives(storage_arc.clone(), &cli.archive_names, cli.max_concurrency),
        upload_block_records(storage_arc.clone(), block_records),
    )?;

    if cli.global.stats {
        let full_stats = stats.finalize(storage_arc.read().await.stats());
        info!(
            "bytes downloaded: {}",
            format_size(full_stats.metadata_bytes_downloaded())
        );
        info!(
            "bytes uploaded: {}",
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
