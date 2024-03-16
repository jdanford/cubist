use std::{collections::HashSet, sync::Arc};

use humantime::format_duration;
use log::info;
use tokio::{sync::RwLock, try_join};

use crate::{
    cli,
    error::Result,
    stats::{format_size, Stats},
};

use super::common::{
    create_storage, delete_archives, delete_blocks, download_archives, download_ref_counts,
    upload_ref_counts,
};

pub async fn main(cli: cli::DeleteArgs) -> Result<()> {
    let mut stats = Stats::new();
    let storage = create_storage(cli.global.storage).await?;
    let storage_arc = Arc::new(RwLock::new(storage));

    let mut ref_counts = download_ref_counts(storage_arc.clone()).await?;
    let mut garbage_blocks = HashSet::new();
    let archives =
        download_archives(storage_arc.clone(), &cli.archive_names, cli.max_concurrency).await?;

    for archive in archives {
        let archive_deleted_blocks = ref_counts.sub(&archive.ref_counts)?;
        garbage_blocks.extend(archive_deleted_blocks);
    }

    delete_blocks(storage_arc.clone(), garbage_blocks.iter().by_ref()).await?;
    stats.blocks_deleted += garbage_blocks.len() as u64;

    try_join!(
        delete_archives(storage_arc.clone(), &cli.archive_names, cli.max_concurrency),
        upload_ref_counts(storage_arc.clone(), ref_counts),
    )?;

    let elapsed_time = stats.end();
    let storage = storage_arc.read().await;
    let storage_stats = storage.stats();

    if cli.global.stats {
        info!(
            "bytes downloaded: {}",
            format_size(storage_stats.bytes_downloaded)
        );
        info!(
            "bytes uploaded: {}",
            format_size(storage_stats.bytes_uploaded)
        );
        info!("blocks deleted: {}", stats.blocks_deleted);
        info!("elapsed time: {}", format_duration(elapsed_time));
    }

    Ok(())
}
