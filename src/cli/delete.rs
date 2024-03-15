use std::sync::Arc;

use humantime::format_duration;
use log::info;
use tokio::{sync::RwLock, try_join};

use crate::{cli, error::Result, stats::Stats};

use super::common::{
    delete_archive, download_archive, download_ref_counts, remove_blocks, update_ref_counts,
};

pub async fn main(cli: cli::DeleteArgs) -> Result<()> {
    let mut stats = Stats::new();
    let storage = cli::create_storage(cli.global.storage).await;
    let storage_arc = Arc::new(RwLock::new(storage));

    let (archive, mut ref_counts) = try_join!(
        download_archive(&cli.archive_name, storage_arc.clone()),
        download_ref_counts(storage_arc.clone()),
    )?;

    let removed_hashes = ref_counts.sub(&archive.ref_counts)?;

    delete_archive(storage_arc.clone(), cli.archive_name).await?;
    remove_blocks(storage_arc.clone(), removed_hashes.iter().by_ref()).await?;
    update_ref_counts(storage_arc.clone(), ref_counts, &archive.ref_counts).await?;
    // TODO: do something about `archive:latest`

    let elapsed_time = stats.end();
    // let storage = storage.read().await;
    // let storage_stats = storage.stats();

    if cli.global.stats {
        info!("elapsed time: {}", format_duration(elapsed_time));
    }

    Ok(())
}
