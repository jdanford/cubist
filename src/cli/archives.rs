use clap::builder::styling::AnsiColor;
use humantime::format_duration;
use log::info;
use tokio::try_join;

use crate::{
    arc::rwarc,
    error::Result,
    hash,
    ops::{download_archive_records, download_block_records},
    stats::CoreStats,
};

use super::format::format_size;

use super::{print_stat, storage::create_storage, ArchivesArgs};

pub async fn main(cli: ArchivesArgs) -> Result<()> {
    let stats = CoreStats::new();
    let storage = create_storage(&cli.global).await?;
    let storage = rwarc(storage);

    let (archive_records, block_records) = try_join!(
        download_archive_records(storage.clone()),
        download_block_records(storage.clone()),
    )?;

    for archive_record in archive_records.iter_by_created() {
        let formatted_time = &archive_record.created.format("%Y-%m-%d %H:%M");
        let short_hash = hash::format_short(&archive_record.hash, block_records.unique_count());
        let time_style = AnsiColor::Blue.on_default();
        info!("{time_style}{formatted_time}{time_style:#} {short_hash}");
    }

    if cli.global.stats {
        let full_stats = stats.finalize(storage.read().await.stats());
        print_stat(
            "metadata downloaded",
            format_size(full_stats.metadata_bytes_downloaded()),
        );
        print_stat("elapsed time", format_duration(full_stats.elapsed_time()));
    }

    Ok(())
}
