use clap::builder::styling::AnsiColor;
use humantime::format_duration;
use log::info;

use crate::{arc::rwarc, error::Result, ops::download_archive_records, stats::CommandStats};

use super::{
    format::{format_size, format_time},
    print_stat,
    storage::create_storage,
    ArchivesArgs,
};

pub async fn main(cli: ArchivesArgs) -> Result<()> {
    let stats = CommandStats::new();
    let storage = rwarc(create_storage(&cli.global).await?);
    let archive_records = download_archive_records(storage.clone()).await?;

    for (hash, archive_record) in archive_records.iter_by_created() {
        let formatted_time = format_time(&archive_record.created);
        let time_style = AnsiColor::Blue.on_default();
        info!("{time_style}{formatted_time}{time_style:#} {hash}");
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
