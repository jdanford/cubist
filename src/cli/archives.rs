use humantime::format_duration;
use log::info;

use crate::{
    cli::{self, format::format_size},
    error::Result,
    stats::CoreStats,
    storage,
};

use super::{print_stat, storage::create_storage};

pub async fn main(cli: cli::ArchivesArgs) -> Result<()> {
    let stats = CoreStats::new();
    let mut storage = create_storage(&cli.global).await?;

    let prefix = storage::ARCHIVE_KEY_PREFIX;
    let keys = storage.keys(Some(prefix)).await?;
    let mut archive_names = keys
        .into_iter()
        .map(|key| key.strip_prefix(prefix).unwrap().to_owned())
        .collect::<Vec<_>>();
    archive_names.sort();

    for archive_name in &archive_names {
        info!("{archive_name}");
    }

    if cli.global.stats {
        let full_stats = stats.finalize(storage.stats());
        print_stat(
            "metadata downloaded",
            format_size(full_stats.metadata_bytes_downloaded()),
        );
        print_stat("elapsed time", format_duration(full_stats.elapsed_time()));
    }

    Ok(())
}
