use humantime::format_duration;
use log::info;

use crate::{cli, error::Result, stats::Stats, storage::ARCHIVE_KEY_PREFIX};

use super::common::create_storage;

pub async fn main(cli: cli::ArchivesArgs) -> Result<()> {
    let mut stats = Stats::new();
    let mut storage = create_storage(cli.global.storage).await?;

    let prefix = ARCHIVE_KEY_PREFIX;
    let keys = storage.keys(Some(prefix)).await?;
    let mut archive_names = keys
        .into_iter()
        .map(|key| key.strip_prefix(prefix).unwrap().to_owned())
        .collect::<Vec<_>>();
    archive_names.sort();

    for archive_name in archive_names {
        info!("{archive_name}");
    }

    let elapsed_time = stats.end();
    // let storage_stats = storage.stats();

    if cli.global.stats {
        info!("elapsed time: {}", format_duration(elapsed_time));
    }

    Ok(())
}
