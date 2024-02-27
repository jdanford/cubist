use std::{path::PathBuf, time::Duration};

use clap::Parser;
use cubist::{
    backup::backup,
    cli::{Cli, Command},
    error::Result,
    restore::restore,
    // storage::CloudStorage,
    storage::LocalStorage,
};

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::formatted_builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()
        .unwrap();

    let cli = Cli::parse();
    // let storage = CloudStorage::from_env().await;
    let storage = LocalStorage::new(PathBuf::from("data"), Duration::from_millis(50));
    match cli.command {
        Command::Backup {
            compression_level,
            target_block_size,
            max_concurrency,
            bucket,
            paths,
        } => {
            backup(
                storage,
                compression_level,
                target_block_size,
                max_concurrency,
                bucket,
                paths,
            )
            .await
        }
        Command::Restore {
            max_concurrency,
            bucket,
            path,
        } => restore(storage, max_concurrency, bucket, path).await,
    }
}
