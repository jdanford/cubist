use std::{path::PathBuf, time::Duration};

use clap::Parser;
use cubist::logger;
use cubist::{
    backup::backup,
    cli::{Cli, Command},
    error::Result,
    restore::restore,
    storage::{CloudStorage, LocalStorage},
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::new()
        .format(logger::format)
        .filter_level(log::LevelFilter::Info)
        .try_init()
        .unwrap();

    let cli = Cli::parse();
    // let storage = Box::new(CloudStorage::from_env().await);
    let storage = Box::new(LocalStorage::new(PathBuf::from("data"), Duration::from_millis(100)));
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
