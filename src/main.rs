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
    let cli = Cli::parse();
    // let storage = CloudStorage::from_env().await;
    let storage = LocalStorage::new(PathBuf::from("data"), Duration::from_millis(10));
    match cli.command {
        Command::Backup {
            compression_level,
            target_block_size,
            bucket,
            paths,
        } => backup(storage, compression_level, target_block_size, bucket, paths).await,
        Command::Restore { bucket, path } => restore(storage, bucket, path).await,
    }
}
