use std::sync::{Arc, Mutex};

use chrono::Utc;
use tokio::spawn;

use crate::{
    cli::{self},
    error::Result,
    file::Archive,
};

use super::{backup_recursive, upload_archive, upload_pending_files, BackupArgs, BackupState};

pub async fn main(args: cli::BackupArgs) -> Result<()> {
    cli::init_logger(args.logger);

    let time = Utc::now();
    let archive = Archive::new();

    let storage = cli::create_storage(args.storage).await;
    let args = Arc::new(BackupArgs {
        storage,
        compression_level: args.compression_level,
        target_block_size: args.target_block_size,
        max_concurrency: args.max_concurrency,
        paths: args.paths,
    });
    let state = Arc::new(Mutex::new(BackupState { archive }));

    let (sender, receiver) = async_channel::bounded(args.max_concurrency);

    let uploader_args = args.clone();
    let uploader_state = state.clone();
    let uploader_task = spawn(async move {
        upload_pending_files(uploader_args, uploader_state, receiver).await;
    });

    for path in &args.paths {
        backup_recursive(args.clone(), state.clone(), sender.clone(), path).await?;
    }

    sender.close();
    uploader_task.await?;
    upload_archive(args, state, time).await?;
    Ok(())
}
