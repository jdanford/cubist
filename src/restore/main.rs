use std::{collections::HashMap, sync::{Arc, Mutex}};

use tokio::spawn;

use crate::{cli, error::Result};

use super::{
    download_archive, download_pending_files, restore_recursive, RestoreArgs, RestoreState,
};

pub async fn main(args: cli::RestoreArgs) -> Result<()> {
    cli::init_logger(args.logger);

    let storage = cli::create_storage(args.storage).await;
    let archive = download_archive(&storage).await?;
    let local_blocks = HashMap::new();

    let args = Arc::new(RestoreArgs {
        storage,
        max_concurrency: args.max_concurrency,
        output_path: args.path,
        archive,
    });
    let state = Arc::new(Mutex::new(RestoreState { local_blocks }));

    let (sender, receiver) = async_channel::bounded(args.max_concurrency);

    let downloader_args = args.clone();
    let downloader_state = state.clone();
    let downloader_task = spawn(async move {
        download_pending_files(downloader_args, downloader_state, receiver).await;
    });

    restore_recursive(args, state, sender).await?;
    downloader_task.await?;
    Ok(())
}
