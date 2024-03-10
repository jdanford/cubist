mod blocks;
mod files;
mod stats;

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use tokio::spawn;

use crate::{archive::Archive, cli, error::Result, hash::Hash, storage::BoxedStorage};

use self::{
    blocks::LocalBlock,
    files::{download_archive, download_pending_files, restore_recursive},
    stats::Stats,
};

struct Args {
    storage: BoxedStorage,
    max_concurrency: usize,
    output_path: PathBuf,
    archive: Archive,
}

struct State {
    local_blocks: HashMap<Hash, LocalBlock>,
    stats: Stats,
}

pub async fn main(args: cli::RestoreArgs) -> Result<()> {
    cli::init_logger(args.logger);
    let storage = cli::create_storage(args.storage).await;

    let archive = download_archive(&storage).await?;
    let local_blocks = HashMap::new();
    let stats = Stats::new();
    let args = Arc::new(Args {
        storage,
        max_concurrency: args.max_concurrency,
        output_path: args.path,
        archive,
    });
    let state = Arc::new(Mutex::new(State {
        local_blocks,
        stats,
    }));
    let (sender, receiver) = async_channel::bounded(args.max_concurrency);

    let downloader_args = args.clone();
    let downloader_state = state.clone();
    let downloader_task = spawn(async move {
        download_pending_files(downloader_args, downloader_state, receiver).await;
    });

    restore_recursive(args, state.clone(), sender.clone()).await?;
    sender.close();
    downloader_task.await?;

    let stats = &mut state.lock().unwrap().stats;
    stats.end();

    Ok(())
}
