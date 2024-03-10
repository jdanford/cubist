mod blocks;
mod files;
mod stats;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use tokio::spawn;

use crate::{
    archive::Archive,
    cli::{self},
    error::Result,
    storage::BoxedStorage,
};

use self::{
    files::{backup_recursive, upload_archive, upload_pending_files},
    stats::Stats,
};

struct Args {
    storage: BoxedStorage,
    compression_level: u8,
    target_block_size: u32,
    max_concurrency: usize,
    paths: Vec<PathBuf>,
}

struct State {
    archive: Archive,
    stats: Stats,
}

pub async fn main(args: cli::BackupArgs) -> Result<()> {
    cli::init_logger(args.logger);
    let storage = cli::create_storage(args.storage).await;

    let time = Utc::now();
    let archive = Archive::new();
    let stats = Stats::new();
    let args = Arc::new(Args {
        storage,
        compression_level: args.compression_level,
        target_block_size: args.target_block_size,
        max_concurrency: args.max_concurrency,
        paths: args.paths,
    });
    let state = Arc::new(Mutex::new(State { archive, stats }));
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
    upload_archive(args, state.clone(), time).await?;

    let stats = &mut state.lock().unwrap().stats;
    stats.end();

    Ok(())
}
