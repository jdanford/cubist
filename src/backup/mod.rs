mod blocks;
mod files;
mod stats;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::task::spawn_blocking;
use tokio::{spawn, try_join};

use crate::storage;
use crate::{
    archive::Archive,
    cli::{self},
    error::Result,
    refs::RefCounts,
    serde::{deserialize, serialize},
    storage::BoxedStorage,
};

use self::{
    files::{backup_recursive, upload_pending_files},
    stats::Stats,
};

struct Args {
    storage: BoxedStorage,
    compression_level: u8,
    target_block_size: u32,
    max_concurrency: usize,
    paths: Vec<PathBuf>,
}

#[derive(Debug)]
struct State {
    archive: Archive,
    stats: Stats,
}

pub async fn main(args: cli::BackupArgs) -> Result<()> {
    cli::init_logger(args.logger);
    let storage = cli::create_storage(args.storage).await;

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

    try_join!(
        async { upload_archive(args.clone(), state.clone()).await },
        async {
            // TODO: don't ignore all errors
            let mut ref_counts = download_ref_counts(args.clone(), state.clone())
                .await
                .unwrap_or_else(|_| RefCounts::new());
            ref_counts.add(&state.lock().unwrap().archive.ref_counts);
            upload_ref_counts(args.clone(), state.clone(), &ref_counts).await
        }
    )?;

    let stats = &mut state.lock().unwrap().stats;
    stats.end();
    // TODO: print stats

    Ok(())
}

async fn download_ref_counts(args: Arc<Args>, state: Arc<Mutex<State>>) -> Result<RefCounts> {
    let bytes = args.storage.get(storage::REF_COUNTS_KEY).await?;
    state.lock().unwrap().stats.bytes_downloaded += bytes.len() as u64;

    let ref_counts = deserialize(&bytes)?;
    Ok(ref_counts)
}

async fn upload_ref_counts(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    ref_counts: &RefCounts,
) -> Result<()> {
    let bytes = serialize(ref_counts)?;
    let size = bytes.len() as u64;

    args.storage.put(storage::REF_COUNTS_KEY, bytes).await?;
    state.lock().unwrap().stats.bytes_uploaded += size;
    Ok(())
}

async fn upload_archive(args: Arc<Args>, state: Arc<Mutex<State>>) -> Result<()> {
    let time = state.lock().unwrap().stats.start_time;
    let timestamp = time.format("%Y%m%d%H%M%S").to_string();
    let key = storage::archive_key(&timestamp);
    let timestamp_bytes = timestamp.into_bytes();
    let timestamp_size = timestamp_bytes.len() as u64;

    let task_state = state.clone();
    let archive_bytes =
        spawn_blocking(move || serialize(&task_state.lock().unwrap().archive)).await??;
    let archive_size = archive_bytes.len() as u64;

    args.storage.put(&key, archive_bytes).await?;
    state.lock().unwrap().stats.bytes_uploaded = archive_size;

    args.storage
        .put(storage::ARCHIVE_KEY_LATEST, timestamp_bytes)
        .await?;
    state.lock().unwrap().stats.bytes_uploaded = timestamp_size;
    Ok(())
}
