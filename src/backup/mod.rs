mod blocks;
mod files;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use humantime::format_duration;
use log::info;
use tokio::{spawn, task::spawn_blocking, try_join};

use crate::stats::format_size;
use crate::storage;
use crate::{
    archive::Archive,
    cli::{self},
    error::Result,
    refs::RefCounts,
    serde::{deserialize, serialize},
    stats::Stats,
    storage::BoxedStorage,
};

use self::files::{backup_recursive, upload_pending_files};

struct Args {
    storage: BoxedStorage,
    compression_level: u8,
    target_block_size: u32,
    max_concurrency: u32,
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
    let (sender, receiver) = async_channel::bounded(args.max_concurrency as usize);

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
        upload_archive(args.clone(), state.clone()),
        update_ref_counts(args.clone(), state.clone())
    )?;

    let stats = &mut state.lock().unwrap().stats;
    let elapsed_time = stats.end();

    info!("bytes downloaded: {}", format_size(stats.bytes_downloaded));
    info!("bytes uploaded: {}", format_size(stats.bytes_uploaded));
    info!("bytes read: {}", format_size(stats.bytes_read));
    info!("files read: {}", stats.files_read);
    info!("blocks uploaded: {}", stats.blocks_uploaded);
    info!("blocks used: {}", stats.blocks_used);
    info!("elapsed time: {}", format_duration(elapsed_time));

    Ok(())
}

async fn update_ref_counts(args: Arc<Args>, state: Arc<Mutex<State>>) -> Result<()> {
    let mut ref_counts = download_ref_counts(args.clone(), state.clone()).await?;
    ref_counts.add(&state.lock().unwrap().archive.ref_counts);
    upload_ref_counts(args.clone(), state.clone(), ref_counts).await
}

async fn download_ref_counts(args: Arc<Args>, state: Arc<Mutex<State>>) -> Result<RefCounts> {
    let maybe_bytes = args.storage.try_get(storage::REF_COUNTS_KEY).await?;
    let ref_counts = if let Some(bytes) = maybe_bytes {
        state.lock().unwrap().stats.bytes_downloaded += bytes.len() as u64;
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        RefCounts::new()
    };
    Ok(ref_counts)
}

async fn upload_ref_counts(
    args: Arc<Args>,
    state: Arc<Mutex<State>>,
    ref_counts: RefCounts,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&ref_counts)).await??;
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
