mod blocks;
mod files;

use std::path::PathBuf;
use std::sync::Arc;

use humantime::format_duration;
use log::info;
use tokio::sync::RwLock;
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
    compression_level: u8,
    target_block_size: u32,
    max_concurrency: u32,
    paths: Vec<PathBuf>,
}

struct State {
    storage: BoxedStorage,
    archive: Archive,
    stats: Stats,
}

pub async fn main(args: cli::BackupArgs) -> Result<()> {
    cli::init_logger(args.logger);
    let storage = cli::create_storage(args.storage).await;

    let archive = Archive::new();
    let stats = Stats::new();
    let args = Arc::new(Args {
        compression_level: args.compression_level,
        target_block_size: args.target_block_size,
        max_concurrency: args.max_concurrency,
        paths: args.paths,
    });
    let state = Arc::new(RwLock::new(State {
        storage,
        archive,
        stats,
    }));
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
        update_ref_counts(args.clone(), state.clone()),
    )?;

    let mut locked_state = state.write().await;
    let elapsed_time = locked_state.stats.end();
    let main_stats = &locked_state.stats;
    let storage_stats = locked_state.storage.stats();

    info!(
        "bytes downloaded: {}",
        format_size(storage_stats.bytes_downloaded)
    );
    info!(
        "bytes uploaded: {}",
        format_size(storage_stats.bytes_uploaded)
    );
    info!("bytes read: {}", format_size(main_stats.bytes_read));
    info!("files read: {}", main_stats.files_read);
    info!("blocks uploaded: {}", main_stats.blocks_uploaded);
    info!("blocks used: {}", main_stats.blocks_used);
    info!("elapsed time: {}", format_duration(elapsed_time));

    Ok(())
}

async fn update_ref_counts(args: Arc<Args>, state: Arc<RwLock<State>>) -> Result<()> {
    let mut ref_counts = download_ref_counts(args.clone(), state.clone()).await?;
    ref_counts.add(&state.read().await.archive.ref_counts);
    upload_ref_counts(args.clone(), state.clone(), ref_counts).await
}

async fn download_ref_counts(_args: Arc<Args>, state: Arc<RwLock<State>>) -> Result<RefCounts> {
    let maybe_bytes = state
        .write()
        .await
        .storage
        .try_get(storage::REF_COUNTS_KEY)
        .await?;
    let ref_counts = if let Some(bytes) = maybe_bytes {
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        RefCounts::new()
    };
    Ok(ref_counts)
}

async fn upload_ref_counts(
    _args: Arc<Args>,
    state: Arc<RwLock<State>>,
    ref_counts: RefCounts,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&ref_counts)).await??;
    state
        .write()
        .await
        .storage
        .put(storage::REF_COUNTS_KEY, bytes)
        .await?;
    Ok(())
}

async fn upload_archive(_args: Arc<Args>, state: Arc<RwLock<State>>) -> Result<()> {
    let time = state.read().await.stats.start_time;
    let timestamp = time.format("%Y%m%d%H%M%S").to_string();
    let key = storage::archive_key(&timestamp);

    let task_state = state.clone();
    let archive_bytes =
        spawn_blocking(move || serialize(&task_state.blocking_read().archive)).await??;

    state.write().await.storage.put(&key, archive_bytes).await?;
    state
        .write()
        .await
        .storage
        .put(storage::ARCHIVE_KEY_LATEST, timestamp.into())
        .await?;
    Ok(())
}
