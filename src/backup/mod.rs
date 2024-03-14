mod blocks;
mod files;

use std::{path::PathBuf, sync::Arc};

use humantime::format_duration;
use log::info;
use tokio::{spawn, sync::RwLock, task::spawn_blocking, try_join};

use crate::{
    archive::Archive,
    cli,
    error::Result,
    refs::RefCounts,
    serde::{deserialize, serialize},
    stats::{format_size, Stats},
    storage::{self, BoxedStorage},
};

use self::files::{backup_recursive, upload_pending_files};

#[derive(Debug)]
struct Args {
    compression_level: u8,
    target_block_size: u32,
    max_concurrency: u32,
    paths: Vec<PathBuf>,
}

#[derive(Debug)]
struct State {
    storage: BoxedStorage,
    archive: Archive,
    ref_counts: RefCounts,
    stats: Stats,
}

pub async fn main(args: cli::BackupArgs) -> Result<()> {
    cli::init_logger(args.logger);
    let mut storage = cli::create_storage(args.storage).await;

    let archive = Archive::new();
    let stats = Stats::new();
    let ref_counts = download_ref_counts(&mut storage).await?;

    let args = Arc::new(Args {
        compression_level: args.compression_level,
        target_block_size: args.target_block_size,
        max_concurrency: args.max_concurrency,
        paths: args.paths,
    });
    let state = Arc::new(RwLock::new(State {
        storage,
        archive,
        ref_counts,
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

    let State {
        storage,
        archive,
        ref_counts,
        mut stats,
    } = Arc::try_unwrap(state).unwrap().into_inner();
    let storage = Arc::new(RwLock::new(storage));
    let archive = Arc::new(archive);

    try_join!(
        update_ref_counts(storage.clone(), archive.clone(), ref_counts),
        upload_archive(storage.clone(), archive.clone(), &stats),
    )?;

    let elapsed_time = stats.end();
    let storage = storage.read().await;
    let storage_stats = storage.stats();

    info!(
        "bytes downloaded: {}",
        format_size(storage_stats.bytes_downloaded)
    );
    info!(
        "bytes uploaded: {}",
        format_size(storage_stats.bytes_uploaded)
    );
    info!("bytes read: {}", format_size(stats.bytes_read));
    info!("files read: {}", stats.files_read);
    info!("blocks uploaded: {}", stats.blocks_uploaded);
    info!("blocks used: {}", stats.blocks_used);
    info!("elapsed time: {}", format_duration(elapsed_time));
    Ok(())
}

async fn update_ref_counts(
    storage: Arc<RwLock<BoxedStorage>>,
    archive: Arc<Archive>,
    mut ref_counts: RefCounts,
) -> Result<()> {
    ref_counts.add(&archive.ref_counts);
    upload_ref_counts(storage, ref_counts).await
}

async fn upload_archive(
    storage: Arc<RwLock<BoxedStorage>>,
    archive: Arc<Archive>,
    stats: &Stats,
) -> Result<()> {
    let timestamp = stats.start_time.format("%Y%m%d%H%M%S").to_string();
    let key = storage::archive_key(&timestamp);
    let archive_bytes = spawn_blocking(move || serialize(archive.as_ref())).await??;
    storage.write().await.put(&key, archive_bytes).await?;
    storage
        .write()
        .await
        .put(storage::ARCHIVE_KEY_LATEST, timestamp.into())
        .await?;
    Ok(())
}

async fn download_ref_counts(storage: &mut BoxedStorage) -> Result<RefCounts> {
    let maybe_bytes = storage.try_get(storage::REF_COUNTS_KEY).await?;
    let ref_counts = if let Some(bytes) = maybe_bytes {
        spawn_blocking(move || deserialize(&bytes)).await??
    } else {
        RefCounts::new()
    };
    Ok(ref_counts)
}

async fn upload_ref_counts(
    storage: Arc<RwLock<BoxedStorage>>,
    ref_counts: RefCounts,
) -> Result<()> {
    let bytes = spawn_blocking(move || serialize(&ref_counts)).await??;
    storage
        .write()
        .await
        .put(storage::REF_COUNTS_KEY, bytes)
        .await?;
    Ok(())
}
