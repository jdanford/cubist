use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    pin::pin,
    sync::{Arc, Mutex},
};

use async_channel::{Receiver, Sender};
use chrono::{DateTime, Utc};
use log::{info, warn};
use tokio::{
    fs::{self, File},
    spawn,
    sync::Semaphore,
    task::spawn_blocking,
};
use tokio_stream::StreamExt;
use walkdir::{DirEntry, WalkDir};

use crate::{
    block::{self, BlockHash},
    error::Result,
    file::{read_metadata, Archive, FileHash, Node},
    hash,
    storage::{LocalStorage, Storage},
};

struct BackupArgs {
    storage: LocalStorage,
    compression_level: u32,
    target_block_size: u32,
    max_concurrency: usize,
    bucket: String,
    paths: Vec<PathBuf>,
}

struct BackupState {
    archive: Archive,
}

struct PendingFile {
    local_path: PathBuf,
    archive_path: PathBuf,
}

pub async fn backup(
    storage: LocalStorage,
    compression_level: u32,
    target_block_size: u32,
    max_concurrency: usize,
    bucket: String,
    paths: Vec<PathBuf>,
) -> Result<()> {
    let time = Utc::now();
    let archive = Archive::new();
    let args = Arc::new(BackupArgs {
        storage,
        compression_level,
        target_block_size,
        max_concurrency,
        bucket,
        paths,
    });
    let state = Arc::new(Mutex::new(BackupState { archive }));
    let (sender, receiver) = async_channel::bounded(args.max_concurrency);

    let uploader_args = args.clone();
    let uploader_state = state.clone();
    let uploader_task = spawn(async move {
        upload_pending_files(
            uploader_args.clone(),
            uploader_state.clone(),
            receiver.clone(),
        )
        .await;
    });

    for path in &args.paths {
        backup_recursive(args.clone(), state.clone(), sender.clone(), path).await?;
    }

    uploader_task.await?;
    upload_archive(args.clone(), state.clone(), time).await?;
    Ok(())
}

async fn upload_archive(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    time: DateTime<Utc>,
) -> Result<()> {
    let data = spawn_blocking(move || {
        let mut data = vec![];
        ciborium::into_writer(&state.lock().unwrap().archive, &mut data)?;
        Result::Ok(data)
    })
    .await??;

    let timestamp = time.format("%Y-%m-%dT%H:%M:%S").to_string();
    let key = format!("archive:{}", timestamp);
    args.storage.put(&args.bucket, &key, data).await?;

    let latest_key = "archive:latest";
    args.storage
        .put(&args.bucket, latest_key, timestamp.into())
        .await?;
    Ok(())
}

async fn backup_recursive(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    sender: Sender<PendingFile>,
    path: &Path,
) -> Result<()> {
    let walker = WalkDir::new(path);
    for entry_result in walker {
        let entry = entry_result?;
        if entry.file_type().is_dir() && entry.depth() == 0 {
            continue;
        }

        backup_from_entry(args.clone(), state.clone(), sender.clone(), entry, path).await?;
    }

    Ok(())
}

async fn backup_from_entry(
    _args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    sender: Sender<PendingFile>,
    entry: DirEntry,
    base_path: &Path,
) -> Result<()> {
    let local_path = entry.path();
    let archive_path = local_path.strip_prefix(base_path)?;
    info!("backing up `{}`", archive_path.to_string_lossy());

    let file_type = entry.file_type();
    if file_type.is_file() {
        let pending_file = PendingFile {
            local_path: local_path.to_owned(),
            archive_path: archive_path.to_owned(),
        };
        sender.send(pending_file).await?;
    } else if file_type.is_symlink() {
        let metadata = read_metadata(local_path).await?;
        let path = fs::read_link(local_path).await?;
        let node = Node::Symlink { metadata, path };
        state.lock().unwrap().archive.insert(archive_path, node)?;
    } else if file_type.is_dir() {
        let metadata = read_metadata(local_path).await?;
        let children = BTreeMap::new();
        let node = Node::Directory { metadata, children };
        state.lock().unwrap().archive.insert(archive_path, node)?;
    } else {
        warn!("skipping special file `{}`", local_path.to_string_lossy());
    };

    Ok(())
}

async fn upload_pending_files(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    receiver: Receiver<PendingFile>,
) {
    let semaphore = Arc::new(Semaphore::new(args.max_concurrency));
    while let Ok(pending_file) = receiver.recv().await {
        let args = args.clone();
        let state = state.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        spawn(async move {
            upload_pending_file(args, state, pending_file)
                .await
                .unwrap();
            drop(permit);
        });
    }
}

async fn upload_pending_file(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    pending_file: PendingFile,
) -> Result<()> {
    info!("uploading `{}`", pending_file.archive_path.to_string_lossy());
    let metadata = read_metadata(&pending_file.local_path).await?;
    let mut file = File::open(&pending_file.local_path).await?;
    let (file_hash, block_hashes) = upload_blocks(args.clone(), &mut file).await?;
    upload_file(args.clone(), file_hash, block_hashes).await?;

    let node = Node::File {
        metadata,
        hash: file_hash,
    };

    state
        .lock()
        .unwrap()
        .archive
        .insert(&pending_file.archive_path, node)?;
    Ok(())
}

async fn upload_file(
    args: Arc<BackupArgs>,
    file_hash: FileHash,
    block_hashes: Vec<BlockHash>,
) -> Result<()> {
    let key = file_hash.key();
    if !args.storage.exists(&args.bucket, &key).await? {
        let chunk_size = args.target_block_size as usize;
        let chunk_hash_count = chunk_size / hash::SIZE;
        let chunks = block_hashes.chunks(chunk_hash_count).map(concat_hashes);
        args.storage
            .put_streaming(&args.bucket, &key, chunks)
            .await?;
    }

    Ok(())
}

async fn upload_blocks(
    args: Arc<BackupArgs>,
    file: &mut File,
) -> Result<(FileHash, Vec<BlockHash>)> {
    let mut chunker = block::chunker(file, args.target_block_size);
    let mut chunks = pin!(chunker.as_stream());

    let mut hasher = blake3::Hasher::new();
    let mut block_hashes = Vec::new();

    while let Some(chunk_result) = chunks.next().await {
        let chunk = chunk_result?;
        let block_hash = upload_block(args.clone(), &chunk.data).await?;
        hasher.update(block_hash.as_bytes());
        block_hashes.push(block_hash);
    }

    let file_hash = hasher.finalize().into();
    Ok((file_hash, block_hashes))
}

async fn upload_block(args: Arc<BackupArgs>, data: &[u8]) -> Result<BlockHash> {
    let block_hash = block::hash(data).await?;
    let key = block_hash.key();

    if !args.storage.exists(&args.bucket, &key).await? {
        let compressed_data = block::compress(data, args.compression_level).await?;
        args.storage
            .put(&args.bucket, &key, compressed_data)
            .await?;
    }

    Ok(block_hash)
}

fn concat_hashes(hashes: &[BlockHash]) -> Vec<u8> {
    concat_arrays(hashes.iter().map(|hash| hash.as_bytes()).cloned())
}

fn concat_arrays<T: Clone, const N: usize, I>(arrays: I) -> Vec<T>
where
    I: ExactSizeIterator<Item = [T; N]>,
{
    let len = arrays.len() * N;
    let mut output = Vec::with_capacity(len);

    for array in arrays {
        output.extend_from_slice(&array);
    }

    output
}
