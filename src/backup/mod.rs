mod upload;

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
    block,
    error::Result,
    file::{read_metadata, Archive, Node},
    hash::{self, Hash},
    storage::BoxedStorage,
};

pub use self::upload::UploadTree;

pub struct BackupArgs {
    pub storage: BoxedStorage,
    pub compression_level: u32,
    pub target_block_size: u32,
    pub max_concurrency: usize,
    pub bucket: String,
    pub paths: Vec<PathBuf>,
}

struct BackupState {
    archive: Archive,
}

struct PendingFile {
    local_path: PathBuf,
    archive_path: PathBuf,
}

pub async fn backup(
    storage: BoxedStorage,
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
        upload_pending_files(uploader_args, uploader_state, receiver).await;
    });

    for path in &args.paths {
        backup_recursive(args.clone(), state.clone(), sender.clone(), path).await?;
    }

    uploader_task.await?;
    upload_archive(args, state, time).await?;
    Ok(())
}

async fn upload_archive(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    time: DateTime<Utc>,
) -> Result<()> {
    let data = spawn_blocking(move || {
        let data = bincode::serialize(&state.lock().unwrap().archive)?;
        Result::Ok(data)
    })
    .await??;

    let timestamp = time.format("%Y-%m-%dT%H:%M:%S").to_string();
    let key = format!("archive:{timestamp}");
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
    info!(
        "uploading `{}`",
        pending_file.archive_path.to_string_lossy()
    );
    let metadata = read_metadata(&pending_file.local_path).await?;
    let mut file = File::open(&pending_file.local_path).await?;
    let hash = upload_file(args.clone(), &mut file).await?;
    let node = Node::File { metadata, hash };

    state
        .lock()
        .unwrap()
        .archive
        .insert(&pending_file.archive_path, node)?;
    Ok(())
}

async fn upload_file(args: Arc<BackupArgs>, file: &mut File) -> Result<Hash> {
    let mut chunker = block::chunker(file, args.target_block_size);
    let mut chunks = pin!(chunker.as_stream());

    let max_layer_size = args.target_block_size as usize / hash::SIZE;
    let mut tree = UploadTree::new(args, max_layer_size);

    while let Some(chunk_result) = chunks.next().await {
        let chunk = chunk_result?;
        tree.add(&chunk.data).await?;
    }

    let hash = tree.finalize().await?;
    Ok(hash)
}
