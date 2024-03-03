use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
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
use walkdir::{DirEntry, WalkDir};

use crate::{
    backup::blocks::upload_file,
    error::Result,
    file::{read_metadata, Node},
    hash,
    serde::serialize,
    storage::{self, archive_key},
};

use super::{BackupArgs, BackupState};

pub struct PendingUpload {
    local_path: PathBuf,
    archive_path: PathBuf,
}

pub async fn upload_archive(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    time: DateTime<Utc>,
) -> Result<()> {
    let timestamp = time.format("%Y%m%d%H%M%S").to_string();
    let key = archive_key(&timestamp);
    let data = spawn_blocking(move || serialize(&state.lock().unwrap().archive)).await?;
    args.storage.put(&key, data).await?;
    args.storage
        .put(storage::ARCHIVE_KEY_LATEST, timestamp.into())
        .await?;
    Ok(())
}

pub async fn backup_recursive(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    sender: Sender<PendingUpload>,
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
    sender: Sender<PendingUpload>,
    entry: DirEntry,
    base_path: &Path,
) -> Result<()> {
    let local_path = entry.path();
    let archive_path = local_path.strip_prefix(base_path)?;
    let file_type = entry.file_type();
    if file_type.is_file() {
        let pending_file = PendingUpload {
            local_path: local_path.to_owned(),
            archive_path: archive_path.to_owned(),
        };
        sender.send(pending_file).await?;
    } else if file_type.is_symlink() {
        let metadata = read_metadata(local_path).await?;
        let path = fs::read_link(local_path).await?;
        let node = Node::Symlink { metadata, path };
        let archive = &mut state.lock().unwrap().archive;
        archive.insert(archive_path, node)?;
    } else if file_type.is_dir() {
        let metadata = read_metadata(local_path).await?;
        let children = BTreeMap::new();
        let node = Node::Directory { metadata, children };
        let archive = &mut state.lock().unwrap().archive;
        archive.insert(archive_path, node)?;
    } else {
        warn!("skipping special file `{}`", local_path.display());
    };

    Ok(())
}

pub async fn upload_pending_files(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    receiver: Receiver<PendingUpload>,
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

    let permit_count = u32::try_from(args.max_concurrency).unwrap();
    let _ = semaphore.acquire_many(permit_count).await.unwrap();
}

async fn upload_pending_file(
    args: Arc<BackupArgs>,
    state: Arc<Mutex<BackupState>>,
    pending_file: PendingUpload,
) -> Result<()> {
    let metadata = read_metadata(&pending_file.local_path).await?;
    let mut file = File::open(&pending_file.local_path).await?;
    let hash = upload_file(args.clone(), &mut file).await?;
    let node = Node::File { metadata, hash };

    state
        .lock()
        .unwrap()
        .archive
        .insert(&pending_file.archive_path, node)?;

    let hash_str = hash::format(&hash);
    info!("{hash_str} <- {}", pending_file.local_path.display());
    Ok(())
}
