use std::{
    mem::size_of_val,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use async_walkdir::WalkDir;
use rand_distr::{Distribution, LogNormal};
use tokio::{fs, time::sleep};
use tokio_stream::StreamExt;

use crate::error::{Error, Result};

use super::{Storage, StorageStats};

#[derive(Debug)]
pub struct LocalStorage {
    path: PathBuf,
    latency: Option<Duration>,
    stats: Arc<Mutex<StorageStats>>,
}

impl LocalStorage {
    pub fn new(path: PathBuf, latency: Option<Duration>) -> Self {
        LocalStorage {
            path,
            latency,
            stats: Arc::new(Mutex::new(StorageStats::new())),
        }
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.path.join(key)
    }

    async fn create_parent_dirs(&self, path: &Path) -> Result<()> {
        let parent = path.parent().unwrap();
        fs::create_dir_all(parent).await?;
        Ok(())
    }

    async fn simulate_latency(&self) {
        let latency = self.latency.unwrap_or_default();
        if !latency.is_zero() {
            let distribution = LogNormal::new(0.0, 0.5).unwrap();
            let multiplier = distribution.sample(&mut rand::thread_rng());
            let randomized_latency = latency.mul_f64(multiplier);
            sleep(randomized_latency).await;
        }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn exists(&self, key: &str) -> Result<bool> {
        let start_time = Instant::now();
        self.simulate_latency().await;

        let path = self.object_path(key);
        let exists = fs::try_exists(path).await?;

        let end_time = Instant::now();
        self.stats.lock().unwrap().add_get(start_time, end_time, 0);
        Ok(exists)
    }

    async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let start_time = Instant::now();
        self.simulate_latency().await;

        let prefix_path = self.path.join(prefix.unwrap_or(""));
        let (dirname, filename_prefix_path) = dirname_and_filename(&prefix_path)?;
        let filename_prefix = filename_prefix_path
            .to_str()
            .ok_or_else(|| invalid_key(&prefix_path))?;

        let mut walker = WalkDir::new(dirname);
        let mut keys = vec![];
        let mut size = 0;

        while let Some(entry) = walker.try_next().await? {
            if entry.file_type().await?.is_dir() {
                continue;
            }

            let raw_size = size_of_val(&entry);
            size += u32::try_from(raw_size).unwrap();

            let absolute_path = entry.path();
            let path = absolute_path.strip_prefix(&self.path)?;
            let filename = path
                .file_name()
                .unwrap()
                .to_str()
                .ok_or_else(|| invalid_key(path))?;

            if filename.starts_with(filename_prefix) {
                let key = path.to_str().ok_or_else(|| invalid_key(path))?;
                keys.push(key.to_owned());
            }
        }

        keys.sort();

        let end_time = Instant::now();
        self.stats
            .lock()
            .unwrap()
            .add_get(start_time, end_time, size);
        Ok(keys)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let start_time = Instant::now();
        self.simulate_latency().await;

        let path = self.object_path(key);
        let bytes = fs::read(path).await.map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Error::ItemNotFound(key.to_owned())
            } else {
                err.into()
            }
        })?;

        let end_time = Instant::now();
        let size = u32::try_from(bytes.len()).unwrap();
        self.stats
            .lock()
            .unwrap()
            .add_get(start_time, end_time, size);
        Ok(bytes)
    }

    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let start_time = Instant::now();
        self.simulate_latency().await;

        let path = self.object_path(key);
        let size = u32::try_from(bytes.len()).unwrap();
        self.create_parent_dirs(&path).await?;
        fs::write(path, bytes).await?;

        let end_time = Instant::now();
        self.stats
            .lock()
            .unwrap()
            .add_put(start_time, end_time, size);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let start_time = Instant::now();
        self.simulate_latency().await;

        let path = self.object_path(key);
        fs::remove_file(path).await?;

        let end_time = Instant::now();
        self.stats.lock().unwrap().add_delete(start_time, end_time);
        Ok(())
    }

    async fn delete_many(&self, keys: Vec<String>) -> Result<()> {
        let start_time = Instant::now();
        self.simulate_latency().await;

        for key in keys {
            self.delete(&key).await?;
        }

        let end_time = Instant::now();
        self.stats.lock().unwrap().add_delete(start_time, end_time);
        Ok(())
    }

    fn stats(&self) -> MutexGuard<StorageStats> {
        self.stats.lock().unwrap()
    }
}

fn dirname_and_filename(path: &Path) -> Result<(&Path, &Path)> {
    if path.to_str().is_some_and(|s| s.ends_with('/')) {
        let dirname = path;
        let filename = Path::new("");
        Ok((dirname, filename))
    } else {
        let dirname = path.parent().unwrap();
        let filename = path.strip_prefix(dirname)?;
        Ok((dirname, filename))
    }
}

fn invalid_key(path: &Path) -> Error {
    Error::InvalidKey(path.to_string_lossy().into_owned())
}
