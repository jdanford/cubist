use std::{
    borrow::Borrow,
    mem::size_of_val,
    path::{Path, PathBuf},
    time::Duration,
};

use async_trait::async_trait;
use async_walkdir::{Filtering, WalkDir};
use rand_distr::{Distribution, LogNormal};
use tokio::{fs, time::sleep};
use tokio_stream::StreamExt;

use crate::{
    error::{Error, Result},
    stats::StorageStats,
};

use super::Storage;

#[derive(Debug)]
pub struct LocalStorage {
    path: PathBuf,
    latency: Option<Duration>,
    stats: StorageStats,
}

impl LocalStorage {
    pub fn new(path: PathBuf, latency: Option<Duration>) -> Self {
        LocalStorage {
            path,
            latency,
            stats: StorageStats::new(),
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
        if let Some(latency) = self.latency {
            let distribution = LogNormal::new(0.0, 0.5).unwrap();
            let multiplier = distribution.sample(&mut rand::thread_rng());
            let randomized_latency = latency.mul_f64(multiplier);
            if !randomized_latency.is_zero() {
                sleep(randomized_latency).await;
            }
        }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn exists(&mut self, key: &str) -> Result<bool> {
        self.simulate_latency().await;

        let path = self.object_path(key);
        let exists = fs::try_exists(path).await?;

        self.stats.get_requests += 1;
        Ok(exists)
    }

    async fn keys(&mut self, prefix: Option<&str>) -> Result<Vec<String>> {
        self.simulate_latency().await;

        let prefix_path = self.path.join(prefix.unwrap_or(""));
        let (dirname, filename_prefix) = if prefix_path.to_str().is_some_and(|s| s.ends_with('/')) {
            (prefix_path.borrow(), "")
        } else {
            let dirname = prefix_path.parent().unwrap();
            let filename_prefix = prefix_path
                .strip_prefix(dirname)?
                .to_str()
                .ok_or_else(|| invalid_key_error(&prefix_path))?;
            (dirname, filename_prefix)
        };

        let mut keys = vec![];
        let mut walker = WalkDir::new(dirname).filter(|entry| async move {
            if let Ok(file_type) = entry.file_type().await {
                if file_type.is_file() {
                    return Filtering::Continue;
                }
            }

            Filtering::Ignore
        });

        while let Some(entry) = walker.try_next().await? {
            self.stats.bytes_downloaded += size_of_val(&entry) as u64;

            let absolute_path = entry.path();
            let path = absolute_path.strip_prefix(&self.path)?;
            let full_key = path.to_str().ok_or_else(|| invalid_key_error(path))?;
            if let Some(key) = full_key.strip_prefix(filename_prefix) {
                keys.push(key.to_owned());
            }
        }

        self.stats.get_requests += 1;
        Ok(keys)
    }

    async fn get(&mut self, key: &str) -> Result<Vec<u8>> {
        self.simulate_latency().await;

        let path = self.object_path(key);
        let bytes = fs::read(path).await.map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Error::ItemNotFound(key.to_owned())
            } else {
                err.into()
            }
        })?;

        self.stats.bytes_downloaded += bytes.len() as u64;
        self.stats.get_requests += 1;
        Ok(bytes)
    }

    async fn put(&mut self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.simulate_latency().await;

        let path = self.object_path(key);
        let size = bytes.len() as u64;

        self.create_parent_dirs(&path).await?;
        fs::write(path, bytes).await?;

        self.stats.bytes_uploaded += size;
        self.stats.put_requests += 1;
        Ok(())
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        let path = self.object_path(key);
        fs::remove_file(path).await?;
        Ok(())
    }

    async fn delete_many(&mut self, keys: Vec<String>) -> Result<()> {
        for key in keys {
            self.delete(&key).await?;
        }

        Ok(())
    }

    fn stats(&self) -> &StorageStats {
        &self.stats
    }
}

fn invalid_key_error(path: &Path) -> Error {
    Error::InvalidKey(path.to_string_lossy().into_owned())
}
