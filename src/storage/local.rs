use std::{io, path::PathBuf, time::Duration};

use async_trait::async_trait;
use rand_distr::{Distribution, LogNormal};
use tokio::{fs, time::sleep};

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

    async fn create_dir(&self) -> Result<()> {
        let result = fs::create_dir(&self.path).await;
        match result {
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            result => result,
        }?;

        Ok(())
    }

    async fn simulate_latency(&self) {
        if let Some(duration) = self.latency {
            let distribution = LogNormal::new(0.0, 0.5).unwrap();
            let multiplier = distribution.sample(&mut rand::thread_rng());
            let randomized_latency = duration.mul_f64(multiplier);
            sleep(randomized_latency).await;
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

        let mut read_dir = fs::read_dir(&self.path).await?;
        let mut keys = vec![];

        while let Some(entry) = read_dir.next_entry().await? {
            let name = entry.file_name();
            let key = name
                .to_str()
                .ok_or_else(|| Error::InvalidKey(name.to_string_lossy().into_owned()))?;
            if key.starts_with(prefix.unwrap_or("")) {
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

        self.create_dir().await?;
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
