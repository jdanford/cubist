use std::{io, path::PathBuf, time::Duration};

use async_trait::async_trait;
use rand_distr::{Distribution, LogNormal};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    time::sleep,
};

use crate::error::{Error, Result};

use super::core::Storage;

pub struct LocalStorage {
    path: PathBuf,
    latency: Option<Duration>,
}

impl LocalStorage {
    pub fn new(path: PathBuf, latency: Option<Duration>) -> Self {
        LocalStorage { path, latency }
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.path.join(key)
    }

    async fn create_dir(&self) -> Result<()> {
        match fs::create_dir(&self.path).await {
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
    async fn exists(&self, key: &str) -> Result<bool> {
        self.simulate_latency().await;

        let path = self.object_path(key);
        let exists = fs::try_exists(path).await?;
        Ok(exists)
    }

    async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>> {
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

        Ok(keys)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.simulate_latency().await;

        let path = self.object_path(key);
        let bytes = fs::read(path).await?;
        Ok(bytes)
    }

    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.simulate_latency().await;

        let path = self.object_path(key);
        self.create_dir().await?;

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .await?;
        file.write_all(&bytes).await?;
        Ok(())
    }
}
