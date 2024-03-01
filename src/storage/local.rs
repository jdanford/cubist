use std::{io, path::PathBuf, time::Duration};

use async_trait::async_trait;
use rand_distr::{Distribution, LogNormal};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    time::sleep,
};

use crate::error::Result;

use super::core::Storage;

pub struct LocalStorage {
    path: PathBuf,
    latency: Duration,
}

impl LocalStorage {
    pub fn new<P: ToOwned<Owned = PathBuf>>(path: P, latency: Duration) -> Self {
        LocalStorage {
            path: path.to_owned(),
            latency,
        }
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.path.join(bucket)
    }

    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.bucket_path(bucket).join(key)
    }

    async fn create_bucket_dir(&self, bucket: &str) -> Result<()> {
        let path = self.bucket_path(bucket);
        match fs::create_dir(path).await {
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            result => result,
        }?;
        Ok(())
    }

    async fn simulate_latency(&self) {
        let log_normal = LogNormal::new(0.0, 0.5).unwrap();
        let multiplier = log_normal.sample(&mut rand::thread_rng());
        let latency = self.latency.mul_f64(multiplier);
        sleep(latency).await;
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn exists(&self, bucket: &str, key: &str) -> Result<bool> {
        self.simulate_latency().await;

        let path = self.object_path(bucket, key);
        let exists = fs::try_exists(path).await?;
        Ok(exists)
    }

    async fn get(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        self.simulate_latency().await;

        let path = self.object_path(bucket, key);
        let data = fs::read(path).await?;
        Ok(data)
    }

    async fn put(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
        self.simulate_latency().await;

        let path = self.object_path(bucket, key);
        self.create_bucket_dir(bucket).await?;
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .await?;
        file.write_all(&data).await?;

        Ok(())
    }
}
