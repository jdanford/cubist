use std::{io, path::PathBuf, time::Duration};

use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    time::sleep,
};

use crate::error::Result;

use super::Storage;

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
        sleep(self.latency).await;
    }
}

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
        // println!("write {:?}", path);
        fs::write(path, &data).await?;
        Ok(())
    }

    async fn put_streaming<I>(&self, bucket: &str, key: &str, chunks: I) -> Result<()>
    where
        I: Iterator<Item = Vec<u8>>,
    {
        self.simulate_latency().await;

        let path = self.object_path(bucket, key);
        self.create_bucket_dir(bucket).await?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .await?;

        for chunk in chunks {
            self.simulate_latency().await;
            file.write_all(&chunk).await?;
        }

        Ok(())
    }
}
