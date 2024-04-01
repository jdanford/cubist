use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use async_trait::async_trait;
use aws_sdk_s3::{
    error::SdkError,
    operation::{get_object::GetObjectError, head_object::HeadObjectError},
    types::{Delete, ObjectIdentifier},
    Client,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use tokio::task::spawn_blocking;

use crate::error::{Error, Result};

use super::{Storage, StorageStats};

const MAX_KEYS_PER_REQUEST: usize = 1000;

#[derive(Debug)]
pub struct S3Storage {
    client: Client,
    bucket: String,
    stats: Arc<Mutex<StorageStats>>,
}

impl S3Storage {
    pub async fn new(bucket: String) -> Self {
        let s3_config = aws_config::load_from_env().await;
        let client = Client::new(&s3_config);
        let stats = Arc::new(Mutex::new(StorageStats::new()));

        S3Storage {
            client,
            bucket,
            stats,
        }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn exists(&self, key: &str) -> Result<bool> {
        let start_time = Instant::now();
        let response_result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(SdkError::into_service_error);

        let exists = match response_result {
            Ok(_) => Ok(true),
            Err(HeadObjectError::NotFound(_)) => Ok(false),
            Err(err) => Err(Error::other(err)),
        }?;

        let end_time = Instant::now();
        self.stats.lock().unwrap().add_get(start_time, end_time, 0);
        Ok(exists)
    }

    async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let prefix_owned = prefix.map(ToOwned::to_owned);

        let start_time = Instant::now();
        let mut stream = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_prefix(prefix_owned)
            .into_paginator()
            .send();

        let mut keys = vec![];
        let mut size = 0;

        while let Some(page) = stream.try_next().await? {
            let contents = page.contents.unwrap_or(vec![]);
            for object in contents {
                if let Some(size_signed) = object.size {
                    size += u32::try_from(size_signed).unwrap();
                }

                let key = object.key.ok_or_else(|| Error::InvalidKey(String::new()))?;
                keys.push(key);
            }
        }

        let end_time = Instant::now();
        self.stats
            .lock()
            .unwrap()
            .add_get(start_time, end_time, size);
        Ok(keys)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let start_time = Instant::now();
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| match err.into_service_error() {
                GetObjectError::NoSuchKey(_) => Error::ItemNotFound(key.to_owned()),
                err => Error::other(err),
            })?;

        let bytes = response.body.collect().await?.to_vec();

        let end_time = Instant::now();
        let size = u32::try_from(bytes.len()).unwrap();
        self.stats
            .lock()
            .unwrap()
            .add_get(start_time, end_time, size);
        Ok(bytes)
    }

    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let size = u32::try_from(bytes.len()).unwrap();
        let (bytes, encoded_digest) = spawn_blocking(move || {
            let encoded_digest = md5_base64(&bytes);
            (bytes, encoded_digest)
        })
        .await?;

        let start_time = Instant::now();
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.into())
            .content_md5(encoded_digest)
            .send()
            .await?;

        let end_time = Instant::now();
        self.stats
            .lock()
            .unwrap()
            .add_put(start_time, end_time, size);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let start_time = Instant::now();
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let end_time = Instant::now();
        self.stats.lock().unwrap().add_delete(start_time, end_time);
        Ok(())
    }

    async fn delete_many(&self, keys: Vec<String>) -> Result<()> {
        for keys in keys.chunks(MAX_KEYS_PER_REQUEST) {
            let mut delete_builder = Delete::builder().quiet(true);
            for key in keys {
                let object = ObjectIdentifier::builder().key(key).build()?;
                delete_builder = delete_builder.objects(object);
            }

            let delete = delete_builder.build()?;

            let start_time = Instant::now();
            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await?;

            let end_time = Instant::now();
            self.stats.lock().unwrap().add_delete(start_time, end_time);
        }

        Ok(())
    }

    fn stats(&self) -> MutexGuard<StorageStats> {
        self.stats.lock().unwrap()
    }
}

fn md5_base64(bytes: &[u8]) -> String {
    let digest = md5::compute(bytes);
    BASE64_STANDARD.encode(digest.0)
}
