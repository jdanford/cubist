use aws_sdk_s3::{
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};

use crate::error::Result;

use super::core::Storage;

pub struct CloudStorage {
    client: aws_sdk_s3::Client,
}

impl CloudStorage {
    pub async fn from_env() -> CloudStorage {
        let s3_config = aws_config::load_from_env().await;
        let client = Client::new(&s3_config);
        CloudStorage { client }
    }
}

impl Storage for CloudStorage {
    async fn exists(&self, bucket: &str, key: &str) -> Result<bool> {
        let head_result = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        Ok(head_result.is_ok())
    }

    async fn get(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let object = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        let data = object.body.collect().await?.to_vec();
        Ok(data)
    }

    async fn put(&self, bucket: &str, key: &str, data: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(data.into())
            .send()
            .await?;
        Ok(())
    }

    async fn put_streaming<I>(&self, bucket: &str, key: &str, chunks: I) -> Result<()>
    where
        I: Iterator<Item = Vec<u8>>,
    {
        let multipart_upload_res = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let upload_id = multipart_upload_res.upload_id().unwrap();
        let mut parts = Vec::new();

        for (index, chunk) in chunks.into_iter().enumerate() {
            let part_number = i32::try_from(index).unwrap() + 1;
            let upload_part_res = self
                .client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .body(chunk.into())
                .part_number(part_number)
                .send()
                .await?;

            let part = CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build();
            parts.push(part);
        }

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(())
    }
}
