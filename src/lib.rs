use anyhow::{anyhow, Result};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

#[derive(Clone)]
pub struct Storage {
    client: Arc<Client>,
    min_part_size: usize,
}

impl Storage {
    pub fn new(endpoint: String, key_id: String, secret_key: String, min_part_size: usize) -> Self {
        let creds = Credentials::new(key_id, secret_key, None, None, "StaticCredentials");
        let s3_config = aws_sdk_s3::config::Builder::new()
            .endpoint_url(endpoint)
            .credentials_provider(creds)
            .region(Region::new("eu-west-2"))
            .force_path_style(true)
            .build();
        let client = Client::from_conf(s3_config);

        Self {
            client: Arc::new(client),
            min_part_size,
        }
    }

    async fn bucket_exists(&self, bucket_name: &str) -> Result<bool> {
        self.client
            .head_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .map(|_| true)
            .map_err(|e| anyhow!(e))
    }

    async fn create_bucket(&self, bucket_name: &str) -> Result<()> {
        self.client
            .create_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| anyhow!(e))
    }

    async fn upsert_bucket(&self, bucket_name: &str) -> Result<()> {
        if !self.bucket_exists(bucket_name).await? {
            self.create_bucket(bucket_name).await
        } else {
            Ok(())
        }
    }

    pub async fn get_byte_range(
        &self,
        bucket_name: &str,
        object_key: &str,
        range_start: usize,
        range_end: usize,
    ) -> Result<Bytes> {
        let offsets_bytes = self.fetch_object(bucket_name, object_key).await?;
        let offsets = deserialize_offsets(&offsets_bytes)?;

        if offsets.is_empty() {
            return Err(anyhow!("No offsets found for object"));
        }

        let mut parts_to_fetch = Vec::new();
        let mut part_start_index = 0;

        while part_start_index < offsets.len() && offsets[part_start_index] < range_start as u64 {
            part_start_index += 1;
        }
        part_start_index = part_start_index.saturating_sub(1);

        if offsets[part_start_index] > range_start as u64 && part_start_index > 0 {
            part_start_index -= 1;
        }

        for (i, offset) in offsets.iter().enumerate().skip(part_start_index) {
            if *offset > range_end as u64 {
                break;
            }
            parts_to_fetch.push(i);
        }

        if parts_to_fetch.is_empty() {
            return Err(anyhow!("The requested range is not covered by any parts."));
        }

        let mut result_bytes = BytesMut::new();
        for part_index in &parts_to_fetch {
            let part_key = format!("{}/{}", object_key, part_index);
            let part_bytes = self.fetch_object(bucket_name, &part_key).await?;
            result_bytes.extend_from_slice(&part_bytes);
        }

        let result_bytes = result_bytes.freeze();

        let first_part_offset = offsets[parts_to_fetch[0]] as usize;
        let slice_start = (range_start - first_part_offset).max(0);
        let slice_end = slice_start + (range_end - range_start);

        let required_bytes = result_bytes.slice(slice_start..slice_end);

        Ok(required_bytes)
    }

    async fn fetch_object(&self, bucket_name: &str, object_key: &str) -> Result<Bytes> {
        let byte_stream = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(object_key)
            .send()
            .await?
            .body
            .collect()
            .await?;
        Ok(byte_stream.into_bytes())
    }

    pub async fn upload(
        &self,
        bucket_name: &str,
        object_key: &str,
        mut rx: mpsc::Receiver<Bytes>,
    ) -> Result<()> {
        let (tx_offset, mut rx_offset) = mpsc::channel::<u64>(16);
        let client = Arc::clone(&self.client);
        let bucket = bucket_name.to_string();
        let key = object_key.to_string();

        self.upsert_bucket(bucket_name).await?;

        tokio::task::spawn(async move {
            let mut offsets = Vec::new();
            while let Some(n) = rx_offset.recv().await {
                offsets.push(n);
                let serialized_offsets = serialize_offsets(&offsets);
                let bytes = Bytes::from(serialized_offsets);
                put(
                    client.clone(),
                    bucket.to_string(),
                    format!("{}.dat", key),
                    bytes,
                )
                .await?;
            }
            Ok::<(), anyhow::Error>(())
        });

        let mut buffer = BytesMut::new();
        let mut pkt_num = 0;
        let mut offset = 0;
        while let Some(payload) = rx.recv().await {
            buffer.extend_from_slice(&payload);
            if buffer.len() >= self.min_part_size {
                let client = Arc::clone(&self.client);
                let bucket = bucket_name.to_string();
                let key = object_key.to_string();
                let len = buffer.len();
                let part_data = buffer.split_to(len).freeze();
                upload_part(client, bucket, key, part_data, pkt_num).await?;
                pkt_num += 1;
                tx_offset.send(offset).await?;
                offset += len as u64;
            }
        }

        let client = Arc::clone(&self.client);
        let bucket = bucket_name.to_string();
        let key = object_key.to_string();

        self.flush_remaining(
            Arc::clone(&client),
            bucket.to_string(),
            key.to_string(),
            buffer.freeze(),
            pkt_num,
        )
        .await?;

        Ok(())
    }

    async fn flush_remaining(
        &self,
        client: Arc<Client>,
        bucket: String,
        key: String,
        buffer: Bytes,
        pkt_num: usize,
    ) -> Result<()> {
        if !buffer.is_empty() {
            upload_part(client, bucket, key, buffer, pkt_num).await?;
        }
        Ok(())
    }

    pub async fn list_bucket(&self, bucket_name: &str) -> Result<ListBucketResult> {
        let mut objects = Vec::new();

        let mut response = self
            .client
            .list_objects_v2()
            .bucket(bucket_name.to_owned())
            .max_keys(10)
            .into_paginator()
            .send();

        while let Some(result) = response.next().await {
            match result {
                Ok(output) => {
                    objects.extend(output.contents().iter().map(|object| S3Object {
                        key: object.key().unwrap_or_default().to_string(),
                        size: object.size().unwrap_or_default(),
                    }));
                }
                Err(err) => return Err(anyhow!(err)),
            }
        }

        Ok(ListBucketResult { objects })
    }
}

#[derive(Debug)]
pub struct ListBucketResult {
    pub objects: Vec<S3Object>,
}

#[derive(Debug)]
pub struct S3Object {
    pub key: String,
    pub size: i64,
}

async fn put(
    client: Arc<Client>,
    bucket_name: String,
    object_key: String,
    body: Bytes,
) -> Result<()> {
    let bucket = bucket_name.to_string();
    let key = object_key.to_string();
    let byte_stream = ByteStream::from(body);
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(byte_stream)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| anyhow!(e))
}

async fn upload_part(
    client: Arc<Client>,
    bucket: String,
    key: String,
    buffer: Bytes,
    pkt_num: usize,
) -> Result<()> {
    let key_suffix = format!("{}/{}", key, pkt_num);
    let byte_stream = ByteStream::from(buffer);
    client
        .put_object()
        .bucket(bucket)
        .key(key_suffix)
        .body(byte_stream)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| anyhow!(e))
}

fn serialize_offsets(offsets: &Vec<u64>) -> Vec<u8> {
    offsets
        .iter()
        .map(|&offset| offset.to_be_bytes())
        .flatten()
        .collect()
}

fn deserialize_offsets(bytes: &[u8]) -> Result<Vec<u64>> {
    if bytes.len() % 8 != 0 {
        return Err(anyhow!("Invalid byte length for offsets"));
    }
    let mut offsets = Vec::with_capacity(bytes.len() / 8);
    for chunk in bytes.chunks_exact(8) {
        let offset = u64::from_be_bytes(
            chunk
                .try_into()
                .map_err(|_| anyhow!("Failed to convert bytes to u64"))?,
        );
        offsets.push(offset);
    }
    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::env;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    const TEST_ENDPOINT: &str = "https://wavey-test.fr-par-1.linodeobjects.com";
    const TEST_BUCKET_NAME: &str = "wavey-test";
    const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB

    fn get_env_var(key: &str) -> String {
        env::var(key).expect(&format!("Environment variable {} not set", key))
    }

    fn create_storage() -> Storage {
        let key_id = get_env_var("TEST_KEY_ID");
        let secret_key = get_env_var("TEST_SECRET_KEY");
        Storage::new(TEST_ENDPOINT.to_string(), key_id, secret_key, MIN_PART_SIZE)
    }

    #[tokio::test]
    async fn test_bucket_creation() {
        let storage = create_storage();
        let result = storage.create_bucket(TEST_BUCKET_NAME).await;
        assert!(result.is_ok(), "Bucket creation failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_bucket_existence() {
        let storage = create_storage();
        let result = storage.bucket_exists(TEST_BUCKET_NAME).await;
        assert!(
            result.is_ok(),
            "Checking bucket existence failed: {:?}",
            result.err()
        );
        assert!(result.unwrap(), "Bucket does not exist when it should.");
    }

    #[tokio::test]
    async fn test_upload_and_retrieve_object() {
        let storage = create_storage();
        let (tx, rx) = mpsc::channel(16);

        let upload_task = tokio::spawn(async move {
            let data = Bytes::from("Hello, S3!");
            tx.send(data).await.unwrap();
        });

        let upload_result = storage.upload(TEST_BUCKET_NAME, "test-object", rx).await;
        assert!(
            upload_result.is_ok(),
            "Object upload failed: {:?}",
            upload_result.err()
        );

        upload_task.await.unwrap();

        let fetched_data = storage
            .fetch_object(TEST_BUCKET_NAME, "test-object/0")
            .await;
        assert!(
            fetched_data.is_ok(),
            "Object fetch failed: {:?}",
            fetched_data.err()
        );

        let fetched_bytes = fetched_data.unwrap();
        assert_eq!(
            fetched_bytes,
            Bytes::from("Hello, S3!"),
            "Fetched data does not match uploaded data."
        );
    }

    #[tokio::test]
    async fn test_list_bucket() {
        let storage = create_storage();
        let result = storage.list_bucket(TEST_BUCKET_NAME).await;
        assert!(result.is_ok(), "Listing bucket failed: {:?}", result.err());

        let list_result = result.unwrap();
        assert!(
            !list_result.objects.is_empty(),
            "Bucket is empty when it should have objects."
        );
    }
}
