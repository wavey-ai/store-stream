use anyhow::{anyhow, Result};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

pub mod resumable;

#[derive(Clone)]
pub struct Storage {
    client: Arc<Client>,
}

impl Storage {
    pub fn new(endpoint: String, key_id: String, secret_key: String) -> Self {
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
        }
    }

    async fn bucket_exists(&self, bucket_name: &str) -> Result<bool> {
        match self.client.head_bucket().bucket(bucket_name).send().await {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .raw_response()
                    .map(|response| response.status().as_u16())
                    == Some(404) =>
            {
                Ok(false)
            }
            Err(err) => Err(anyhow!(err)),
        }
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
        range_end: Option<usize>,
    ) -> Result<Bytes> {
        let requested_len = inclusive_range_len(range_start, range_end)?;
        let offsets_bytes = self
            .fetch_object(bucket_name, &format!("{}.dat", object_key))
            .await?;
        let offsets = deserialize_offsets(&offsets_bytes)?;

        let parts_to_fetch = part_indexes_for_range(&offsets, range_start, range_end)?;

        let mut result_bytes = BytesMut::new();
        for part_index in &parts_to_fetch {
            let part_key = format!("{}/{:010}", object_key, part_index);
            debug!("get object {}/{}", bucket_name, &part_key);
            let part_bytes = self.fetch_object(bucket_name, &part_key).await?;
            result_bytes.extend_from_slice(&part_bytes);
        }

        let result_bytes = result_bytes.freeze();

        let first_part_offset = usize::try_from(offsets[parts_to_fetch[0]])
            .map_err(|_| anyhow!("Object offset does not fit in usize"))?;
        if first_part_offset > range_start {
            return Err(anyhow!(
                "The requested range is before the first stored part."
            ));
        }
        let slice_start = range_start - first_part_offset;
        if slice_start > result_bytes.len()
            || (requested_len.is_some() && slice_start == result_bytes.len())
        {
            return Err(anyhow!(
                "The requested range starts beyond the object data."
            ));
        }
        let slice_end = if let Some(requested_len) = requested_len {
            slice_start
                .checked_add(requested_len)
                .ok_or_else(|| anyhow!("The requested range is too large."))?
        } else {
            result_bytes.len()
        };

        Ok(result_bytes.slice(slice_start..slice_end.min(result_bytes.len())))
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
        min_part_size: usize,
    ) -> Result<()> {
        if min_part_size == 0 {
            return Err(anyhow!("min_part_size must be greater than zero"));
        }

        let (tx_offset, mut rx_offset) = mpsc::channel::<u64>(16);
        let client = Arc::clone(&self.client);
        let bucket = bucket_name.to_string();
        let key = object_key.to_string();

        self.upsert_bucket(bucket_name).await?;

        let offset_writer = tokio::task::spawn(async move {
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

        let upload_result = async {
            while let Some(payload) = rx.recv().await {
                let mut remaining = payload.as_ref();
                while !remaining.is_empty() {
                    let take = (min_part_size - buffer.len()).min(remaining.len());
                    buffer.extend_from_slice(&remaining[..take]);
                    remaining = &remaining[take..];
                    if buffer.len() == min_part_size {
                        let client = Arc::clone(&self.client);
                        let bucket = bucket_name.to_string();
                        let key = object_key.to_string();
                        let part_data = buffer.split().freeze();
                        upload_part(client, bucket, key, part_data, pkt_num).await?;
                        tx_offset.send(offset).await?;
                        pkt_num += 1;
                        offset += min_part_size as u64;
                    }
                }
            }

            let client = Arc::clone(&self.client);
            let bucket = bucket_name.to_string();
            let key = object_key.to_string();
            let remaining = buffer.freeze();

            if !remaining.is_empty() || pkt_num == 0 {
                upload_part(client, bucket, key, remaining, pkt_num).await?;
                tx_offset.send(offset).await?;
            }

            Ok::<(), anyhow::Error>(())
        }
        .await;

        drop(tx_offset);

        let offset_result = offset_writer
            .await
            .map_err(|err| anyhow!("offset writer task failed: {err}"))?;

        upload_result?;
        offset_result?;

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
    let key_suffix = format!("{}/{:010}", key, pkt_num);
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

fn serialize_offsets(offsets: &[u64]) -> Vec<u8> {
    offsets
        .iter()
        .flat_map(|&offset| offset.to_be_bytes())
        .collect()
}

fn deserialize_offsets(bytes: &[u8]) -> Result<Vec<u64>> {
    if !bytes.len().is_multiple_of(8) {
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
    if offsets.first().copied().is_some_and(|first| first != 0)
        || offsets.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(anyhow!("Offsets must start at zero and increase"));
    }
    Ok(offsets)
}

fn inclusive_range_len(range_start: usize, range_end: Option<usize>) -> Result<Option<usize>> {
    range_end
        .map(|end| {
            end.checked_sub(range_start)
                .and_then(|len| len.checked_add(1))
                .ok_or_else(|| anyhow!("Invalid byte range"))
        })
        .transpose()
}

fn part_indexes_for_range(
    offsets: &[u64],
    range_start: usize,
    range_end: Option<usize>,
) -> Result<Vec<usize>> {
    let _ = inclusive_range_len(range_start, range_end)?;

    if offsets.is_empty() {
        return Err(anyhow!("No offsets found for object"));
    }

    let part_start_index = offsets
        .partition_point(|&offset| offset <= range_start as u64)
        .saturating_sub(1);

    let mut parts_to_fetch = Vec::new();
    for (i, offset) in offsets.iter().enumerate().skip(part_start_index) {
        if let Some(range_end) = range_end {
            if *offset > range_end as u64 {
                break;
            }
        }
        parts_to_fetch.push(i);
    }

    if parts_to_fetch.is_empty() {
        return Err(anyhow!("The requested range is not covered by any parts."));
    }

    Ok(parts_to_fetch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::env;
    use tokio::sync::mpsc;

    const TEST_ENDPOINT: &str = "http://localhost:9000";
    const TEST_BUCKET_NAME: &str = "test";
    const MIN_PART_SIZE: usize = 1024 * 10;

    fn get_env_var(key: &str) -> String {
        env::var(key).unwrap_or_else(|_| panic!("Environment variable {} not set", key))
    }

    fn create_storage() -> Storage {
        let key_id = get_env_var("TEST_KEY_ID");
        let secret_key = get_env_var("TEST_SECRET_KEY");
        Storage::new(TEST_ENDPOINT.to_string(), key_id, secret_key)
    }

    #[test]
    fn serializes_offsets_as_big_endian_u64s() {
        let bytes = serialize_offsets(&[0, 1, 258]);
        assert_eq!(bytes.len(), 24);
        assert_eq!(deserialize_offsets(&bytes).unwrap(), vec![0, 1, 258]);
    }

    #[test]
    fn deserialize_offsets_rejects_partial_u64() {
        let err = deserialize_offsets(&[0, 1, 2]).unwrap_err();
        assert!(err.to_string().contains("Invalid byte length"));
    }

    #[test]
    fn deserialize_offsets_rejects_invalid_order() {
        assert!(deserialize_offsets(&serialize_offsets(&[1, 10])).is_err());
        assert!(deserialize_offsets(&serialize_offsets(&[0, 10, 10])).is_err());
        assert!(deserialize_offsets(&serialize_offsets(&[0, 20, 10])).is_err());
    }

    #[test]
    fn inclusive_range_len_counts_end_byte() {
        assert_eq!(inclusive_range_len(0, Some(0)).unwrap(), Some(1));
        assert_eq!(inclusive_range_len(10, Some(12)).unwrap(), Some(3));
        assert_eq!(inclusive_range_len(10, None).unwrap(), None);
    }

    #[test]
    fn inclusive_range_len_rejects_inverted_range() {
        assert!(inclusive_range_len(12, Some(10)).is_err());
    }

    #[test]
    fn part_indexes_cover_cross_part_ranges() {
        let offsets = vec![0, 10, 20];
        assert_eq!(
            part_indexes_for_range(&offsets, 9, Some(10)).unwrap(),
            vec![0, 1]
        );
        assert_eq!(
            part_indexes_for_range(&offsets, 10, Some(10)).unwrap(),
            vec![1]
        );
        assert_eq!(part_indexes_for_range(&offsets, 21, None).unwrap(), vec![2]);
    }

    #[ignore = "requires local S3-compatible storage and TEST_KEY_ID/TEST_SECRET_KEY"]
    #[tokio::test]
    async fn test_bucket_creation() {
        let storage = create_storage();
        let result = storage.create_bucket(TEST_BUCKET_NAME).await;
        assert!(result.is_ok(), "Bucket creation failed: {:?}", result.err());
    }

    #[ignore = "requires local S3-compatible storage and TEST_KEY_ID/TEST_SECRET_KEY"]
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

    #[ignore = "requires local S3-compatible storage and TEST_KEY_ID/TEST_SECRET_KEY"]
    #[tokio::test]
    async fn test_upload_and_retrieve_object() {
        let storage = create_storage();
        let (tx, rx) = mpsc::channel(16);

        let upload_task = tokio::spawn(async move {
            let data = Bytes::from("Hello, S3!");
            tx.send(data).await.unwrap();
        });

        let upload_result = storage
            .upload(TEST_BUCKET_NAME, "test-object", rx, MIN_PART_SIZE)
            .await;
        assert!(
            upload_result.is_ok(),
            "Object upload failed: {:?}",
            upload_result.err()
        );

        upload_task.await.unwrap();

        let fetched_data = storage
            .fetch_object(TEST_BUCKET_NAME, "test-object/0000000000")
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

    #[ignore = "requires local S3-compatible storage and TEST_KEY_ID/TEST_SECRET_KEY"]
    #[tokio::test]
    async fn test_large_input_is_saved_as_fixed_size_parts() {
        let storage = create_storage();
        let key = format!(
            "store-stream-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let payload = Bytes::from(vec![42; MIN_PART_SIZE * 2 + 31]);
        let (tx, rx) = mpsc::channel(1);
        tx.send(payload.clone()).await.unwrap();
        drop(tx);

        storage
            .upload(TEST_BUCKET_NAME, &key, rx, MIN_PART_SIZE)
            .await
            .unwrap();
        let offsets = storage
            .fetch_object(TEST_BUCKET_NAME, &format!("{key}.dat"))
            .await
            .unwrap();
        assert_eq!(
            deserialize_offsets(&offsets).unwrap(),
            vec![0, MIN_PART_SIZE as u64, (MIN_PART_SIZE * 2) as u64]
        );
        assert_eq!(
            storage
                .get_byte_range(TEST_BUCKET_NAME, &key, 0, None)
                .await
                .unwrap(),
            payload
        );
        for object in (0..3)
            .map(|part| format!("{key}/{part:010}"))
            .chain(std::iter::once(format!("{key}.dat")))
        {
            storage
                .client
                .delete_object()
                .bucket(TEST_BUCKET_NAME)
                .key(object)
                .send()
                .await
                .unwrap();
        }
    }

    #[ignore = "requires local S3-compatible storage and TEST_KEY_ID/TEST_SECRET_KEY"]
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
