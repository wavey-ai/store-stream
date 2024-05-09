use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{config::Region, Client};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;

pub struct Storage {
    client: Arc<Client>,
    min_part_size: usize,
}

impl Storage {
    pub fn new(endpoint: String, key_id: String, secret_key: String, min_part_size: usize) -> Self {
        let creds = Credentials::new(
            key_id.clone(),
            secret_key.clone(),
            None,
            None,
            "StaticCredentials",
        );

        let s3_config = aws_sdk_s3::config::Builder::new()
            .endpoint_url(endpoint)
            .credentials_provider(creds)
            .region(Region::new("eu-central-1"))
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        Self {
            client: Arc::new(client),
            min_part_size,
        }
    }

    pub async fn upload(
        &self,
        bucket_name: &str,
        object_key: &str,
        mut rx: Receiver<Bytes>,
    ) -> Result<(), PutObjectError> {
        let mut buffer = BytesMut::new();
        let mut pkt_num = 0;

        while let Ok(payload) = rx.recv().await {
            buffer.extend_from_slice(&payload);
            if buffer.len() >= self.min_part_size {
                let client = Arc::clone(&self.client);
                let bucket = bucket_name.to_string();
                let key = object_key.to_string();

                let part_data = buffer.split_to(buffer.len()).freeze();
                tokio::task::spawn(async move {
                    upload_part(client, bucket, key, part_data, pkt_num).await;
                });
                pkt_num += 1;
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
    ) -> Result<(), PutObjectError> {
        if !buffer.is_empty() {
            upload_part(client, bucket, key, buffer, pkt_num).await;
        }
        Ok(())
    }
}

async fn upload_part(
    client: Arc<Client>,
    bucket: String,
    key: String,
    buffer: Bytes,
    pkt_num: usize,
) {
    let key_suffix = format!("{}/{}.ts", key, pkt_num);
    let byte_stream = ByteStream::from(buffer);
    match client
        .put_object()
        .bucket(bucket.to_string())
        .key(key_suffix)
        .body(byte_stream)
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) => {
            dbg!(e);
        }
    };
}
