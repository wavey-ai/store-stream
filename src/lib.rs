use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Error as S3Error;
use aws_sdk_s3::{config::Region, Client};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::oneshot;

const PART_SIZE: usize = 5 * 1024 * 1024;

pub struct Storage {
    client: Arc<Client>,
}

impl Storage {
    pub fn new(endpoint: String, key_id: String, secret_key: String) -> Self {
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
        }
    }

    pub async fn upload(
        &self,
        bucket_name: &str,
        object_key: &str,
        mut rx: Receiver<Bytes>,
    ) -> oneshot::Sender<()> {
        let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
        let client = Arc::clone(&self.client);

        let bucket = bucket_name.to_string();
        let key = object_key.to_string();

        tokio::spawn(async move {
            let create_resp = client
                .create_multipart_upload()
                .bucket(bucket.to_string())
                .key(key.to_string())
                .send()
                .await
                .unwrap();
            let upload_id = create_resp.upload_id.expect("Expected upload ID");

            let mut buffer = Bytes::new();
            let mut part_number = 1;

            loop {
                tokio::select! {
                    Ok(data) = rx.recv() => {
                        buffer = Bytes::from([buffer.as_ref(), data.as_ref()].concat());
                        while buffer.len() >= PART_SIZE {
                            let part_data = buffer.split_to(PART_SIZE);
                            let byte_stream = ByteStream::from(part_data);
                            client.upload_part()
                                .bucket(bucket.to_string())
                                .key(key.to_string())
                                .upload_id(&upload_id)
                                .part_number(part_number)
                                .body(byte_stream)
                                .send()
                                .await;
                            part_number += 1;
                        }
                    }
                    _ = &mut cancel_rx => {
                        break;
                    }
                }
            }

            if !buffer.is_empty() {
                let byte_stream = ByteStream::from(buffer);
                client
                    .upload_part()
                    .bucket(bucket.to_string())
                    .key(key.to_string())
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(byte_stream)
                    .send()
                    .await;
            }

            client
                .complete_multipart_upload()
                .bucket(bucket.to_string())
                .key(key.to_string())
                .upload_id(upload_id)
                .send()
                .await;
        });

        cancel_tx
    }
}
