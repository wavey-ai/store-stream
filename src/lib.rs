use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Error as S3Error;
use aws_sdk_s3::{config::Region, Client};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;

const PART_SIZE: usize = 5 * 1024 * 1024;

pub struct Storage {
    client: Arc<Client>,
}

impl Storage {
    pub async fn new(key_id: String, secret_key: String) -> Self {
        let creds = Credentials::new(
            key_id.clone(),
            secret_key.clone(),
            None,
            None,
            "StaticCredentials",
        );

        let conf = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .region(Region::new("us-east-1"))
            .build();
        let client = Client::new(&conf);
        Self {
            client: Arc::new(client),
        }
    }

    pub async fn upload(
        &self,
        bucket: &str,
        key: &str,
        mut rx: Receiver<Bytes>,
    ) -> Result<(), S3Error> {
        let client = &self.client;

        let create_resp = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;

        let upload_id = create_resp.upload_id.expect("Expected upload ID");

        let mut buffer = Bytes::new();
        let mut part_number = 1;

        while let Ok(data) = rx.recv().await {
            buffer = Bytes::from([buffer.as_ref(), data.as_ref()].concat());

            while buffer.len() >= PART_SIZE {
                let part_data = buffer.split_to(PART_SIZE);
                let byte_stream = ByteStream::from(part_data);

                let _ = client
                    .upload_part()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(byte_stream)
                    .send()
                    .await?;
                part_number += 1;
            }
        }

        if !buffer.is_empty() {
            let byte_stream = ByteStream::from(buffer);
            let _ = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(byte_stream)
                .send()
                .await?;
        }

        let _ = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(())
    }
}
