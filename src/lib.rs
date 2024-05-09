use au::AuPayload;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Error as S3Error;
use aws_sdk_s3::{config::Region, Client};
use bytes::BytesMut;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;

const PART_SIZE: usize = 1 * 1024 * 1024;

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
        mut rx: Receiver<AuPayload>,
    ) -> Result<(), S3Error> {
        let client = Arc::clone(&self.client);

        let bucket = bucket_name.to_string();
        let key = object_key.to_string();

        let mut buffer = BytesMut::new();

        let mut part_number = 0;
        let mut dts: Option<i64> = None;
        while let Ok(payload) = rx.recv().await {
            if dts.is_none() {
                dts = Some(payload.dts());
            }

            buffer.extend_from_slice(&payload.lp_to_nal_start_code());
            if buffer.len() >= PART_SIZE {
                let part_data = buffer.split_to(buffer.len()).freeze();
                let bucket = bucket.clone();
                let key = key.clone();
                let client = Arc::clone(&client);
                tokio::task::spawn(async move {
                    let byte_stream = ByteStream::from(part_data);
                    let mut req = client
                        .put_object()
                        .bucket(bucket.to_string())
                        .key(format!("{}/{}", key, part_number))
                        .body(byte_stream);
                    if let Some(ref v) = dts {
                        req = req.metadata("dts", format!("{}", v));
                    }
                    match req.send().await {
                        Ok(_) => {}
                        Err(e) => {
                            dbg!(e);
                        }
                    }
                });

                dts = None;
                part_number += 1;
            }
        }

        if !buffer.is_empty() {
            let part_data = buffer.split_to(buffer.len()).freeze();
            let byte_stream = ByteStream::from(part_data);
            let mut req = client
                .put_object()
                .bucket(bucket.to_string())
                .key(format!("{}/{}", key, part_number))
                .body(byte_stream);
            if let Some(ref v) = dts {
                req = req.metadata("dts", format!("{}", v));
            }
            match req.send().await {
                Ok(_) => {}
                Err(e) => {
                    dbg!(e);
                }
            }
        }

        Ok(())
    }
}
