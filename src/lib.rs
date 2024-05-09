use au::{AuKind, AuPayload};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{config::Region, Client};
use bytes::BytesMut;
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
        mut rx: Receiver<AuPayload>,
    ) -> Result<(), PutObjectError> {
        let client = Arc::clone(&self.client);
        let bucket = bucket_name.to_string();
        let key = object_key.to_string();
        let mut buffer_aac = BytesMut::new();
        let mut buffer_avc = BytesMut::new();
        let mut part_number_aac = 0;
        let mut part_number_avc = 0;
        let mut dts: Option<i64> = None;

        while let Ok(payload) = rx.recv().await {
            if dts.is_none() {
                dts = Some(payload.dts());
            }

            let buffer = match payload.kind {
                AuKind::AAC => &mut buffer_aac,
                AuKind::AVC => &mut buffer_avc,
                _ => continue,
            };

            buffer.extend_from_slice(&payload.lp_to_nal_start_code());
            if buffer.len() >= self.min_part_size {
                self.upload_part(
                    &client,
                    &bucket,
                    &key,
                    buffer,
                    payload.kind.clone(),
                    part_number_aac,
                    part_number_avc,
                    &dts,
                )
                .await;
                match payload.kind {
                    AuKind::AAC => part_number_aac += 1,
                    AuKind::AVC => part_number_avc += 1,
                    _ => {}
                }
            }
        }

        self.flush_remaining(
            &client,
            &bucket,
            &key,
            &mut buffer_aac,
            AuKind::AAC,
            part_number_aac,
            &dts,
        )
        .await?;
        self.flush_remaining(
            &client,
            &bucket,
            &key,
            &mut buffer_avc,
            AuKind::AVC,
            part_number_avc,
            &dts,
        )
        .await?;

        Ok(())
    }

    async fn upload_part(
        &self,
        client: &Arc<Client>,
        bucket: &str,
        key: &str,
        buffer: &mut BytesMut,
        kind: AuKind,
        part_number_aac: usize,
        part_number_avc: usize,
        dts: &Option<i64>,
    ) {
        let part_data = buffer.split_to(buffer.len()).freeze();
        let key_suffix = match kind {
            AuKind::AAC => format!("{}/{}.aac", key, part_number_aac),
            AuKind::AVC => format!("{}/{}.avc", key, part_number_avc),
            _ => unreachable!(),
        };

        let byte_stream = ByteStream::from(part_data);
        let mut req = client
            .put_object()
            .bucket(bucket.to_string())
            .key(key_suffix)
            .body(byte_stream);

        if let Some(v) = dts {
            req = req.metadata("dts", &v.to_string());
        }

        req.send().await.map_err(|e| {
            dbg!(e);
        });
    }

    async fn flush_remaining(
        &self,
        client: &Arc<Client>,
        bucket: &str,
        key: &str,
        buffer: &mut BytesMut,
        kind: AuKind,
        part_number: usize,
        dts: &Option<i64>,
    ) -> Result<(), PutObjectError> {
        if !buffer.is_empty() {
            self.upload_part(
                client,
                bucket,
                key,
                buffer,
                kind,
                part_number,
                part_number,
                dts,
            )
            .await;
        }
        Ok(())
    }
}
