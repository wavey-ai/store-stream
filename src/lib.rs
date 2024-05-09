use au::{AuKind, AuPayload};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{config::Region, Client};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;

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
    ) -> Result<(), PutObjectError> {
        let mut part_number_aac = 0;
        let mut part_number_avc = 0;

        while let Ok(payload) = rx.recv().await {
            let byte_stream = ByteStream::from(payload.lp_to_nal_start_code());
            let client = Arc::clone(&self.client);
            let bucket = bucket_name.to_string();
            let key = object_key.to_string();
            let kind = payload.kind.clone();
            let dts = payload.dts();
            let pts = payload.pts();
            tokio::task::spawn(async move {
                upload_part(
                    client,
                    bucket,
                    key,
                    byte_stream,
                    kind,
                    part_number_aac,
                    part_number_avc,
                    dts,
                    pts,
                )
                .await;
            });

            match payload.kind {
                AuKind::AAC => part_number_aac += 1,
                AuKind::AVC => part_number_avc += 1,
                _ => {}
            }
        }

        Ok(())
    }
}

async fn upload_part(
    client: Arc<Client>,
    bucket: String,
    key: String,
    body: ByteStream,
    kind: AuKind,
    part_number_aac: usize,
    part_number_avc: usize,
    dts: i64,
    pts: i64,
) {
    let key_suffix = match kind {
        AuKind::AAC => format!("{}/{}.aac", key, part_number_aac),
        AuKind::AVC => format!("{}/{}.avc", key, part_number_avc),
        _ => unreachable!(),
    };

    client
        .put_object()
        .bucket(bucket.to_string())
        .key(key_suffix)
        .body(body)
        .metadata("dts", &dts.to_string())
        .metadata("pts", &pts.to_string())
        .send()
        .await
        .map_err(|e| {
            dbg!(e);
        });
}
