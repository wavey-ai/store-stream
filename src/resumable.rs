//! Durable uploads with fixed-size chunks and a cloud-neutral final object.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, WriteMultipart};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone)]
pub struct ResumableStore {
    staging: Arc<dyn ObjectStore>,
    destination: Arc<dyn ObjectStore>,
    prefix: Path,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadSpec {
    pub id: String,
    pub source_version: String,
    pub destination: String,
    pub size: u64,
    pub chunk_size: u64,
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UploadState {
    Active { confirmed_offset: u64 },
    Complete { sha256: String },
    Aborted,
}

#[derive(Clone)]
pub struct UploadSession {
    store: ResumableStore,
    spec: UploadSpec,
    root: Path,
}

#[derive(Serialize, Deserialize)]
struct CompleteRecord {
    sha256: String,
}

impl ResumableStore {
    /// Use separate stores to allow a temporary bucket and a final bucket.
    pub fn new(
        staging: Arc<dyn ObjectStore>,
        destination: Arc<dyn ObjectStore>,
        prefix: Path,
    ) -> Self {
        Self {
            staging,
            destination,
            prefix,
        }
    }

    pub async fn begin(&self, spec: UploadSpec) -> Result<UploadSession> {
        validate_spec(&spec)?;
        let root = self.root(&spec.id);
        let manifest = root.clone().join("manifest.json");
        let payload = Bytes::from(serde_json::to_vec(&spec)?);
        match self
            .staging
            .put_opts(&manifest, payload.into(), create())
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                let saved: UploadSpec =
                    serde_json::from_slice(&self.staging.get(&manifest).await?.bytes().await?)?;
                if saved != spec {
                    bail!("upload ID already belongs to a different upload");
                }
            }
            Err(error) => return Err(error.into()),
        }
        Ok(UploadSession {
            store: self.clone(),
            spec,
            root,
        })
    }

    /// The caller must compare `source_version` with the current source before resuming.
    pub async fn resume(&self, id: &str, source_version: &str) -> Result<UploadSession> {
        if id.is_empty() {
            bail!("upload ID is empty");
        }
        let root = self.root(id);
        let manifest = root.clone().join("manifest.json");
        let spec: UploadSpec =
            serde_json::from_slice(&self.staging.get(&manifest).await?.bytes().await?)?;
        validate_spec(&spec)?;
        if spec.id != id || spec.source_version != source_version {
            bail!("source version changed; start a new upload");
        }
        Ok(UploadSession {
            store: self.clone(),
            spec,
            root,
        })
    }

    fn root(&self, id: &str) -> Path {
        self.prefix
            .clone()
            .join(hex(&Sha256::digest(id.as_bytes())))
    }
}

impl UploadSession {
    pub fn spec(&self) -> &UploadSpec {
        &self.spec
    }

    pub async fn state(&self) -> Result<UploadState> {
        if let Some(bytes) = read_optional(
            &self.store.staging,
            &self.root.clone().join("complete.json"),
        )
        .await?
        {
            let record: CompleteRecord = serde_json::from_slice(&bytes)?;
            return Ok(UploadState::Complete {
                sha256: record.sha256,
            });
        }
        if exists(&self.store.staging, &self.root.clone().join("aborted")).await? {
            return Ok(UploadState::Aborted);
        }
        let count = self.part_count();
        let mut seen = vec![false; usize::try_from(count).context("too many chunks")?];
        let prefix = self.root.clone().join("parts");
        let mut listing = self.store.staging.list(Some(&prefix));
        while let Some(meta) = listing.try_next().await? {
            let name = meta
                .location
                .as_ref()
                .strip_prefix(&format!("{prefix}/"))
                .ok_or_else(|| anyhow!("unexpected part path"))?;
            let index: u64 = name.parse().context("invalid part index")?;
            if index >= count {
                bail!("part index is outside the upload");
            }
            if meta.size != self.part_len(index) {
                bail!("stored part has the wrong size");
            }
            seen[index as usize] = true;
        }
        let confirmed_parts = seen.iter().take_while(|present| **present).count() as u64;
        if seen[confirmed_parts as usize..]
            .iter()
            .any(|present| *present)
        {
            bail!("stored parts contain a gap");
        }
        let confirmed_offset = if confirmed_parts == count {
            self.spec.size
        } else {
            confirmed_parts * self.spec.chunk_size
        };
        Ok(UploadState::Active { confirmed_offset })
    }

    /// Append one whole chunk. The last chunk can be shorter.
    pub async fn append(&self, offset: u64, bytes: Bytes) -> Result<u64> {
        if !offset.is_multiple_of(self.spec.chunk_size) || offset >= self.spec.size {
            bail!("chunk offset is outside the upload or is not aligned");
        }
        let index = offset / self.spec.chunk_size;
        if bytes.len() as u64 != self.part_len(index) {
            bail!("chunk has the wrong size");
        }
        if exists(
            &self.store.staging,
            &self.root.clone().join("complete.json"),
        )
        .await?
        {
            bail!("upload is complete");
        }
        if exists(&self.store.staging, &self.root.clone().join("aborted")).await? {
            bail!("upload is aborted");
        }
        let path = self.part_path(index);
        if exists(&self.store.staging, &path).await? {
            self.verify_existing_part(&path, &bytes).await?;
            return match self.state().await? {
                UploadState::Active { confirmed_offset } => Ok(confirmed_offset),
                _ => bail!("upload is no longer active"),
            };
        }
        if index > 0 {
            let previous = self
                .store
                .staging
                .head(&self.part_path(index - 1))
                .await
                .context("previous chunk is missing")?;
            if previous.size != self.part_len(index - 1) {
                bail!("previous chunk has the wrong size");
            }
        }
        match self
            .store
            .staging
            .put_opts(&path, bytes.clone().into(), create())
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.verify_existing_part(&path, &bytes).await?;
                return match self.state().await? {
                    UploadState::Active { confirmed_offset } => Ok(confirmed_offset),
                    _ => bail!("upload is no longer active"),
                };
            }
            Err(error) => return Err(error.into()),
        }
        Ok(offset + bytes.len() as u64)
    }

    /// Return a range from the confirmed prefix before finalization.
    pub async fn read_confirmed(&self, start: u64, length: u64) -> Result<Bytes> {
        let confirmed = match self.state().await? {
            UploadState::Active { confirmed_offset } => confirmed_offset,
            _ => bail!("upload is not active"),
        };
        let end = start.checked_add(length).context("range overflow")?;
        if end > confirmed {
            bail!("range exceeds the confirmed prefix");
        }
        if length == 0 {
            return Ok(Bytes::new());
        }
        let capacity = usize::try_from(length).context("range is too large")?;
        let mut result = Vec::with_capacity(capacity);
        let mut position = start;
        while position < end {
            let index = position / self.spec.chunk_size;
            let part_start = position % self.spec.chunk_size;
            let part_end = (end - index * self.spec.chunk_size).min(self.part_len(index));
            let chunk = self
                .store
                .staging
                .get_range(&self.part_path(index), part_start..part_end)
                .await?;
            if chunk.len() as u64 != part_end - part_start {
                bail!("stored part returned a short range");
            }
            result.extend_from_slice(&chunk);
            position += chunk.len() as u64;
        }
        Ok(Bytes::from(result))
    }

    /// Build one normal object. Retry this call after an interrupted finalization.
    pub async fn finish(&self) -> Result<String> {
        let confirmed = match self.state().await? {
            UploadState::Active { confirmed_offset } => confirmed_offset,
            UploadState::Aborted => bail!("upload is aborted"),
            UploadState::Complete { sha256 } => {
                self.cleanup_parts().await?;
                return Ok(sha256);
            }
        };
        if confirmed != self.spec.size {
            bail!("upload is incomplete");
        }
        let digest = self.hash_parts().await?;
        if let Some(expected) = &self.spec.sha256 {
            if !expected.eq_ignore_ascii_case(&digest) {
                bail!("upload checksum differs from the expected checksum");
            }
        }
        let target = Path::parse(&self.spec.destination)?;
        if exists(&self.store.destination, &target).await? {
            let actual = hash_object(&self.store.destination, &target).await?;
            if actual != digest {
                bail!("destination already contains different bytes");
            }
        } else if self.spec.size == 0 {
            self.store
                .destination
                .put(&target, Bytes::new().into())
                .await?;
        } else {
            let upload = self.store.destination.put_multipart(&target).await?;
            let mut writer = WriteMultipart::new(upload);
            let result: Result<()> = async {
                for index in 0..self.part_count() {
                    let mut stream = self
                        .store
                        .staging
                        .get(&self.part_path(index))
                        .await?
                        .into_stream();
                    while let Some(chunk) = stream.try_next().await? {
                        writer.wait_for_capacity(4).await?;
                        writer.put(chunk);
                    }
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                writer.abort().await?;
                return Err(error);
            }
            writer.finish().await?;
        }
        let record = CompleteRecord {
            sha256: digest.clone(),
        };
        let bytes = Bytes::from(serde_json::to_vec(&record)?);
        match self
            .store
            .staging
            .put_opts(
                &self.root.clone().join("complete.json"),
                bytes.into(),
                create(),
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        self.cleanup_parts().await?;
        Ok(digest)
    }

    /// Mark the session aborted and remove saved chunks.
    pub async fn abort(&self) -> Result<()> {
        if matches!(self.state().await?, UploadState::Complete { .. }) {
            bail!("upload is complete");
        }
        let target = Path::parse(&self.spec.destination)?;
        if exists(&self.store.destination, &target).await? {
            bail!("destination exists; inspect it before aborting");
        }
        let marker = self.root.clone().join("aborted");
        match self
            .store
            .staging
            .put_opts(&marker, Bytes::new().into(), create())
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        self.cleanup_parts().await
    }

    async fn verify_existing_part(&self, path: &Path, bytes: &Bytes) -> Result<()> {
        let saved = self.store.staging.get(path).await?.bytes().await?;
        if saved != *bytes {
            bail!("chunk conflicts with stored bytes");
        }
        Ok(())
    }

    async fn hash_parts(&self) -> Result<String> {
        let mut hasher = Sha256::new();
        for index in 0..self.part_count() {
            let mut seen = 0u64;
            let mut stream = self
                .store
                .staging
                .get(&self.part_path(index))
                .await?
                .into_stream();
            while let Some(chunk) = stream.try_next().await? {
                seen += chunk.len() as u64;
                hasher.update(&chunk);
            }
            if seen != self.part_len(index) {
                bail!("stored part has the wrong size");
            }
        }
        Ok(hex(&hasher.finalize()))
    }

    async fn cleanup_parts(&self) -> Result<()> {
        for index in 0..self.part_count() {
            let path = self.part_path(index);
            match self.store.staging.delete(&path).await {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn part_count(&self) -> u64 {
        self.spec.size / self.spec.chunk_size
            + u64::from(!self.spec.size.is_multiple_of(self.spec.chunk_size))
    }

    fn part_len(&self, index: u64) -> u64 {
        (self.spec.size - index * self.spec.chunk_size).min(self.spec.chunk_size)
    }

    fn part_path(&self, index: u64) -> Path {
        self.root.clone().join("parts").join(format!("{index:020}"))
    }
}

fn validate_spec(spec: &UploadSpec) -> Result<()> {
    if spec.id.is_empty() || spec.source_version.is_empty() || spec.chunk_size == 0 {
        bail!("upload ID, source version, and chunk size are required");
    }
    if spec.destination.is_empty() {
        bail!("destination is required");
    }
    Path::parse(&spec.destination)?;
    let count = spec.size / spec.chunk_size + u64::from(!spec.size.is_multiple_of(spec.chunk_size));
    if count > 1_000_000 {
        bail!("upload requires more than one million chunks");
    }
    if let Some(digest) = &spec.sha256 {
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("SHA-256 must be 64 hexadecimal characters");
        }
    }
    Ok(())
}

fn create() -> PutOptions {
    PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    }
}

async fn exists(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<bool> {
    match store.head(path).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn read_optional(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<Option<Bytes>> {
    match store.get(path).await {
        Ok(result) => Ok(Some(result.bytes().await?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn hash_object(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut stream = store.get(path).await?.into_stream();
    while let Some(chunk) = stream.try_next().await? {
        hasher.update(&chunk);
    }
    Ok(hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn setup() -> (ResumableStore, Arc<dyn ObjectStore>, Arc<dyn ObjectStore>) {
        let staging: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let destination: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = ResumableStore::new(
            staging.clone(),
            destination.clone(),
            Path::from("temporary"),
        );
        (store, staging, destination)
    }

    fn spec(size: u64) -> UploadSpec {
        UploadSpec {
            id: "file-1".into(),
            source_version: "etag-1".into(),
            destination: "projects/file-1".into(),
            size,
            chunk_size: 4,
            sha256: None,
        }
    }

    #[tokio::test]
    async fn resumes_and_finishes_into_one_object() {
        let (store, staging, destination) = setup();
        let session = store.begin(spec(10)).await.unwrap();
        assert_eq!(
            session
                .append(0, Bytes::from_static(b"abcd"))
                .await
                .unwrap(),
            4
        );
        drop(session);

        let session = store.resume("file-1", "etag-1").await.unwrap();
        assert_eq!(
            session.state().await.unwrap(),
            UploadState::Active {
                confirmed_offset: 4
            }
        );
        assert_eq!(
            session
                .append(0, Bytes::from_static(b"abcd"))
                .await
                .unwrap(),
            4
        );
        assert!(session
            .append(0, Bytes::from_static(b"xxxx"))
            .await
            .is_err());
        assert_eq!(
            session
                .append(4, Bytes::from_static(b"efgh"))
                .await
                .unwrap(),
            8
        );
        assert_eq!(
            session.append(8, Bytes::from_static(b"ij")).await.unwrap(),
            10
        );
        assert_eq!(
            session.read_confirmed(2, 6).await.unwrap(),
            Bytes::from_static(b"cdefgh")
        );
        let digest = session.finish().await.unwrap();
        assert_eq!(digest, hex(&Sha256::digest(b"abcdefghij")));
        assert_eq!(
            session.state().await.unwrap(),
            UploadState::Complete {
                sha256: digest.clone()
            }
        );
        assert_eq!(session.finish().await.unwrap(), digest);
        assert_eq!(
            destination
                .get(&Path::from("projects/file-1"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"abcdefghij")
        );
        assert!(!exists(&staging, &session.part_path(0)).await.unwrap());
    }

    #[tokio::test]
    async fn source_and_manifest_cannot_change() {
        let (store, _, _) = setup();
        store.begin(spec(4)).await.unwrap();
        store.begin(spec(4)).await.unwrap();
        assert!(store.resume("file-1", "etag-2").await.is_err());
        let mut changed = spec(4);
        changed.destination = "other".into();
        assert!(store.begin(changed).await.is_err());
    }

    #[tokio::test]
    async fn only_complete_sequential_chunks_are_confirmed() {
        let (store, _, _) = setup();
        let session = store.begin(spec(8)).await.unwrap();
        assert!(session
            .append(4, Bytes::from_static(b"efgh"))
            .await
            .is_err());
        assert!(session.append(0, Bytes::from_static(b"abc")).await.is_err());
        assert!(session.read_confirmed(0, 1).await.is_err());
        session
            .append(0, Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        assert!(session.read_confirmed(3, 2).await.is_err());
    }

    #[tokio::test]
    async fn finish_recovers_when_final_object_was_written_before_marker() {
        let (store, _, destination) = setup();
        let session = store.begin(spec(4)).await.unwrap();
        session
            .append(0, Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        destination
            .put(
                &Path::from("projects/file-1"),
                Bytes::from_static(b"abcd").into(),
            )
            .await
            .unwrap();
        session.finish().await.unwrap();
        assert!(matches!(
            session.state().await.unwrap(),
            UploadState::Complete { .. }
        ));
    }

    #[tokio::test]
    async fn finish_rejects_existing_different_object() {
        let (store, _, destination) = setup();
        let session = store.begin(spec(4)).await.unwrap();
        session
            .append(0, Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        destination
            .put(
                &Path::from("projects/file-1"),
                Bytes::from_static(b"wxyz").into(),
            )
            .await
            .unwrap();
        assert!(session.finish().await.is_err());
    }

    #[tokio::test]
    async fn checksum_and_completion_require_all_chunks() {
        let (store, _, _) = setup();
        let mut upload = spec(8);
        upload.sha256 = Some(hex(&Sha256::digest(b"different")));
        let session = store.begin(upload).await.unwrap();
        assert!(session.finish().await.is_err());
        session
            .append(0, Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        assert!(session.finish().await.is_err());
        session
            .append(4, Bytes::from_static(b"efgh"))
            .await
            .unwrap();
        assert!(session.finish().await.is_err());
    }

    #[tokio::test]
    async fn abort_keeps_a_tombstone() {
        let (store, staging, _) = setup();
        let session = store.begin(spec(4)).await.unwrap();
        session
            .append(0, Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        session.abort().await.unwrap();
        assert_eq!(session.state().await.unwrap(), UploadState::Aborted);
        assert!(!exists(&staging, &session.part_path(0)).await.unwrap());
        assert!(session
            .append(0, Bytes::from_static(b"abcd"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn empty_upload_finishes() {
        let (store, _, destination) = setup();
        let session = store.begin(spec(0)).await.unwrap();
        session.finish().await.unwrap();
        assert_eq!(
            destination
                .head(&Path::from("projects/file-1"))
                .await
                .unwrap()
                .size,
            0
        );
    }

    #[tokio::test]
    #[ignore = "requires local S3-compatible storage and TEST_KEY_ID/TEST_SECRET_KEY"]
    async fn resumes_against_s3_compatible_storage() {
        use object_store::aws::AmazonS3Builder;
        use std::time::{SystemTime, UNIX_EPOCH};

        let key = std::env::var("TEST_KEY_ID").unwrap();
        let secret = std::env::var("TEST_SECRET_KEY").unwrap();
        let bucket = std::env::var("TEST_BUCKET").unwrap_or_else(|_| "test".into());
        let endpoint =
            std::env::var("TEST_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
        let s3: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region("us-west-2")
                .with_endpoint(endpoint)
                .with_access_key_id(key)
                .with_secret_access_key(secret)
                .with_allow_http(true)
                .build()
                .unwrap(),
        );
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let store = ResumableStore::new(
            s3.clone(),
            s3.clone(),
            Path::from(format!("resume-test/{unique}")),
        );
        let data = Bytes::from(vec![b'z'; 6 * 1024 * 1024 + 17]);
        let mut upload = spec(data.len() as u64);
        upload.id = format!("session-{unique}");
        upload.destination = format!("resume-test/{unique}/final");
        upload.chunk_size = 2 * 1024 * 1024;
        let first = store.begin(upload.clone()).await.unwrap();
        first
            .append(0, data.slice(..2 * 1024 * 1024))
            .await
            .unwrap();
        drop(first);
        let resumed = store
            .resume(&upload.id, &upload.source_version)
            .await
            .unwrap();
        assert_eq!(
            resumed.state().await.unwrap(),
            UploadState::Active {
                confirmed_offset: upload.chunk_size
            }
        );
        for offset in (upload.chunk_size as usize..data.len()).step_by(upload.chunk_size as usize) {
            let end = (offset + upload.chunk_size as usize).min(data.len());
            resumed
                .append(offset as u64, data.slice(offset..end))
                .await
                .unwrap();
        }
        resumed.finish().await.unwrap();
        assert_eq!(
            s3.get(&Path::from(upload.destination.clone()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            data
        );
        s3.delete(&Path::from(upload.destination)).await.unwrap();
        s3.delete(&resumed.root.clone().join("manifest.json"))
            .await
            .unwrap();
        s3.delete(&resumed.root.clone().join("complete.json"))
            .await
            .unwrap();
    }
}
