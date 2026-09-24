# Store Stream

A Rust library that writes stream chunks to S3-compatible object storage and reads saved byte ranges.

You can stream multi-gigabyte files to storage. The library saves data at
minimum-part-size intervals. Byte-range queries can read saved data before the
upload is complete.

## Features

- Chunked object uploads with a configurable part size
- Byte range fetching with offset tracking
- Bucket management (creation, existence checking)
- Object listing
- Async/await support using Tokio
- Error handling with Anyhow
- Support for S3-compatible storage services

## Usage

### Initialization

```rust
let storage = Storage::new(
    "https://your-endpoint.com".to_string(),
    "your-key-id".to_string(),
    "your-secret-key".to_string()
);
```

### Uploading Objects

The library supports streaming uploads using Tokio channels:

```rust
use tokio::sync::mpsc;
use bytes::Bytes;

let (tx, rx) = mpsc::channel(16);

// Spawn a task to send data
tokio::spawn(async move {
    let data = Bytes::from("Hello, S3!");
    tx.send(data).await.unwrap();
});

// Upload the data in objects of at least 5 MiB where possible.
storage
    .upload("bucket-name", "object-key", rx, 5 * 1024 * 1024)
    .await?;
```

An upload writes chunks under `object-key/0000000000`,
`object-key/0000000001`, and so on. It also writes `object-key.dat`, a compact
big-endian `u64` offset table. The offset table is updated after each chunk is
stored, so byte ranges can be fetched before a long upload has completed.

Each chunk is a separate object. The library does not use the S3 multipart upload API.
`Storage::upload` starts at part zero. It does not resume an interrupted call.

### Fetching Objects

Fetch specific byte ranges from objects:

```rust
let bytes = storage
    .get_byte_range("bucket-name", "object-key", 0, Some(100))
    .await?;
```

`range_end` is inclusive, matching HTTP byte range semantics. Passing `None`
for `range_end` fetches from `range_start` through the currently available end
of the stream.

### Bucket Operations

```rust
// Create a bucket
storage.create_bucket("new-bucket").await?;

// Check if bucket exists
let exists = storage.bucket_exists("bucket-name").await?;

// List bucket contents
let result = storage.list_bucket("bucket-name").await?;
for object in result.objects {
    println!("Key: {}, Size: {}", object.key, object.size);
}
```

## Features

- **Path Style Access**: Forces path-style access for compatibility with various S3-compatible services
- **Chunked Upload**: Splits input into objects at the configured part size
- **Offset Tracking**: Maintains object part offsets for efficient byte range access
- **Streaming Support**: Uses Tokio channels for efficient streaming of data

## Requirements

- Rust 2021 edition or later
- Tokio runtime
- AWS SDK for Rust

## Tests

`cargo test` runs unit tests for offset serialization and range selection.
Live S3-compatible storage tests are marked ignored because they require a
running endpoint and credentials:

```bash
TEST_KEY_ID=minioadmin TEST_SECRET_KEY=minioadmin cargo test -- --ignored
```

## License

MIT
