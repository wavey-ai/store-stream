# Store Stream

A Rust library that provides a high-level interface for interacting with S3-compatible object storage services. This implementation includes support for multipart uploads, byte range fetching, and efficient handling of large objects.

It is possible to stream multi-gb files to storage which will be saved at intervals of the minimum part size and can be addressed via byte range queries before the upload has completed.

## Features

- Multipart upload support with configurable minimum part size
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
    "your-secret-key".to_string(),
    5 * 1024 * 1024 // 5MB minimum part size
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

// Upload the data
storage.upload("bucket-name", "object-key", rx).await?;
```

### Fetching Objects

Fetch specific byte ranges from objects:

```rust
let bytes = storage
    .get_byte_range("bucket-name", "object-key", 0, Some(100))
    .await?;
```

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
- **Chunked Upload**: Automatically handles large file uploads by splitting them into chunks
- **Offset Tracking**: Maintains object part offsets for efficient byte range access
- **Streaming Support**: Uses Tokio channels for efficient streaming of data

## Requirements

- Rust 2021 edition or later
- Tokio runtime
- AWS SDK for Rust

## License

MIT
