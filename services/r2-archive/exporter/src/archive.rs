use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use tempfile::{Builder, NamedTempFile};
use tokio::io::AsyncReadExt;
use tokio::runtime::Handle;

use crate::config::ExportBackend;

pub const MULTIPART_PART_SIZE: u64 = 64 * 1024 * 1024;
const MAX_MULTIPART_PARTS: u64 = 10_000;
const SDK_MAX_ATTEMPTS: u32 = 5;

pub trait Archive: Send + Sync {
    fn stage(&self, key: &str) -> Result<NamedTempFile>;
    fn commit(&self, key: &str, file: NamedTempFile) -> Result<()>;
    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()>;
    fn exists(&self, key: &str) -> Result<bool>;
    fn get(&self, key: &str, max_bytes: usize) -> Result<Option<Vec<u8>>>;
}

pub async fn build_archive(backend: ExportBackend) -> Result<Arc<dyn Archive>> {
    match backend {
        ExportBackend::Local { root } => Ok(Arc::new(LocalArchive::new(root)?)),
        ExportBackend::R2 {
            endpoint,
            access_key,
            secret_key,
            bucket,
        } => Ok(Arc::new(
            R2Archive::new(endpoint, access_key, secret_key, bucket).await?,
        )),
    }
}

fn validate_archive_key(key: &str) -> Result<()> {
    ensure!(!key.is_empty(), "archive key must not be empty");
    ensure!(
        key.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && !segment.contains('\\')
                && !segment.chars().any(char::is_control)
        }),
        "invalid archive key: {key:?}"
    );
    for component in Path::new(key).components() {
        if !matches!(component, Component::Normal(_)) {
            bail!("invalid archive key: {key:?}");
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct LocalArchive {
    root: PathBuf,
}

impl LocalArchive {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)
            .with_context(|| format!("create local archive root {}", root.display()))?;
        let root = root
            .canonicalize()
            .with_context(|| format!("canonicalize local archive root {}", root.display()))?;
        ensure!(root.is_dir(), "local archive root is not a directory");
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn target(&self, key: &str) -> Result<PathBuf> {
        validate_archive_key(key)?;
        let relative = Path::new(key);
        Ok(self.root.join(relative))
    }

    fn prepare_parent(&self, key: &str) -> Result<(PathBuf, PathBuf)> {
        let target = self.target(key)?;
        let parent = target.parent().context("archive target has no parent")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("create archive directory {}", parent.display()))?;
        let canonical_parent = parent
            .canonicalize()
            .with_context(|| format!("canonicalize archive directory {}", parent.display()))?;
        ensure!(
            canonical_parent.starts_with(&self.root),
            "archive key resolves outside local export directory: {key:?}"
        );
        let filename = target
            .file_name()
            .context("archive target has no filename")?;
        Ok((canonical_parent.join(filename), canonical_parent))
    }

    fn existing_target(&self, key: &str) -> Result<Option<PathBuf>> {
        let target = self.target(key)?;
        let target = match target.canonicalize() {
            Ok(target) => target,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| format!("resolve archive object {key}"));
            }
        };
        ensure!(
            target.starts_with(&self.root),
            "archive key resolves outside local export directory: {key:?}"
        );
        Ok(Some(target))
    }

    fn sync_directory(path: &Path) -> Result<()> {
        File::open(path)
            .with_context(|| format!("open archive directory {}", path.display()))?
            .sync_all()
            .with_context(|| format!("sync archive directory {}", path.display()))
    }
}

impl Archive for LocalArchive {
    fn stage(&self, key: &str) -> Result<NamedTempFile> {
        let (target, parent) = self.prepare_parent(key)?;
        let filename = target
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("archive");
        Builder::new()
            .prefix(&format!(".{filename}."))
            .suffix(".tmp")
            .tempfile_in(&parent)
            .with_context(|| format!("create staged archive object for {key}"))
    }

    fn commit(&self, key: &str, file: NamedTempFile) -> Result<()> {
        let (target, parent) = self.prepare_parent(key)?;
        file.as_file()
            .sync_all()
            .with_context(|| format!("sync staged archive object for {key}"))?;
        file.persist(&target)
            .map_err(|error| error.error)
            .with_context(|| format!("atomically publish archive object {key}"))?;
        Self::sync_directory(&parent)
    }

    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        let mut staged = self.stage(key)?;
        staged
            .write_all(data)
            .with_context(|| format!("write staged archive object {key}"))?;
        staged
            .flush()
            .with_context(|| format!("flush staged archive object {key}"))?;
        self.commit(key, staged)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self
            .existing_target(key)?
            .is_some_and(|path| path.is_file()))
    }

    fn get(&self, key: &str, max_bytes: usize) -> Result<Option<Vec<u8>>> {
        let Some(target) = self.existing_target(key)? else {
            return Ok(None);
        };
        let mut file = File::open(&target).with_context(|| format!("open archive object {key}"))?;
        let length = file
            .metadata()
            .with_context(|| format!("read archive object metadata {key}"))?
            .len();
        ensure!(
            length <= max_bytes as u64,
            "archive object {key} exceeds {max_bytes} byte read limit"
        );
        let mut data = Vec::with_capacity(length as usize);
        file.read_to_end(&mut data)
            .with_context(|| format!("read archive object {key}"))?;
        Ok(Some(data))
    }
}

#[derive(Clone)]
pub struct R2Archive {
    runtime: Handle,
    store: Arc<dyn S3Store>,
}

impl R2Archive {
    async fn new(
        endpoint: String,
        access_key: String,
        secret_key: String,
        bucket: String,
    ) -> Result<Self> {
        let credentials = Credentials::new(access_key, secret_key, None, None, "r2-static");
        let sdk_config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .credentials_provider(credentials)
            .region(Region::new("auto"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .retry_config(RetryConfig::standard().with_max_attempts(SDK_MAX_ATTEMPTS))
            .build();
        let store: Arc<dyn S3Store> = Arc::new(AwsS3Store {
            client: Client::from_conf(sdk_config),
            bucket,
        });
        store.ensure_bucket().await.context("verify R2 bucket")?;
        Ok(Self {
            runtime: Handle::current(),
            store,
        })
    }

    #[cfg(test)]
    fn with_store(runtime: Handle, store: Arc<dyn S3Store>) -> Self {
        Self { runtime, store }
    }
}

impl Archive for R2Archive {
    fn stage(&self, key: &str) -> Result<NamedTempFile> {
        validate_archive_key(key)?;
        Builder::new()
            .prefix(".polymarket-export.")
            .suffix(".tmp")
            .tempfile()
            .with_context(|| format!("create staged R2 object for {key}"))
    }

    fn commit(&self, key: &str, file: NamedTempFile) -> Result<()> {
        validate_archive_key(key)?;
        let size = file
            .as_file()
            .metadata()
            .with_context(|| format!("read staged R2 object metadata for {key}"))?
            .len();
        self.runtime.block_on(upload_file(
            self.store.as_ref(),
            key,
            file.path().to_path_buf(),
            size,
        ))
    }

    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
        validate_archive_key(key)?;
        self.runtime
            .block_on(self.store.put(key, UploadBody::Bytes(data.to_vec())))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        validate_archive_key(key)?;
        self.runtime.block_on(self.store.head(key))
    }

    fn get(&self, key: &str, max_bytes: usize) -> Result<Option<Vec<u8>>> {
        validate_archive_key(key)?;
        self.runtime.block_on(self.store.get(key, max_bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MultipartPart {
    number: i32,
    offset: u64,
    length: u64,
}

fn multipart_plan(size: u64) -> Result<Vec<MultipartPart>> {
    ensure!(size > MULTIPART_PART_SIZE, "multipart object is too small");
    let count = size.div_ceil(MULTIPART_PART_SIZE);
    ensure!(
        count <= MAX_MULTIPART_PARTS,
        "R2 object requires {count} multipart parts; maximum is {MAX_MULTIPART_PARTS}"
    );
    let mut parts = Vec::with_capacity(count as usize);
    for index in 0..count {
        let offset = index * MULTIPART_PART_SIZE;
        parts.push(MultipartPart {
            number: i32::try_from(index + 1).context("multipart part number overflow")?,
            offset,
            length: (size - offset).min(MULTIPART_PART_SIZE),
        });
    }
    Ok(parts)
}

async fn upload_file(store: &dyn S3Store, key: &str, path: PathBuf, size: u64) -> Result<()> {
    if size <= MULTIPART_PART_SIZE {
        return store
            .put(
                key,
                UploadBody::FileRange {
                    path,
                    offset: 0,
                    length: size,
                },
            )
            .await;
    }

    let plan = multipart_plan(size)?;
    let upload_id = store
        .create_multipart(key)
        .await
        .with_context(|| format!("start multipart upload for {key}"))?;
    let mut completed = Vec::with_capacity(plan.len());
    for part in plan {
        let result = store
            .upload_part(
                key,
                &upload_id,
                part.number,
                UploadBody::FileRange {
                    path: path.clone(),
                    offset: part.offset,
                    length: part.length,
                },
            )
            .await;
        match result {
            Ok(etag) => completed.push(UploadedPart {
                number: part.number,
                etag,
            }),
            Err(error) => {
                return abort_multipart(store, key, &upload_id, error).await;
            }
        }
    }
    if let Err(error) = store.complete(key, &upload_id, completed).await {
        return abort_multipart(store, key, &upload_id, error).await;
    }
    Ok(())
}

async fn abort_multipart(
    store: &dyn S3Store,
    key: &str,
    upload_id: &str,
    original: anyhow::Error,
) -> Result<()> {
    match store.abort(key, upload_id).await {
        Ok(()) => Err(original),
        Err(abort_error) => Err(original.context(format!(
            "abort multipart upload for {key} also failed: {abort_error:#}"
        ))),
    }
}

enum UploadBody {
    Bytes(Vec<u8>),
    FileRange {
        path: PathBuf,
        offset: u64,
        length: u64,
    },
}

impl UploadBody {
    fn length(&self) -> u64 {
        match self {
            Self::Bytes(data) => data.len() as u64,
            Self::FileRange { length, .. } => *length,
        }
    }

    async fn into_sdk(self) -> Result<ByteStream> {
        match self {
            Self::Bytes(data) => Ok(ByteStream::from(data)),
            Self::FileRange {
                path,
                offset,
                length,
            } => ByteStream::read_from()
                .path(path)
                .offset(offset)
                .length(Length::Exact(length))
                .build()
                .await
                .context("open staged R2 upload range"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UploadedPart {
    number: i32,
    etag: String,
}

#[async_trait]
trait S3Store: Send + Sync {
    async fn ensure_bucket(&self) -> Result<()>;
    async fn head(&self, key: &str) -> Result<bool>;
    async fn get(&self, key: &str, max_bytes: usize) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &str, body: UploadBody) -> Result<()>;
    async fn create_multipart(&self, key: &str) -> Result<String>;
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: UploadBody,
    ) -> Result<String>;
    async fn complete(&self, key: &str, upload_id: &str, parts: Vec<UploadedPart>) -> Result<()>;
    async fn abort(&self, key: &str, upload_id: &str) -> Result<()>;
}

struct AwsS3Store {
    client: Client,
    bucket: String,
}

#[async_trait]
impl S3Store for AwsS3Store {
    async fn ensure_bucket(&self) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .with_context(|| {
                format!(
                    "R2 bucket {:?} is unavailable; create it and grant access before starting the exporter",
                    self.bucket
                )
            })?;
        Ok(())
    }

    async fn head(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_not_found()) =>
            {
                Ok(false)
            }
            Err(error) => Err(error).with_context(|| format!("head R2 object {key}")),
        }
    }

    async fn get(&self, key: &str, max_bytes: usize) -> Result<Option<Vec<u8>>> {
        let response = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_no_such_key()) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error).with_context(|| format!("get R2 object {key}")),
        };
        if let Some(length) = response.content_length() {
            ensure!(
                length >= 0 && length as u64 <= max_bytes as u64,
                "R2 object {key} exceeds {max_bytes} byte read limit"
            );
        }
        let read_limit = max_bytes
            .checked_add(1)
            .context("R2 object read limit overflow")? as u64;
        let mut reader = response.body.into_async_read().take(read_limit);
        let mut data = Vec::with_capacity(max_bytes.min(64 * 1024));
        reader
            .read_to_end(&mut data)
            .await
            .with_context(|| format!("read R2 object {key}"))?;
        ensure!(
            data.len() <= max_bytes,
            "R2 object {key} exceeds {max_bytes} byte read limit"
        );
        Ok(Some(data))
    }

    async fn put(&self, key: &str, body: UploadBody) -> Result<()> {
        let content_length = i64::try_from(body.length()).context("R2 object is too large")?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_length(content_length)
            .body(body.into_sdk().await?)
            .send()
            .await
            .with_context(|| format!("put R2 object {key}"))?;
        Ok(())
    }

    async fn create_multipart(&self, key: &str) -> Result<String> {
        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("create R2 multipart upload for {key}"))?;
        response
            .upload_id()
            .map(ToOwned::to_owned)
            .context("R2 create multipart response has no upload ID")
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: UploadBody,
    ) -> Result<String> {
        let content_length = i64::try_from(body.length()).context("R2 part is too large")?;
        let response = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .content_length(content_length)
            .body(body.into_sdk().await?)
            .send()
            .await
            .with_context(|| format!("upload R2 part {part_number} for {key}"))?;
        response
            .e_tag()
            .map(ToOwned::to_owned)
            .context("R2 upload part response has no ETag")
    }

    async fn complete(&self, key: &str, upload_id: &str, parts: Vec<UploadedPart>) -> Result<()> {
        let mut upload = CompletedMultipartUpload::builder();
        for part in parts {
            upload = upload.parts(
                CompletedPart::builder()
                    .part_number(part.number)
                    .e_tag(part.etag)
                    .build(),
            );
        }
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(upload.build())
            .send()
            .await
            .with_context(|| format!("complete R2 multipart upload for {key}"))?;
        Ok(())
    }

    async fn abort(&self, key: &str, upload_id: &str) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .with_context(|| format!("abort R2 multipart upload for {key}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Read;
    use std::sync::Mutex;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn atomically_replaces_local_objects_and_lists_no_staging_files() {
        let directory = TempDir::new().unwrap();
        let archive = LocalArchive::new(directory.path().join("archive")).unwrap();
        let key = "2026-08-13/04/book.parquet";

        archive.put_bytes(key, b"first").unwrap();
        archive.put_bytes(key, b"second").unwrap();

        let mut contents = Vec::new();
        File::open(archive.root().join(key))
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();
        assert_eq!(contents, b"second");
        assert!(archive.exists(key).unwrap());
        assert_eq!(archive.get(key, 6).unwrap().unwrap(), b"second");
        assert!(archive.get(key, 5).is_err());
        assert_eq!(archive.get("missing", 10).unwrap(), None);
        assert!(!archive.exists("2026-08-13/04/manifest.json").unwrap());
        assert_eq!(
            fs::read_dir(archive.root().join("2026-08-13/04"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn rejects_traversal_absolute_and_symlink_escape() {
        let directory = TempDir::new().unwrap();
        let archive = LocalArchive::new(directory.path().join("archive")).unwrap();
        assert!(archive.put_bytes("../outside", b"bad").is_err());
        assert!(archive.put_bytes("/absolute", b"bad").is_err());
        assert!(archive.put_bytes("date/../outside", b"bad").is_err());
        assert!(archive.put_bytes("date//outside", b"bad").is_err());
        assert!(archive.put_bytes("date\\outside", b"bad").is_err());

        #[cfg(unix)]
        {
            fs::write(directory.path().join("outside"), b"outside").unwrap();
            std::os::unix::fs::symlink(directory.path(), archive.root().join("escape")).unwrap();
            assert!(archive.put_bytes("escape/outside", b"bad").is_err());
            assert!(archive.exists("escape/outside").is_err());
            assert!(archive.get("escape/outside", 32).is_err());
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum StoreEvent {
        Head(String),
        Get(String, usize),
        Put(String, u64),
        Create(String),
        Upload(i32, u64),
        Complete(Vec<UploadedPart>),
        Abort(String),
    }

    #[derive(Default)]
    struct FakeStore {
        events: Mutex<Vec<StoreEvent>>,
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        fail_part: Mutex<Option<i32>>,
        fail_complete: Mutex<bool>,
        fail_abort: Mutex<bool>,
    }

    impl FakeStore {
        fn events(&self) -> Vec<StoreEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl S3Store for FakeStore {
        async fn ensure_bucket(&self) -> Result<()> {
            Ok(())
        }

        async fn head(&self, key: &str) -> Result<bool> {
            self.events
                .lock()
                .unwrap()
                .push(StoreEvent::Head(key.to_owned()));
            Ok(self.objects.lock().unwrap().contains_key(key))
        }

        async fn get(&self, key: &str, max_bytes: usize) -> Result<Option<Vec<u8>>> {
            self.events
                .lock()
                .unwrap()
                .push(StoreEvent::Get(key.to_owned(), max_bytes));
            let objects = self.objects.lock().unwrap();
            let Some(data) = objects.get(key) else {
                return Ok(None);
            };
            ensure!(data.len() <= max_bytes, "fake object exceeds read limit");
            Ok(Some(data.clone()))
        }

        async fn put(&self, key: &str, body: UploadBody) -> Result<()> {
            let length = body.length();
            self.events
                .lock()
                .unwrap()
                .push(StoreEvent::Put(key.to_owned(), length));
            if let UploadBody::Bytes(data) = body {
                self.objects.lock().unwrap().insert(key.to_owned(), data);
            }
            Ok(())
        }

        async fn create_multipart(&self, key: &str) -> Result<String> {
            self.events
                .lock()
                .unwrap()
                .push(StoreEvent::Create(key.to_owned()));
            Ok("upload-1".to_owned())
        }

        async fn upload_part(
            &self,
            _key: &str,
            _upload_id: &str,
            part_number: i32,
            body: UploadBody,
        ) -> Result<String> {
            self.events
                .lock()
                .unwrap()
                .push(StoreEvent::Upload(part_number, body.length()));
            if *self.fail_part.lock().unwrap() == Some(part_number) {
                bail!("part {part_number} failed");
            }
            Ok(format!("etag-{part_number}"))
        }

        async fn complete(
            &self,
            _key: &str,
            _upload_id: &str,
            parts: Vec<UploadedPart>,
        ) -> Result<()> {
            self.events
                .lock()
                .unwrap()
                .push(StoreEvent::Complete(parts));
            if *self.fail_complete.lock().unwrap() {
                bail!("complete failed");
            }
            Ok(())
        }

        async fn abort(&self, _key: &str, upload_id: &str) -> Result<()> {
            self.events
                .lock()
                .unwrap()
                .push(StoreEvent::Abort(upload_id.to_owned()));
            if *self.fail_abort.lock().unwrap() {
                bail!("abort failed");
            }
            Ok(())
        }
    }

    #[test]
    fn multipart_plan_is_fixed_size_bounded_and_ordered() {
        assert!(multipart_plan(MULTIPART_PART_SIZE).is_err());
        assert_eq!(
            multipart_plan(MULTIPART_PART_SIZE + 1).unwrap(),
            [
                MultipartPart {
                    number: 1,
                    offset: 0,
                    length: MULTIPART_PART_SIZE,
                },
                MultipartPart {
                    number: 2,
                    offset: MULTIPART_PART_SIZE,
                    length: 1,
                },
            ]
        );
        assert!(
            multipart_plan(MAX_MULTIPART_PARTS * MULTIPART_PART_SIZE + 1).is_err(),
            "accepted more than the S3 multipart part limit"
        );
    }

    #[test]
    fn small_and_large_uploads_use_bounded_file_ranges() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let store = FakeStore::default();
        runtime
            .block_on(upload_file(
                &store,
                "small.parquet",
                PathBuf::from("unused"),
                MULTIPART_PART_SIZE,
            ))
            .unwrap();
        assert_eq!(
            store.events(),
            [StoreEvent::Put(
                "small.parquet".to_owned(),
                MULTIPART_PART_SIZE
            )]
        );

        let store = FakeStore::default();
        runtime
            .block_on(upload_file(
                &store,
                "large.parquet",
                PathBuf::from("unused"),
                2 * MULTIPART_PART_SIZE + 7,
            ))
            .unwrap();
        assert_eq!(
            store.events(),
            [
                StoreEvent::Create("large.parquet".to_owned()),
                StoreEvent::Upload(1, MULTIPART_PART_SIZE),
                StoreEvent::Upload(2, MULTIPART_PART_SIZE),
                StoreEvent::Upload(3, 7),
                StoreEvent::Complete(vec![
                    UploadedPart {
                        number: 1,
                        etag: "etag-1".to_owned(),
                    },
                    UploadedPart {
                        number: 2,
                        etag: "etag-2".to_owned(),
                    },
                    UploadedPart {
                        number: 3,
                        etag: "etag-3".to_owned(),
                    },
                ]),
            ]
        );
    }

    #[test]
    fn multipart_failures_always_abort_and_preserve_both_errors() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let store = FakeStore::default();
        *store.fail_part.lock().unwrap() = Some(2);
        let error = runtime
            .block_on(upload_file(
                &store,
                "failed.parquet",
                PathBuf::from("unused"),
                2 * MULTIPART_PART_SIZE,
            ))
            .unwrap_err();
        assert!(format!("{error:#}").contains("part 2 failed"));
        assert_eq!(
            store.events().last(),
            Some(&StoreEvent::Abort("upload-1".to_owned()))
        );
        assert!(
            !store
                .events()
                .iter()
                .any(|event| matches!(event, StoreEvent::Complete(_)))
        );

        let store = FakeStore::default();
        *store.fail_complete.lock().unwrap() = true;
        *store.fail_abort.lock().unwrap() = true;
        let error = runtime
            .block_on(upload_file(
                &store,
                "failed-complete.parquet",
                PathBuf::from("unused"),
                MULTIPART_PART_SIZE + 1,
            ))
            .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("complete failed"));
        assert!(error.contains("abort failed"));
        assert_eq!(
            store.events().last(),
            Some(&StoreEvent::Abort("upload-1".to_owned()))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn r2_archive_runs_sync_calls_on_blocking_workers_with_exact_keys() {
        let store = Arc::new(FakeStore::default());
        let archive = R2Archive::with_store(Handle::current(), store.clone());
        let key = "2026-08-13/04/manifest.json";
        tokio::task::spawn_blocking(move || {
            let mut staged = archive.stage("2026-08-13/04/book.parquet").unwrap();
            staged.write_all(b"parquet").unwrap();
            let staged_path = staged.path().to_path_buf();
            archive
                .commit("2026-08-13/04/book.parquet", staged)
                .unwrap();
            assert!(!staged_path.exists());
            archive.put_bytes(key, b"manifest").unwrap();
            assert!(archive.exists(key).unwrap());
            assert_eq!(archive.get(key, 8).unwrap().unwrap(), b"manifest");
            assert!(archive.get(key, 7).is_err());
            assert_eq!(archive.get("2026-08-13/05/manifest.json", 8).unwrap(), None);
            assert!(archive.exists("../manifest.json").is_err());
        })
        .await
        .unwrap();
        assert_eq!(
            store.events(),
            [
                StoreEvent::Put("2026-08-13/04/book.parquet".to_owned(), 7),
                StoreEvent::Put(key.to_owned(), 8),
                StoreEvent::Head(key.to_owned()),
                StoreEvent::Get(key.to_owned(), 8),
                StoreEvent::Get(key.to_owned(), 7),
                StoreEvent::Get("2026-08-13/05/manifest.json".to_owned(), 8),
            ]
        );
    }
}
