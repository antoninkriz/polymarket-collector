use std::fs::{self, File};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use tempfile::{Builder, NamedTempFile};

pub trait Archive: Send + Sync {
    fn stage(&self, key: &str) -> Result<NamedTempFile>;
    fn commit(&self, key: &str, file: NamedTempFile) -> Result<()>;
    fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()>;
    fn exists(&self, key: &str) -> Result<bool>;
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
        let relative = Path::new(key);
        ensure!(!key.is_empty(), "archive key must not be empty");
        for component in relative.components() {
            if !matches!(component, Component::Normal(_)) {
                bail!("archive key escapes local export directory: {key:?}");
            }
        }
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
        Ok(self.target(key)?.is_file())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

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

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(directory.path(), archive.root().join("escape")).unwrap();
            assert!(archive.put_bytes("escape/outside", b"bad").is_err());
        }
    }
}
