use std::{
    fs::{self, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use penstock_core::CheckpointStore;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Errors produced by [`FileCheckpoint`].
#[derive(Debug, Error)]
pub enum FileCheckpointError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredCheckpoint<C> {
    cursor: C,
}

/// A JSON checkpoint stored in a local file.
#[derive(Debug, Clone)]
pub struct FileCheckpoint {
    path: PathBuf,
}

impl FileCheckpoint {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn temporary_path(&self) -> PathBuf {
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("checkpoint");
        self.path
            .with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), sequence))
    }
}

impl<C> CheckpointStore<C> for FileCheckpoint
where
    C: DeserializeOwned + Serialize + Sync,
{
    type Error = FileCheckpointError;

    async fn load(&self) -> Result<Option<C>, Self::Error> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let stored: StoredCheckpoint<C> = serde_json::from_slice(&bytes)?;
        Ok(Some(stored.cursor))
    }

    async fn save(&self, cursor: &C) -> Result<(), Self::Error> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }

        let body = serde_json::to_vec(&StoredCheckpoint { cursor })?;
        loop {
            let temporary_path = self.temporary_path();
            let mut temporary = match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary_path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            };

            let result = (|| -> Result<(), std::io::Error> {
                temporary.write_all(&body)?;
                temporary.sync_all()?;
                drop(temporary);
                fs::rename(&temporary_path, &self.path)
            })();
            if result.is_err() {
                let _ = fs::remove_file(&temporary_path);
            }
            return result.map_err(Into::into);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Cursor {
        partition: String,
        offset: u64,
    }

    fn cursor(offset: u64) -> Cursor {
        Cursor {
            partition: "events".to_owned(),
            offset,
        }
    }

    #[tokio::test]
    async fn missing_file_loads_as_none() {
        let directory = tempfile::tempdir().unwrap();
        let checkpoint = FileCheckpoint::new(directory.path().join("missing.json"));
        let loaded: Option<Cursor> = checkpoint.load().await.unwrap();
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn creates_parents_round_trips_structured_cursors_and_cleans_up() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/state.json");
        let checkpoint = FileCheckpoint::new(&path);

        checkpoint.save(&cursor(12)).await.unwrap();
        checkpoint.save(&cursor(u64::MAX)).await.unwrap();

        let loaded: Option<Cursor> = checkpoint.load().await.unwrap();
        assert_eq!(loaded, Some(cursor(u64::MAX)));
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn rejects_malformed_and_incompatible_json() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        let checkpoint = FileCheckpoint::new(&path);

        for body in [
            "not json",
            r#"{"cursor":123}"#,
            r#"{"cursor":{"partition":"events"}}"#,
        ] {
            fs::write(&path, body).unwrap();
            let loaded: Result<Option<Cursor>, _> = checkpoint.load().await;
            assert!(matches!(loaded, Err(FileCheckpointError::Json(_))));
        }
    }
}
