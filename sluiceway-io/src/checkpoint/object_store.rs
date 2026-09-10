use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, path::Path};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sluiceway_core::CheckpointStore;
use thiserror::Error;

/// Errors produced by [`ObjectStoreCheckpoint`].
#[derive(Debug, Error)]
pub enum ObjectStoreCheckpointError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredCheckpoint<C> {
    cursor: C,
}

/// A JSON checkpoint stored in a caller-configured object store.
#[derive(Debug, Clone)]
pub struct ObjectStoreCheckpoint {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ObjectStoreCheckpoint {
    pub fn new(store: Arc<dyn ObjectStore>, path: impl Into<Path>) -> Self {
        Self {
            store,
            path: path.into(),
        }
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<Cp> CheckpointStore<Cp> for ObjectStoreCheckpoint
where
    Cp: DeserializeOwned + Serialize + Sync,
{
    type Error = ObjectStoreCheckpointError;

    async fn load(&self) -> Result<Option<Cp>, Self::Error> {
        let bytes = match self.store.get(&self.path).await {
            Ok(result) => result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let stored: StoredCheckpoint<Cp> = serde_json::from_slice(&bytes)?;
        Ok(Some(stored.cursor))
    }

    async fn save(&self, checkpoint: &Cp) -> Result<(), Self::Error> {
        let body = serde_json::to_vec(&StoredCheckpoint { cursor: checkpoint })?;
        self.store.put(&self.path, PutPayload::from(body)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Cursor {
        shard: String,
        token: Vec<u8>,
    }

    fn cursor(token: &[u8]) -> Cursor {
        Cursor {
            shard: "west".to_owned(),
            token: token.to_vec(),
        }
    }

    #[tokio::test]
    async fn missing_round_trip_overwrite_and_shared_arc() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("state.json");
        let first = ObjectStoreCheckpoint::new(store.clone(), path.clone());
        let second = ObjectStoreCheckpoint::new(store.clone(), path);

        let loaded: Option<Cursor> = first.load().await.unwrap();
        assert_eq!(loaded, None);
        first.save(&cursor(&[1, 2])).await.unwrap();
        second.save(&cursor(&[3, 4])).await.unwrap();
        let loaded: Option<Cursor> = first.load().await.unwrap();
        assert_eq!(loaded, Some(cursor(&[3, 4])));
    }

    #[tokio::test]
    async fn rejects_incompatible_payloads() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("state.json");
        let checkpoint = ObjectStoreCheckpoint::new(store.clone(), path.clone());

        store
            .put(&path, PutPayload::from_static(br#"{"cursor":123}"#))
            .await
            .unwrap();
        let loaded: Result<Option<Cursor>, _> = checkpoint.load().await;
        assert!(matches!(loaded, Err(ObjectStoreCheckpointError::Json(_))));
    }
}
