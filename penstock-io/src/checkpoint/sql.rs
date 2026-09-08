use penstock_core::CheckpointStore;
use serde::{Serialize, de::DeserializeOwned};
use sqlx_core::{database::Database, pool::Pool};
use thiserror::Error;

const DEFAULT_TABLE: &str = "checkpoints";
#[cfg(feature = "sql-postgres")]
const DEFAULT_POSTGRES_SCHEMA: &str = "penstock";

/// Errors produced by [`SqlCheckpoint`].
#[derive(Debug, Error)]
pub enum SqlCheckpointError {
    #[error("pipeline_id must contain between 1 and 255 bytes")]
    InvalidPipelineId,
    #[error("invalid portable SQL {kind} identifier {identifier:?}")]
    InvalidIdentifier {
        kind: &'static str,
        identifier: String,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Database(#[from] sqlx_core::Error),
}

/// A keyed JSON checkpoint stored in a caller-owned SQLx connection pool.
#[derive(Debug, Clone)]
pub struct SqlCheckpoint<DB: Database> {
    pool: Pool<DB>,
    pipeline_id: String,
    table: String,
    #[cfg(feature = "sql-postgres")]
    schema: String,
}

impl<DB: Database> SqlCheckpoint<DB> {
    /// Construct a checkpoint without performing any DDL.
    pub fn new(pool: Pool<DB>, pipeline_id: impl Into<String>) -> Result<Self, SqlCheckpointError> {
        let pipeline_id = pipeline_id.into();
        if pipeline_id.is_empty() || pipeline_id.len() > 255 {
            return Err(SqlCheckpointError::InvalidPipelineId);
        }
        Ok(Self {
            pool,
            pipeline_id,
            table: DEFAULT_TABLE.to_owned(),
            #[cfg(feature = "sql-postgres")]
            schema: DEFAULT_POSTGRES_SCHEMA.to_owned(),
        })
    }

    /// Override the default `checkpoints` table.
    pub fn table(mut self, table: impl Into<String>) -> Result<Self, SqlCheckpointError> {
        self.table = validated_identifier("table", table.into())?;
        Ok(self)
    }

    pub fn pool(&self) -> &Pool<DB> {
        &self.pool
    }

    pub fn pipeline_id(&self) -> &str {
        &self.pipeline_id
    }
}

fn validated_identifier(
    kind: &'static str,
    identifier: String,
) -> Result<String, SqlCheckpointError> {
    let mut bytes = identifier.bytes();
    let valid_first = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    if valid_first && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        Ok(identifier)
    } else {
        Err(SqlCheckpointError::InvalidIdentifier { kind, identifier })
    }
}

fn encode_cursor<C: Serialize>(cursor: &C) -> Result<String, SqlCheckpointError> {
    serde_json::to_string(cursor).map_err(Into::into)
}

fn decode_cursor<C: DeserializeOwned>(cursor: String) -> Result<C, SqlCheckpointError> {
    serde_json::from_str(&cursor).map_err(Into::into)
}

#[cfg(feature = "sql-postgres")]
impl SqlCheckpoint<sqlx_postgres::Postgres> {
    /// Override the default `penstock` PostgreSQL schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Result<Self, SqlCheckpointError> {
        self.schema = validated_identifier("schema", schema.into())?;
        Ok(self)
    }

    fn qualified_table(&self) -> String {
        format!(r#""{}"."{}""#, self.schema, self.table)
    }

    /// Idempotently create the configured schema and checkpoint table.
    pub async fn initialize(&self) -> Result<(), SqlCheckpointError> {
        sqlx_core::query::query(&format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, self.schema))
            .execute(&self.pool)
            .await?;
        sqlx_core::query::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (\"pipeline_id\" VARCHAR(255) PRIMARY KEY, \"cursor\" TEXT NOT NULL)",
            self.qualified_table()
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Initialize storage eagerly and return this checkpoint.
    pub async fn auto(self) -> Result<Self, SqlCheckpointError> {
        self.initialize().await?;
        Ok(self)
    }
}

#[cfg(feature = "sql-postgres")]
impl<C> CheckpointStore<C> for SqlCheckpoint<sqlx_postgres::Postgres>
where
    C: DeserializeOwned + Serialize + Sync,
{
    type Error = SqlCheckpointError;

    async fn load(&self) -> Result<Option<C>, Self::Error> {
        let cursor: Option<String> = sqlx_core::query_scalar::query_scalar(&format!(
            "SELECT \"cursor\" FROM {} WHERE \"pipeline_id\" = $1",
            self.qualified_table()
        ))
        .bind(&self.pipeline_id)
        .fetch_optional(&self.pool)
        .await?;
        cursor.map(decode_cursor).transpose()
    }

    async fn save(&self, cursor: &C) -> Result<(), Self::Error> {
        let cursor = encode_cursor(cursor)?;
        sqlx_core::query::query(&format!(
            "INSERT INTO {} (\"pipeline_id\", \"cursor\") VALUES ($1, $2) \
             ON CONFLICT (\"pipeline_id\") DO UPDATE SET \"cursor\" = EXCLUDED.\"cursor\"",
            self.qualified_table()
        ))
        .bind(&self.pipeline_id)
        .bind(cursor)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(feature = "sql-mysql")]
impl SqlCheckpoint<sqlx_mysql::MySql> {
    fn quoted_table(&self) -> String {
        format!("`{}`", self.table)
    }

    /// Idempotently create the checkpoint table in the pool-selected database.
    pub async fn initialize(&self) -> Result<(), SqlCheckpointError> {
        sqlx_core::query::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (`pipeline_id` VARCHAR(255) PRIMARY KEY, `cursor` LONGTEXT NOT NULL)",
            self.quoted_table()
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Initialize storage eagerly and return this checkpoint.
    pub async fn auto(self) -> Result<Self, SqlCheckpointError> {
        self.initialize().await?;
        Ok(self)
    }
}

#[cfg(feature = "sql-mysql")]
impl<C> CheckpointStore<C> for SqlCheckpoint<sqlx_mysql::MySql>
where
    C: DeserializeOwned + Serialize + Sync,
{
    type Error = SqlCheckpointError;

    async fn load(&self) -> Result<Option<C>, Self::Error> {
        let cursor: Option<String> = sqlx_core::query_scalar::query_scalar(&format!(
            "SELECT `cursor` FROM {} WHERE `pipeline_id` = ?",
            self.quoted_table()
        ))
        .bind(&self.pipeline_id)
        .fetch_optional(&self.pool)
        .await?;
        cursor.map(decode_cursor).transpose()
    }

    async fn save(&self, cursor: &C) -> Result<(), Self::Error> {
        let cursor = encode_cursor(cursor)?;
        sqlx_core::query::query(&format!(
            "INSERT INTO {} (`pipeline_id`, `cursor`) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE `cursor` = VALUES(`cursor`)",
            self.quoted_table()
        ))
        .bind(&self.pipeline_id)
        .bind(cursor)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(feature = "sql-sqlite")]
impl SqlCheckpoint<sqlx_sqlite::Sqlite> {
    fn quoted_table(&self) -> String {
        format!(r#""{}""#, self.table)
    }

    /// Idempotently create the checkpoint table in the pool-selected database.
    pub async fn initialize(&self) -> Result<(), SqlCheckpointError> {
        sqlx_core::query::query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (\"pipeline_id\" TEXT PRIMARY KEY, \"cursor\" TEXT NOT NULL)",
            self.quoted_table()
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Initialize storage eagerly and return this checkpoint.
    pub async fn auto(self) -> Result<Self, SqlCheckpointError> {
        self.initialize().await?;
        Ok(self)
    }
}

#[cfg(feature = "sql-sqlite")]
impl<C> CheckpointStore<C> for SqlCheckpoint<sqlx_sqlite::Sqlite>
where
    C: DeserializeOwned + Serialize + Sync,
{
    type Error = SqlCheckpointError;

    async fn load(&self) -> Result<Option<C>, Self::Error> {
        let cursor: Option<String> = sqlx_core::query_scalar::query_scalar(&format!(
            "SELECT \"cursor\" FROM {} WHERE \"pipeline_id\" = ?",
            self.quoted_table()
        ))
        .bind(&self.pipeline_id)
        .fetch_optional(&self.pool)
        .await?;
        cursor.map(decode_cursor).transpose()
    }

    async fn save(&self, cursor: &C) -> Result<(), Self::Error> {
        let cursor = encode_cursor(cursor)?;
        sqlx_core::query::query(&format!(
            "INSERT INTO {} (\"pipeline_id\", \"cursor\") VALUES (?, ?) \
             ON CONFLICT (\"pipeline_id\") DO UPDATE SET \"cursor\" = EXCLUDED.\"cursor\"",
            self.quoted_table()
        ))
        .bind(&self.pipeline_id)
        .bind(cursor)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(all(test, feature = "sql-sqlite"))]
mod tests {
    use serde::Deserialize;
    use sqlx_sqlite::SqlitePoolOptions;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Cursor {
        partition: u16,
        offset: String,
    }

    fn cursor(partition: u16, offset: &str) -> Cursor {
        Cursor {
            partition,
            offset: offset.to_owned(),
        }
    }

    async fn pool() -> Pool<sqlx_sqlite::Sqlite> {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn initializes_isolates_ids_and_upserts_structured_cursors() {
        let pool = pool().await;
        let first = SqlCheckpoint::new(pool.clone(), "first")
            .unwrap()
            .auto()
            .await
            .unwrap();
        let second = SqlCheckpoint::new(pool, "second").unwrap();

        let loaded: Option<Cursor> = first.load().await.unwrap();
        assert_eq!(loaded, None);
        first.save(&cursor(1, "start")).await.unwrap();
        first.save(&cursor(1, "finish")).await.unwrap();
        second.save(&cursor(2, "other")).await.unwrap();
        let first_loaded: Option<Cursor> = first.load().await.unwrap();
        let second_loaded: Option<Cursor> = second.load().await.unwrap();
        assert_eq!(first_loaded, Some(cursor(1, "finish")));
        assert_eq!(second_loaded, Some(cursor(2, "other")));
    }

    #[tokio::test]
    async fn custom_table_and_no_implicit_ddl() {
        let pool = pool().await;
        let missing = SqlCheckpoint::new(pool.clone(), "pipeline").unwrap();
        let loaded: Result<Option<Cursor>, _> = missing.load().await;
        assert!(matches!(loaded, Err(SqlCheckpointError::Database(_))));

        let custom = SqlCheckpoint::new(pool, "pipeline")
            .unwrap()
            .table("custom_checkpoints")
            .unwrap()
            .auto()
            .await
            .unwrap();
        custom.save(&cursor(3, "saved")).await.unwrap();
        let loaded: Option<Cursor> = custom.load().await.unwrap();
        assert_eq!(loaded, Some(cursor(3, "saved")));
    }

    #[tokio::test]
    async fn rejects_invalid_configuration_and_stored_cursor() {
        let pool = pool().await;
        assert!(matches!(
            SqlCheckpoint::new(pool.clone(), ""),
            Err(SqlCheckpointError::InvalidPipelineId)
        ));
        assert!(matches!(
            SqlCheckpoint::new(pool.clone(), "pipeline")
                .unwrap()
                .table("not-valid"),
            Err(SqlCheckpointError::InvalidIdentifier { .. })
        ));

        SqlCheckpoint::new(pool.clone(), "pipeline")
            .unwrap()
            .initialize()
            .await
            .unwrap();
        sqlx_core::query::query(
            "INSERT INTO \"checkpoints\" (\"pipeline_id\", \"cursor\") VALUES (?, ?)",
        )
        .bind("bad")
        .bind("not json")
        .execute(&pool)
        .await
        .unwrap();
        let malformed = SqlCheckpoint::new(pool, "bad").unwrap();
        let loaded: Result<Option<Cursor>, _> = malformed.load().await;
        assert!(matches!(loaded, Err(SqlCheckpointError::Json(_))));
    }
}
