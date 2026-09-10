#![cfg(any(feature = "sql-postgres", feature = "sql-mysql"))]

use serde::{Deserialize, Serialize};
use sluiceway_core::CheckpointStore as _;
use sluiceway_io::{SqlCheckpoint, SqlCheckpointError};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Cursor {
    partition: String,
    offset: u64,
}

fn cursor(partition: &str, offset: u64) -> Cursor {
    Cursor {
        partition: partition.to_owned(),
        offset,
    }
}

#[cfg(feature = "sql-postgres")]
#[tokio::test]
#[ignore = "requires SLUICEWAY_TEST_POSTGRES_URL"]
async fn postgres_checkpoint_contract() {
    let url = std::env::var("SLUICEWAY_TEST_POSTGRES_URL")
        .expect("SLUICEWAY_TEST_POSTGRES_URL must be set");
    let pool = sqlx_postgres::PgPoolOptions::new()
        .connect(&url)
        .await
        .unwrap();

    let default = SqlCheckpoint::new(pool.clone(), "default-contract")
        .unwrap()
        .auto()
        .await
        .unwrap();
    let custom = SqlCheckpoint::new(pool.clone(), "custom-contract")
        .unwrap()
        .schema("sluiceway_integration")
        .unwrap()
        .table("custom_checkpoints")
        .unwrap();
    let (left, right) = tokio::join!(custom.initialize(), custom.initialize());
    left.unwrap();
    right.unwrap();

    sqlx_core::query::query(
        "DELETE FROM \"sluiceway\".\"checkpoints\" WHERE \"pipeline_id\" LIKE '%-contract'",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx_core::query::query(
        "DELETE FROM \"sluiceway_integration\".\"custom_checkpoints\" \
         WHERE \"pipeline_id\" LIKE '%-contract'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let missing: Option<Cursor> = default.load().await.unwrap();
    assert_eq!(missing, None);
    default.save(&cursor("primary", 1)).await.unwrap();
    default.save(&cursor("primary", u64::MAX)).await.unwrap();
    custom.save(&cursor("secondary", 42)).await.unwrap();
    let default_loaded: Option<Cursor> = default.load().await.unwrap();
    let custom_loaded: Option<Cursor> = custom.load().await.unwrap();
    assert_eq!(default_loaded, Some(cursor("primary", u64::MAX)));
    assert_eq!(custom_loaded, Some(cursor("secondary", 42)));

    let data_type: String = sqlx_core::query_scalar::query_scalar(
        "SELECT data_type FROM information_schema.columns \
         WHERE table_schema = 'sluiceway' AND table_name = 'checkpoints' AND column_name = 'cursor'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(data_type, "text");

    sqlx_core::query::query(
        "INSERT INTO \"sluiceway\".\"checkpoints\" (\"pipeline_id\", \"cursor\") \
         VALUES ($1, $2) ON CONFLICT (\"pipeline_id\") \
         DO UPDATE SET \"cursor\" = EXCLUDED.\"cursor\"",
    )
    .bind("invalid-contract")
    .bind("not json")
    .execute(&pool)
    .await
    .unwrap();
    let invalid = SqlCheckpoint::new(pool, "invalid-contract").unwrap();
    let loaded: Result<Option<Cursor>, _> = invalid.load().await;
    assert!(matches!(loaded, Err(SqlCheckpointError::Json(_))));
}

#[cfg(feature = "sql-mysql")]
#[tokio::test]
#[ignore = "requires SLUICEWAY_TEST_MYSQL_URL"]
async fn mysql_checkpoint_contract() {
    let url =
        std::env::var("SLUICEWAY_TEST_MYSQL_URL").expect("SLUICEWAY_TEST_MYSQL_URL must be set");
    let pool = sqlx_mysql::MySqlPoolOptions::new()
        .connect(&url)
        .await
        .unwrap();

    let default = SqlCheckpoint::new(pool.clone(), "default-contract")
        .unwrap()
        .auto()
        .await
        .unwrap();
    let custom = SqlCheckpoint::new(pool.clone(), "custom-contract")
        .unwrap()
        .table("custom_checkpoints")
        .unwrap();
    let (left, right) = tokio::join!(custom.initialize(), custom.initialize());
    left.unwrap();
    right.unwrap();

    sqlx_core::query::query("DELETE FROM `checkpoints` WHERE `pipeline_id` LIKE '%-contract'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx_core::query::query(
        "DELETE FROM `custom_checkpoints` WHERE `pipeline_id` LIKE '%-contract'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let missing: Option<Cursor> = default.load().await.unwrap();
    assert_eq!(missing, None);
    default.save(&cursor("primary", 1)).await.unwrap();
    default.save(&cursor("primary", u64::MAX)).await.unwrap();
    custom.save(&cursor("secondary", 42)).await.unwrap();
    let default_loaded: Option<Cursor> = default.load().await.unwrap();
    let custom_loaded: Option<Cursor> = custom.load().await.unwrap();
    assert_eq!(default_loaded, Some(cursor("primary", u64::MAX)));
    assert_eq!(custom_loaded, Some(cursor("secondary", 42)));

    sqlx_core::query::query(
        "INSERT INTO `checkpoints` (`pipeline_id`, `cursor`) VALUES (?, ?) \
         ON DUPLICATE KEY UPDATE `cursor` = VALUES(`cursor`)",
    )
    .bind("invalid-contract")
    .bind("not json")
    .execute(&pool)
    .await
    .unwrap();
    let invalid = SqlCheckpoint::new(pool, "invalid-contract").unwrap();
    let loaded: Result<Option<Cursor>, _> = invalid.load().await;
    assert!(matches!(loaded, Err(SqlCheckpointError::Json(_))));
}
