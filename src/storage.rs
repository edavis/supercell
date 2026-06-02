use anyhow::{Context, Result};
use chrono::{prelude::*, Duration};
use sqlx::{Execute, Pool, QueryBuilder, Sqlite};

use model::FeedContent;

pub type StoragePool = Pool<Sqlite>;

pub mod model {
    use chrono::{DateTime, SubsecRound, Utc};
    use sqlx::prelude::*;

    #[derive(Clone, FromRow)]
    pub struct FeedContent {
        pub feed_id: String,
        pub uri: String,
        pub indexed_at: i64,
        pub score: i32,
    }

    impl FeedContent {
        pub(crate) fn age_in_hours(&self, now: i64) -> i64 {
            let target = DateTime::from_timestamp_micros(self.indexed_at)
                .map(|value| value.trunc_subsecs(0).timestamp());
            if target.is_none() {
                return 1;
            }
            let target = target.unwrap();
            let diff_seconds = now - target;
            std::cmp::max((diff_seconds / (60 * 60)) + 1, 1)
        }
    }

    #[derive(Clone, FromRow)]
    pub struct Denylist {
        pub subject: String,
        pub reason: String,
        pub created_at: DateTime<Utc>,
    }
}

pub async fn feed_content_upsert(pool: &StoragePool, feed_content: &FeedContent) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let now = Utc::now();
    let res = sqlx::query("INSERT OR IGNORE INTO feed_content (feed_id, uri, indexed_at, updated_at, score) VALUES (?, ?, ?, ?, ?)")
        .bind(&feed_content.feed_id)
        .bind(&feed_content.uri)
        .bind(feed_content.indexed_at)
        .bind(now)
        .bind(feed_content.score)
        .execute(tx.as_mut())
        .await.context("failed to insert feed content record")?;

    if res.rows_affected() == 0 {
        sqlx::query("UPDATE feed_content SET score = score + ?, updated_at = ? WHERE feed_id = ? AND uri = ?")
            .bind(feed_content.score)
            .bind(now)
            .bind(&feed_content.feed_id)
            .bind(&feed_content.uri)
            .execute(tx.as_mut())
            .await
            .context("failed to update feed content record")?;
    }

    tx.commit().await.context("failed to commit transaction")
}

pub async fn feed_content_update(pool: &StoragePool, feed_content: &FeedContent) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let now = Utc::now();
    sqlx::query(
        "UPDATE feed_content SET score = score + ?, updated_at = ? WHERE feed_id = ? AND uri = ?",
    )
    .bind(feed_content.score)
    .bind(now)
    .bind(&feed_content.feed_id)
    .bind(&feed_content.uri)
    .execute(tx.as_mut())
    .await
    .context("failed to update feed content record")?;

    tx.commit().await.context("failed to commit transaction")
}

pub async fn feed_content_cached(
    pool: &StoragePool,
    feed_uri: &str,
    limit: u32,
) -> Result<Vec<FeedContent>> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let query = "SELECT * FROM feed_content WHERE feed_id = ? ORDER BY indexed_at DESC LIMIT ?";

    let results = sqlx::query_as::<_, FeedContent>(query)
        .bind(feed_uri)
        .bind(limit)
        .fetch_all(tx.as_mut())
        .await?;

    tx.commit().await.context("failed to commit transaction")?;

    Ok(results)
}

pub async fn consumer_control_insert(pool: &StoragePool, source: &str, time_us: i64) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let now = Utc::now();
    sqlx::query(
        "INSERT OR REPLACE INTO consumer_control (source, time_us, updated_at) VALUES (?, ?, ?)",
    )
    .bind(source)
    .bind(time_us)
    .bind(now)
    .execute(tx.as_mut())
    .await?;

    tx.commit().await.context("failed to commit transaction")
}

pub async fn consumer_control_get(pool: &StoragePool, source: &str) -> Result<Option<i64>> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let result =
        sqlx::query_scalar::<_, i64>("SELECT time_us FROM consumer_control WHERE source = ?")
            .bind(source)
            .fetch_optional(tx.as_mut())
            .await
            .context("failed to select consumer control record")?;

    tx.commit().await.context("failed to commit transaction")?;

    Ok(result)
}

pub async fn verifcation_method_insert(
    pool: &StoragePool,
    did: &str,
    multikey: &str,
) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let now = Utc::now();
    sqlx::query(
        "INSERT OR REPLACE INTO verification_method_cache (did, multikey, updated_at) VALUES (?, ?, ?)",
    )
    .bind(did)
    .bind(multikey)
    .bind(now)
    .execute(tx.as_mut())
        .await.context("failed to update verification method cache")?;

    tx.commit().await.context("failed to commit transaction")
}

pub async fn verification_method_cleanup(pool: &StoragePool) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let now = Utc::now();
    let seven_days_ago = now - Duration::days(7);
    sqlx::query("DELETE FROM verification_method_cache WHERE updated_at < ?")
        .bind(seven_days_ago)
        .execute(tx.as_mut())
        .await
        .context("failed to delete old verification method cache records")?;

    tx.commit().await.context("failed to commit transaction")
}

pub async fn verification_method_get(pool: &StoragePool, did: &str) -> Result<Option<String>> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let result = sqlx::query_scalar::<_, String>(
        "SELECT multikey FROM verification_method_cache WHERE did = ?",
    )
    .bind(did)
    .fetch_optional(tx.as_mut())
    .await
    .context("failed to select verification method cache record")?;
    tx.commit().await.context("failed to commit transaction")?;
    Ok(result)
}

pub async fn feed_content_truncate_oldest(pool: &StoragePool, age: DateTime<Utc>) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    // TODO: This might need an index.
    sqlx::query("DELETE FROM feed_content WHERE updated_at < ?")
        .bind(age)
        .execute(tx.as_mut())
        .await
        .context("failed to delete feed content beyond mark")?;

    tx.commit().await.context("failed to commit transaction")
}

pub async fn denylist_upsert(pool: &StoragePool, subject: &str, reason: &str) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let now = Utc::now();
    sqlx::query("INSERT OR REPLACE INTO denylist (subject, reason, updated_at) VALUES (?, ?, ?)")
        .bind(subject)
        .bind(reason)
        .bind(now)
        .execute(tx.as_mut())
        .await
        .context("failed to upsert denylist record")?;

    tx.commit().await.context("failed to commit transaction")
}

pub async fn denylist_remove(pool: &StoragePool, subject: &str) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    sqlx::query("DELETE FROM denylist WHERE subject = ?")
        .bind(subject)
        .execute(tx.as_mut())
        .await
        .context("failed to delete denylist record")?;

    tx.commit().await.context("failed to commit transaction")
}

pub async fn feed_content_purge_aturi(
    pool: &StoragePool,
    aturi: &str,
    feed: &Option<String>,
) -> Result<()> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    if let Some(feed) = feed {
        sqlx::query("DELETE FROM feed_content WHERE feed_id = ? AND uri = ?")
            .bind(feed)
            .bind(aturi)
            .execute(tx.as_mut())
            .await
            .context("failed to delete denylist record")?;
    } else {
        sqlx::query("DELETE FROM feed_content WHERE uri = ?")
            .bind(aturi)
            .execute(tx.as_mut())
            .await
            .context("failed to delete denylist record")?;
    }

    tx.commit().await.context("failed to commit transaction")
}

pub async fn denylist_exists(pool: &StoragePool, subjects: &[&str]) -> Result<bool> {
    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let mut query_builder: QueryBuilder<Sqlite> =
        QueryBuilder::new("SELECT COUNT(*) FROM denylist WHERE subject IN (");
    let mut separated = query_builder.separated(", ");
    for subject in subjects {
        separated.push_bind(subject);
    }
    separated.push_unseparated(") ");

    let mut query = sqlx::query_scalar::<_, i64>(query_builder.build().sql());
    for subject in subjects {
        query = query.bind(subject);
    }
    let count = query
        .fetch_one(tx.as_mut())
        .await
        .context("failed to delete denylist record")?;

    tx.commit().await.context("failed to commit transaction")?;

    Ok(count > 0)
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    #[sqlx::test]
    async fn record_feed_content(pool: SqlitePool) -> sqlx::Result<()> {
        let record = super::model::FeedContent {
            feed_id: "feed".to_string(),
            uri: "at://did:plc:qadlgs4xioohnhi2jg54mqds/app.bsky.feed.post/3la3bqjg4hx2n"
                .to_string(),
            indexed_at: 1730673934229172_i64,
            score: 1,
        };
        super::feed_content_upsert(&pool, &record)
            .await
            .expect("failed to insert record");

        let records = super::feed_content_cached(&pool, "feed", 5)
            .await
            .expect("failed to paginate records");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].feed_id, "feed");
        assert_eq!(
            records[0].uri,
            "at://did:plc:qadlgs4xioohnhi2jg54mqds/app.bsky.feed.post/3la3bqjg4hx2n"
        );
        assert_eq!(records[0].indexed_at, 1730673934229172_i64);

        Ok(())
    }

    #[sqlx::test]
    async fn consumer_control(pool: SqlitePool) -> sqlx::Result<()> {
        super::consumer_control_insert(&pool, "foo", 1730673934229172_i64)
            .await
            .expect("failed to insert record");

        assert_eq!(
            super::consumer_control_get(&pool, "foo")
                .await
                .expect("failed to get record"),
            Some(1730673934229172_i64)
        );

        super::consumer_control_insert(&pool, "foo", 1730673934229173_i64)
            .await
            .expect("failed to insert record");

        assert_eq!(
            super::consumer_control_get(&pool, "foo")
                .await
                .expect("failed to get record"),
            Some(1730673934229173_i64)
        );

        Ok(())
    }
}
