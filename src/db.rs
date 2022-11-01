/// Serialize to and from text and bytes formats. Leave further casting to
/// the caller. In some instances a serialized format is preferred, so don't
/// force casting into the interface.
use anyhow::Result;
use futures::TryStreamExt;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;

fn build_in_parameters(num_parameters: usize, start: Option<usize>) -> String {
    let start = start.unwrap_or(1);
    (start..start + num_parameters)
        .map(|x| format!("${}", x))
        .collect::<Vec<String>>()
        .join(",")
}

pub struct Db {
    pool: SqlitePool,
}

impl Db {
    pub async fn new(url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new().connect(url).await?;
        let mut conn = pool.acquire().await?;
        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `Activities` \
            (\
                `id` TEXT PRIMARY KEY, \
                `timestamp_micros` BIGINT NOT NULL, \
                `issuer_did` TEXT NOT NULL, \
                `activity` TEXT NOT NULL\
            );\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `timestamp_micros` \
            ON `Activities`(`timestamp_micros`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `issuer_did` \
            ON `Activities`(`issuer_did`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        Ok(Db { pool })
    }

    pub async fn put_activity(
        &self,
        id: &str,
        timestamp_micros: i64,
        issuer_did: &str,
        activity: &str,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query(
            "\
            INSERT INTO `Activities` \
            (`id`, `timestamp_micros`, `issuer_did`, `activity`) \
            VALUES($1, $2, $3, $4)\
            ",
        )
        .bind(id)
        .bind(timestamp_micros)
        .bind(issuer_did)
        .bind(activity)
        .execute(&mut conn)
        .await?;
        Ok(())
    }

    pub async fn get_issuers_activities(
        &self,
        issuers_did: &[impl AsRef<str>],
        start_timestamp_micros: i64,
    ) -> Result<Vec<String>> {
        let mut conn = self.pool.acquire().await?;
        let parameters = build_in_parameters(issuers_did.len(), Some(2));
        let query_str = format!(
            "\
            SELECT `activity` FROM `Activities` \
            WHERE `timestamp_micros` >= $1 AND `issuer_did` IN ({})
            ORDER BY `timestamp_micros`;\
            ",
            parameters
        );
        let mut query = sqlx::query(&query_str);
        query = query.bind(start_timestamp_micros);
        for issuer_did in issuers_did {
            query = query.bind(issuer_did.as_ref());
        }
        let mut activities = Vec::new();
        let mut rows = query.fetch(&mut conn);
        while let Some(row) = rows.try_next().await? {
            let activity: &str = row.try_get("activity")?;
            activities.push(activity.to_string());
        }
        Ok(activities)
    }

    pub async fn filter_has_activities(
        &self,
        ids: &[impl AsRef<str>],
        start_timestamp_micros: i64,
    ) -> Result<Vec<String>> {
        let mut conn = self.pool.acquire().await?;
        let parameters = build_in_parameters(ids.len(), Some(2));
        let query_str = format!(
            "\
            SELECT `id` FROM `Activities` \
            WHERE `timestamp_micros` >= $1 AND `id` IN ({});\
            ",
            parameters
        );
        let mut query = sqlx::query(&query_str);
        query = query.bind(start_timestamp_micros);
        for id in ids {
            query = query.bind(id.as_ref());
        }
        let mut filtered_ids = Vec::new();
        let mut rows = query.fetch(&mut conn);
        while let Some(row) = rows.try_next().await? {
            let id: &str = row.try_get("id")?;
            filtered_ids.push(id.to_string());
        }
        Ok(filtered_ids)
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn db_new_is_ok() {
        let _ = Db::new("sqlite::memory:").await.unwrap();
    }

    #[tokio::test]
    async fn db_puts_activity() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_activity("a:b", 10, "did:a", "abc").await.unwrap();
    }

    #[tokio::test]
    async fn db_gets_issuers_activities() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_activity("a:b", 10, "did:key:a", "a1").await.unwrap();
        db.put_activity("a:c", 11, "did:key:a", "a2").await.unwrap();
        db.put_activity("a:d", 12, "did:key:b", "a3").await.unwrap();
        db.put_activity("a:e", 13, "did:key:c", "a4").await.unwrap();
        let activities = db
            .get_issuers_activities(&["did:key:a", "did:key:b"], 11)
            .await
            .unwrap();
        assert_eq!(activities, ["a2", "a3"]);
    }

    #[tokio::test]
    async fn db_filters_has_activities() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_activity("a:b", 10, "", "").await.unwrap();
        db.put_activity("a:c", 11, "", "").await.unwrap();
        db.put_activity("a:d", 12, "", "").await.unwrap();
        db.put_activity("a:e", 13, "", "").await.unwrap();
        let has = db
            .filter_has_activities(&["a:b", "a:c", "a:d", "a:f"], 11)
            .await
            .unwrap();
        assert_eq!(has, ["a:c", "a:d"]);
    }
}
