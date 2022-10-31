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
                `timestamp_millis` BIGINT NOT NULL\
            );\
            ",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `timestamp_millis` \
            ON `Activities`(`timestamp_millis`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        Ok(Db { pool })
    }

    pub async fn put_activity(&self, id: &str, timestamp_millis: i64) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query(
            "\
            INSERT INTO `Activities` \
            (`id`, `timestamp_millis`) \
            VALUES($1, $2)\
            ",
        )
        .bind(id)
        .bind(timestamp_millis)
        .execute(&mut conn)
        .await?;
        Ok(())
    }

    pub async fn filter_has_activities(
        &self,
        ids: &[impl AsRef<str>],
        since_timestamp_millis: i64,
    ) -> Result<Vec<String>> {
        let mut conn = self.pool.acquire().await?;
        let parameters = build_in_parameters(ids.len(), Some(2));
        let query_str = format!(
            "\
            SELECT `id` FROM `Activities` \
            WHERE `timestamp_millis` > $1 AND `id` IN ({});\
            ",
            parameters
        );
        dbg!(&query_str);
        let mut query = sqlx::query(&query_str);
        query = query.bind(since_timestamp_millis);
        for id in ids {
            dbg!(id.as_ref());
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
        db.put_activity("a:b", 10).await.unwrap();
    }

    #[tokio::test]
    async fn db_filters_has_activities() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_activity("a:b", 10).await.unwrap();
        db.put_activity("a:c", 11).await.unwrap();
        db.put_activity("a:d", 12).await.unwrap();
        db.put_activity("a:e", 13).await.unwrap();
        let has = db
            .filter_has_activities(&["a:b", "a:c", "a:d", "a:f"], 10)
            .await
            .unwrap();
        assert_eq!(has, ["a:c", "a:d"]);
    }
}
