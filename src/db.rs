/// Serialize to and from text and bytes formats. Leave further casting to
/// the caller. In some instances a serialized format is preferred, so don't
/// force casting into the interface.
use anyhow::Result;
use futures::TryStreamExt;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;

pub struct Db {
    pool: SqlitePool,
}

pub const UNADDRESSED: Option<&[&str]> = None;

impl Db {
    pub async fn new(url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new().connect(url).await?;
        let mut conn = pool.acquire().await?;

        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `Messages` \
            (\
                `idx` INTEGER PRIMARY KEY AUTOINCREMENT,
                `message` TEXT NOT NULL, \
                `message_id` TEXT NOT NULL, \
                `actor_id` TEXT NOT NULL\
            );\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `message_id` \
            ON `Messages`(`message_id`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `actor_id` \
            ON `Messages`(`actor_id`);\
            ",
        )
        .execute(&mut conn)
        .await?;

        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `MessagesAudiences` \
            (\
                `message_id` TEXT NOT NULL, \
                `audience_id` TEXT NOT NULL\
            );\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `message_id` \
            ON `MessagesAudiences`(`message_id`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `audience_id` \
            ON `MessagesAudiences`(`audience_id`);\
            ",
        )
        .execute(&mut conn)
        .await?;

        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `ActorsAudiences` \
            (\
                `actor_id` TEXT NOT NULL, \
                `audience_id` TEXT NOT NULL\
            );\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `actor_id` \
            ON `ActorsAudiences`(`actor_id`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `audience_id` \
            ON `ActorsAudiences`(`audience_id`);\
            ",
        )
        .execute(&mut conn)
        .await?;

        Ok(Db { pool })
    }

    pub async fn put_message(
        &self,
        message: &str,
        message_id: &str,
        actor_id: &str,
        audiences_id: &[impl AsRef<str>],
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query(
            "\
            INSERT INTO `Messages` \
            (`message`, `message_id`, `actor_id`) \
            VALUES($1, $2, $3)\
            ",
        )
        .bind(message)
        .bind(message_id)
        .bind(actor_id)
        .execute(&mut conn)
        .await?;
        for audience_id in audiences_id {
            sqlx::query(
                "\
                INSERT INTO `MessagesAudiences` \
                (`message_id`, `audience_id`) \
                VALUES($1, $2);\
                ",
            )
            .bind(message_id)
            .bind(audience_id.as_ref())
            .execute(&mut conn)
            .await?;
        }
        Ok(())
    }

    pub async fn put_audience(&self, actor_id: &str, audience_id: &str) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query(
            "\
            INSERT INTO `ActorsAudiences` \
            (`actor_id`, `audience_id`) \
            VALUES($1, $2)\
            ",
        )
        .bind(actor_id)
        .bind(audience_id)
        .execute(&mut conn)
        .await?;
        Ok(())
    }

    pub async fn has_message(&self, id: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        let query = sqlx::query(
            "\
            SELECT 1 FROM `Messages` \
            WHERE `message_id` = $1 \
            LIMIT 1;\
            ",
        )
        .bind(id);
        Ok(query.fetch_optional(&mut conn).await?.is_some())
    }

    pub async fn get_inbox_for_did(&self, did: &str, count: i64) -> Result<Vec<String>> {
        let mut conn = self.pool.acquire().await?;
        let query = sqlx::query(
            "\
            SELECT `message` FROM `Messages` \
            WHERE `actor_id` IN (\
                SELECT `audience_id` FROM `ActorsAudiences` \
                WHERE `actor_id` = $1
            ) \
            AND `message_id` IN (\
                SELECT `message_id` FROM `MessagesAudiences` \
                WHERE `MessagesAudiences`.`audience_id` IN (\
                    SELECT `audience_id` FROM `ActorsAudiences` \
                    WHERE `actor_id` = $1
                )\
            ) \
            ORDER BY `idx` DESC
            LIMIT $2;\
            ",
        )
        .bind(did)
        .bind(count);
        let mut messages = Vec::new();
        let mut rows = query.fetch(&mut conn);
        while let Some(row) = rows.try_next().await? {
            let message: &str = row.try_get("message")?;
            messages.push(message.to_string());
        }
        Ok(messages)
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
    async fn db_puts_message() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_message("message", "id:1", "did:1", &["did:1"])
            .await
            .unwrap();
        db.put_message("message", "id:2", "did:1", &["tag:1"])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn db_checks_has_message() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_message("message", "id:1", "did:1", &["did:1"])
            .await
            .unwrap();
        db.put_message("message", "id:2", "did:1", &["tag:1"])
            .await
            .unwrap();
        assert!(db.has_message("id:1").await.unwrap());
        assert!(db.has_message("id:2").await.unwrap());
        assert!(!db.has_message("id:3").await.unwrap());
    }

    #[tokio::test]
    async fn db_puts_follows() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_audience("did:1", "did:2").await.unwrap();
        db.put_audience("did:2", "tag:1").await.unwrap();
    }

    #[tokio::test]
    async fn db_gets_inbox_for_did() {
        let db = Db::new("sqlite::memory:").await.unwrap();

        db.put_message("message 1", "id:1", "did:1", &["did:1"])
            .await
            .unwrap();
        db.put_message("message 2", "id:2", "did:1", &["tag:1"])
            .await
            .unwrap();
        db.put_message("message 3", "id:3", "did:2", &["tag:1"])
            .await
            .unwrap();
        db.put_message("message 4", "id:4", "did:2", &["tag:2"])
            .await
            .unwrap();

        // did:1 isn't in any audience yet
        assert!(db.get_inbox_for_did("did:1", 1).await.unwrap().is_empty());

        // did:1 follows self
        db.put_audience("did:1", "did:1").await.unwrap();
        assert_eq!(
            db.get_inbox_for_did("did:1", 1).await.unwrap(),
            ["message 1"]
        );

        // did:1 follows tag:1
        db.put_audience("did:1", "tag:1").await.unwrap();
        // gets the latest message by self about tag:1
        assert_eq!(
            db.get_inbox_for_did("did:1", 1).await.unwrap(),
            ["message 2"]
        );
        // can get more messages
        assert_eq!(
            db.get_inbox_for_did("did:1", 3).await.unwrap(),
            ["message 2", "message 1"]
        );

        // did:1 follows did:2
        db.put_audience("did:1", "did:2").await.unwrap();
        // gets the latest message by did:2 about tag:1
        assert_eq!(
            db.get_inbox_for_did("did:1", 3).await.unwrap(),
            ["message 3", "message 2", "message 1"]
        );
    }
}
