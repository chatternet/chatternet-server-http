use anyhow::{anyhow, Result};
/// Serialize to and from text and bytes formats. Leave further casting to
/// the caller. In some instances a serialized format is preferred, so don't
/// force casting into the interface.
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Acquire, Row, Sqlite, SqlitePool};

pub type Connection = PoolConnection<Sqlite>;

#[async_trait]
pub trait TableMessages {
    async fn create_messages(&mut self) -> Result<()>;
    async fn put_message(&mut self, message: &str, message_id: &str, actor_id: &str) -> Result<()>;
    async fn has_message(&mut self, id: &str) -> Result<bool>;
}

#[async_trait]
impl TableMessages for Connection {
    async fn create_messages(&mut self) -> Result<()> {
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
        .execute(&mut *self)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `message_id` \
            ON `Messages`(`message_id`);\
            ",
        )
        .execute(&mut *self)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `actor_id` \
            ON `Messages`(`actor_id`);\
            ",
        )
        .execute(&mut *self)
        .await?;
        Ok(())
    }

    async fn put_message(&mut self, message: &str, message_id: &str, actor_id: &str) -> Result<()> {
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
        .execute(&mut *self)
        .await?;
        Ok(())
    }

    async fn has_message(&mut self, id: &str) -> Result<bool> {
        let query = sqlx::query(
            "\
            SELECT 1 FROM `Messages` \
            WHERE `message_id` = $1 \
            LIMIT 1;\
            ",
        )
        .bind(id);
        Ok(query.fetch_optional(&mut *self).await?.is_some())
    }
}

#[async_trait]
pub trait TableMessagesAudiences {
    async fn create_messages_audiences(&mut self) -> Result<()>;
    async fn put_message_audience(&mut self, message_id: &str, audience_id: &str) -> Result<()>;
}

#[async_trait]
impl TableMessagesAudiences for Connection {
    async fn create_messages_audiences(&mut self) -> Result<()> {
        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `MessagesAudiences` \
            (\
                `message_id` TEXT NOT NULL, \
                `audience_id` TEXT NOT NULL\
            );\
            ",
        )
        .execute(&mut *self)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `message_id` \
            ON `MessagesAudiences`(`message_id`);\
            ",
        )
        .execute(&mut *self)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `audience_id` \
            ON `MessagesAudiences`(`audience_id`);\
            ",
        )
        .execute(&mut *self)
        .await?;

        Ok(())
    }

    async fn put_message_audience(&mut self, message_id: &str, audience_id: &str) -> Result<()> {
        sqlx::query(
            "\
            INSERT INTO `MessagesAudiences` \
            (`message_id`, `audience_id`) \
            VALUES($1, $2)\
            ",
        )
        .bind(message_id)
        .bind(audience_id)
        .execute(&mut *self)
        .await?;
        Ok(())
    }
}

#[async_trait]
pub trait TableActorsAudiences {
    async fn create_actors_audiences(&mut self) -> Result<()>;
    async fn put_actor_audience(&mut self, actor_id: &str, audience_id: &str) -> Result<()>;
    async fn get_actor_audiences(&mut self, actor_id: &str) -> Result<Vec<String>>;
}

#[async_trait]
impl TableActorsAudiences for Connection {
    async fn create_actors_audiences(&mut self) -> Result<()> {
        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `ActorsAudiences` \
            (\
                `actor_id` TEXT NOT NULL, \
                `audience_id` TEXT NOT NULL\
            );\
            ",
        )
        .execute(&mut *self)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `actor_id` \
            ON `ActorsAudiences`(`actor_id`);\
            ",
        )
        .execute(&mut *self)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `audience_id` \
            ON `ActorsAudiences`(`audience_id`);\
            ",
        )
        .execute(&mut *self)
        .await?;

        Ok(())
    }

    async fn put_actor_audience(&mut self, actor_id: &str, audience_id: &str) -> Result<()> {
        sqlx::query(
            "\
            INSERT INTO `ActorsAudiences` \
            (`actor_id`, `audience_id`) \
            VALUES($1, $2);\
            ",
        )
        .bind(actor_id)
        .bind(audience_id)
        .execute(&mut *self)
        .await?;
        Ok(())
    }

    async fn get_actor_audiences(&mut self, actor_id: &str) -> Result<Vec<String>> {
        let query = sqlx::query(
            "\
            SELECT `audience_id` FROM `ActorsAudiences` \
            WHERE `actor_id` = $1;\
            ",
        )
        .bind(actor_id);
        let mut audiences_id = Vec::new();
        let mut rows = query.fetch(&mut *self);
        while let Some(row) = rows.try_next().await? {
            let audience_id: &str = row.try_get("audience_id")?;
            audiences_id.push(audience_id.to_string());
        }
        Ok(audiences_id)
    }
}

#[async_trait]
pub trait TablesActivityPub: TableMessages + TableMessagesAudiences + TableActorsAudiences {
    async fn get_inbox_for_did(&mut self, did: &str, count: i64) -> Result<Vec<String>>;
}

#[async_trait]
impl TablesActivityPub for Connection {
    async fn get_inbox_for_did(&mut self, did: &str, count: i64) -> Result<Vec<String>> {
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
        let mut rows = query.fetch(&mut *self);
        while let Some(row) = rows.try_next().await? {
            let message: &str = row.try_get("message")?;
            messages.push(message.to_string());
        }
        Ok(messages)
    }
}

pub type DbPool = SqlitePool;

pub async fn new_db_pool(url: &str) -> Result<DbPool> {
    let db_pool = SqlitePoolOptions::new()
        .connect(url)
        .await
        .map_err(|x| anyhow!(x))?;
    let mut connection = db_pool.acquire().await?;
    connection.begin();
    connection.create_messages().await?;
    connection.create_messages_audiences().await?;
    connection.create_actors_audiences().await?;
    Ok(db_pool)
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn new_db_pool_is_ok() {
        let _ = new_db_pool("sqlite::memory:").await.unwrap();
    }

    #[tokio::test]
    async fn puts_and_has_message() {
        let db_pool = new_db_pool("sqlite::memory:").await.unwrap();
        let mut connection = db_pool.acquire().await.unwrap();
        connection
            .put_message("message", "id:1", "did:1")
            .await
            .unwrap();
        connection
            .put_message("message", "id:2", "did:1")
            .await
            .unwrap();
        assert!(connection.has_message("id:1").await.unwrap());
        assert!(connection.has_message("id:2").await.unwrap());
        assert!(!connection.has_message("id:3").await.unwrap());
    }

    #[tokio::test]
    async fn puts_and_gets_actor_audiences() {
        let db_pool = new_db_pool("sqlite::memory:").await.unwrap();
        let mut connection = db_pool.acquire().await.unwrap();
        connection
            .put_actor_audience("did:1", "did:2")
            .await
            .unwrap();
        connection
            .put_actor_audience("did:1", "tag:1")
            .await
            .unwrap();
        connection
            .put_actor_audience("did:2", "tag:1")
            .await
            .unwrap();
        assert_eq!(
            connection.get_actor_audiences("did:1").await.unwrap(),
            ["did:2", "tag:1"]
        );
        assert_eq!(
            connection.get_actor_audiences("did:2").await.unwrap(),
            ["tag:1"]
        );
    }

    #[tokio::test]
    async fn db_gets_inbox_for_did() {
        let db_pool = new_db_pool("sqlite::memory:").await.unwrap();
        let mut connection = db_pool.acquire().await.unwrap();

        connection
            .put_message("message 1", "id:1", "did:1")
            .await
            .unwrap();
        connection
            .put_message_audience("id:1", "did:1")
            .await
            .unwrap();
        connection
            .put_message("message 2", "id:2", "did:1")
            .await
            .unwrap();
        connection
            .put_message_audience("id:2", "tag:1")
            .await
            .unwrap();
        connection
            .put_message("message 3", "id:3", "did:2")
            .await
            .unwrap();
        connection
            .put_message_audience("id:3", "tag:1")
            .await
            .unwrap();
        connection
            .put_message("message 4", "id:4", "did:2")
            .await
            .unwrap();
        connection
            .put_message_audience("id:4", "tag:2")
            .await
            .unwrap();

        // did:1 isn't in any audience yet
        assert!(connection
            .get_inbox_for_did("did:1", 1)
            .await
            .unwrap()
            .is_empty());

        // did:1 follows self
        connection
            .put_actor_audience("did:1", "did:1")
            .await
            .unwrap();
        assert_eq!(
            connection.get_inbox_for_did("did:1", 1).await.unwrap(),
            ["message 1"]
        );

        // did:1 follows tag:1
        connection
            .put_actor_audience("did:1", "tag:1")
            .await
            .unwrap();
        // gets the latest message by self about tag:1
        assert_eq!(
            connection.get_inbox_for_did("did:1", 1).await.unwrap(),
            ["message 2"]
        );
        // can get more messages
        assert_eq!(
            connection.get_inbox_for_did("did:1", 3).await.unwrap(),
            ["message 2", "message 1"]
        );

        // did:1 follows did:2
        connection
            .put_actor_audience("did:1", "did:2")
            .await
            .unwrap();
        // gets the latest message by did:2 about tag:1
        assert_eq!(
            connection.get_inbox_for_did("did:1", 3).await.unwrap(),
            ["message 3", "message 2", "message 1"]
        );
    }
}
