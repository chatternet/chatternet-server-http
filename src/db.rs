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
    async fn get_message_audiences(&mut self, message_id: &str) -> Result<Vec<String>>;
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

    async fn get_message_audiences(&mut self, message_id: &str) -> Result<Vec<String>> {
        let query = sqlx::query(
            "\
            SELECT `audience_id` FROM `messagesAudiences` \
            WHERE `message_id` = $1;\
            ",
        )
        .bind(message_id);
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
pub trait TableActorsContacts {
    async fn create_actors_contacts(&mut self) -> Result<()>;
    async fn put_actor_contact(&mut self, actor_id: &str, contact_id: &str) -> Result<()>;
    async fn get_actor_contacts(&mut self, actor_id: &str) -> Result<Vec<String>>;
}

#[async_trait]
impl TableActorsContacts for Connection {
    async fn create_actors_contacts(&mut self) -> Result<()> {
        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `ActorsContacts` \
            (\
                `actor_id` TEXT NOT NULL, \
                `contact_id` TEXT NOT NULL\
            );\
            ",
        )
        .execute(&mut *self)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `actor_id` \
            ON `ActorsContacts`(`actor_id`);\
            ",
        )
        .execute(&mut *self)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `contact_id` \
            ON `ActorsContacts`(`contact_id`);\
            ",
        )
        .execute(&mut *self)
        .await?;

        Ok(())
    }

    async fn put_actor_contact(&mut self, actor_id: &str, contact_id: &str) -> Result<()> {
        sqlx::query(
            "\
            INSERT INTO `ActorsContacts` \
            (`actor_id`, `contact_id`) \
            VALUES($1, $2);\
            ",
        )
        .bind(actor_id)
        .bind(contact_id)
        .execute(&mut *self)
        .await?;
        Ok(())
    }

    async fn get_actor_contacts(&mut self, actor_id: &str) -> Result<Vec<String>> {
        let query = sqlx::query(
            "\
            SELECT `contact_id` FROM `ActorsContacts` \
            WHERE `actor_id` = $1;\
            ",
        )
        .bind(actor_id);
        let mut contacts_id = Vec::new();
        let mut rows = query.fetch(&mut *self);
        while let Some(row) = rows.try_next().await? {
            let contact_id: &str = row.try_get("contact_id")?;
            contacts_id.push(contact_id.to_string());
        }
        Ok(contacts_id)
    }
}

#[async_trait]
pub trait TablesActivityPub:
    TableMessages + TableMessagesAudiences + TableActorsAudiences + TableActorsContacts
{
    async fn get_inbox_for_actor(&mut self, actor_id: &str, count: i64) -> Result<Vec<String>>;
}

#[async_trait]
impl TablesActivityPub for Connection {
    async fn get_inbox_for_actor(&mut self, actor_id: &str, count: i64) -> Result<Vec<String>> {
        let query = sqlx::query(
            "\
            SELECT `message` FROM `Messages` \
            WHERE (\
                `actor_id` = $1
                OR `actor_id` IN (\
                    SELECT `contact_id` FROM `ActorsContacts` \
                    WHERE `ActorsContacts`.`actor_id` = $1
                )\
            )\
            AND `message_id` IN (\
                SELECT `message_id` FROM `MessagesAudiences` \
                WHERE `MessagesAudiences`.`audience_id` = $1
                OR `MessagesAudiences`.`audience_id` IN (\
                    SELECT `audience_id` FROM `ActorsAudiences` \
                    WHERE `ActorsAudiences`.`actor_id` = $1
                )\
            ) \
            ORDER BY `idx` DESC
            LIMIT $2;\
            ",
        )
        .bind(actor_id)
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

#[async_trait]
pub trait TableObjects {
    async fn create_objects(&mut self) -> Result<()>;
    async fn put_or_update_object(&mut self, id: &str, object: Option<&str>) -> Result<()>;
    async fn get_object(&mut self, id: &str) -> Result<Option<String>>;
    async fn has_object(&mut self, id: &str) -> Result<bool>;
}

#[async_trait]
impl TableObjects for Connection {
    async fn create_objects(&mut self) -> Result<()> {
        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `Objects` \
            (\
                `object_id` TEXT PRIMARY KEY, \
                `object` TEXT\
            );\
            ",
        )
        .execute(&mut *self)
        .await?;
        Ok(())
    }

    async fn has_object(&mut self, id: &str) -> Result<bool> {
        let query = sqlx::query(
            "\
            SELECT 1 FROM `Objects` \
            WHERE `object_id` = $1 \
            LIMIT 1;\
            ",
        )
        .bind(id);
        Ok(query.fetch_optional(&mut *self).await?.is_some())
    }

    async fn put_or_update_object(&mut self, id: &str, object: Option<&str>) -> Result<()> {
        // insert if object not yet known
        if !self.has_object(id).await? {
            sqlx::query(
                "\
                INSERT INTO `Objects` \
                (`object_id`, `object`) \
                VALUES($1, $2);\
                ",
            )
            .bind(id)
            .bind(object)
            .execute(&mut *self)
            .await?;
        }
        // update only if there is a value to update
        else if object.is_some() {
            sqlx::query(
                "\
                UPDATE `Objects` \
                SET `object` = $1 \
                WHERE `object_id` = $2\
                ",
            )
            .bind(object)
            .bind(id)
            .execute(&mut *self)
            .await?;
        }
        Ok(())
    }

    async fn get_object(&mut self, id: &str) -> Result<Option<String>> {
        Ok(sqlx::query(
            "\
            SELECT `object` FROM `Objects` \
            WHERE `object_id` = $1;\
            ",
        )
        .bind(id)
        .fetch_one(self)
        .await?
        .get("object"))
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
    connection.create_actors_contacts().await?;
    connection.create_objects().await?;
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
            .put_message("message", "id:1", "did:1/actor")
            .await
            .unwrap();
        connection
            .put_message("message", "id:2", "did:1/actor")
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
            .put_actor_audience("did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        connection
            .put_actor_audience("did:1/actor", "tag:1/followers")
            .await
            .unwrap();
        connection
            .put_actor_audience("did:2/actor", "tag:1/followers")
            .await
            .unwrap();
        assert_eq!(
            connection.get_actor_audiences("did:1/actor").await.unwrap(),
            ["did:2/actor/followers", "tag:1/followers"]
        );
        assert_eq!(
            connection.get_actor_audiences("did:2/actor").await.unwrap(),
            ["tag:1/followers"]
        );
    }

    #[tokio::test]
    async fn puts_and_gets_actor_contacts() {
        let db_pool = new_db_pool("sqlite::memory:").await.unwrap();
        let mut connection = db_pool.acquire().await.unwrap();
        connection
            .put_actor_contact("did:1/actor", "did:2/actor")
            .await
            .unwrap();
        connection
            .put_actor_contact("did:2/actor", "did:1/actor")
            .await
            .unwrap();
        connection
            .put_actor_contact("did:2/actor", "did:3/actor")
            .await
            .unwrap();
        assert_eq!(
            connection.get_actor_contacts("did:1/actor").await.unwrap(),
            ["did:2/actor"]
        );
        assert_eq!(
            connection.get_actor_contacts("did:2/actor").await.unwrap(),
            ["did:1/actor", "did:3/actor"]
        );
    }

    #[tokio::test]
    async fn puts_and_gets_message_audiences() {
        let db_pool = new_db_pool("sqlite::memory:").await.unwrap();
        let mut connection = db_pool.acquire().await.unwrap();
        connection
            .put_message_audience("id:1", "did:2/actor/followers")
            .await
            .unwrap();
        connection
            .put_message_audience("id:1", "tag:1/followers")
            .await
            .unwrap();
        connection
            .put_message_audience("id:2", "tag:1/followers")
            .await
            .unwrap();
        assert_eq!(
            connection.get_message_audiences("id:1").await.unwrap(),
            ["did:2/actor/followers", "tag:1/followers"]
        );
        assert_eq!(
            connection.get_message_audiences("id:2").await.unwrap(),
            ["tag:1/followers"]
        );
    }

    #[tokio::test]
    async fn db_gets_inbox_for_actor() {
        let db_pool = new_db_pool("sqlite::memory:").await.unwrap();
        let mut connection = db_pool.acquire().await.unwrap();

        connection
            .put_message("message 1", "id:1", "did:1/actor")
            .await
            .unwrap();
        connection
            .put_message_audience("id:1", "did:1/actor")
            .await
            .unwrap();
        connection
            .put_message("message 2", "id:2", "did:1/actor")
            .await
            .unwrap();
        connection
            .put_message_audience("id:2", "tag:1/followers")
            .await
            .unwrap();
        connection
            .put_message("message 3", "id:3", "did:2/actor")
            .await
            .unwrap();
        connection
            .put_message_audience("id:3", "tag:1/followers")
            .await
            .unwrap();
        connection
            .put_message("message 4", "id:4", "did:2/actor")
            .await
            .unwrap();
        connection
            .put_message_audience("id:4", "tag:2/followers")
            .await
            .unwrap();

        // did:1 gets messages addressed to self
        assert_eq!(
            connection
                .get_inbox_for_actor("did:1/actor", 1)
                .await
                .unwrap(),
            ["message 1"]
        );

        // did:1 follows tag:1
        connection
            .put_actor_audience("did:1/actor", "tag:1/followers")
            .await
            .unwrap();
        assert_eq!(
            connection
                .get_inbox_for_actor("did:1/actor", 1)
                .await
                .unwrap(),
            ["message 2"]
        );
        // can get more messages
        assert_eq!(
            connection
                .get_inbox_for_actor("did:1/actor", 3)
                .await
                .unwrap(),
            ["message 2", "message 1"]
        );

        // did:1 follows did:2
        connection
            .put_actor_audience("did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        // but not a contact of did:2 so can't get messages
        assert_eq!(
            connection
                .get_inbox_for_actor("did:1/actor", 1)
                .await
                .unwrap(),
            ["message 2"]
        );

        // did:1 adds did:2 as a contact
        connection
            .put_actor_contact("did:1/actor", "did:2/actor")
            .await
            .unwrap();
        assert_eq!(
            connection
                .get_inbox_for_actor("did:1/actor", 3)
                .await
                .unwrap(),
            ["message 3", "message 2", "message 1"]
        );
    }

    #[tokio::test]
    async fn db_puts_and_gets_an_object() {
        let db_pool = new_db_pool("sqlite::memory:").await.unwrap();
        let mut connection = db_pool.acquire().await.unwrap();
        connection.put_or_update_object("id:1", None).await.unwrap();
        assert_eq!(connection.get_object("id:1").await.unwrap(), None);
        connection
            .put_or_update_object("id:1", Some("object"))
            .await
            .unwrap();
        assert_eq!(
            connection.get_object("id:1").await.unwrap(),
            Some("object".to_string())
        );
    }
}
