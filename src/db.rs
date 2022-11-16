use std::str::FromStr;

use anyhow::{Error, Result};
/// Serialize to and from text and bytes formats. Leave further casting to
/// the caller. In some instances a serialized format is preferred, so don't
/// force casting into the interface.
use futures::TryStreamExt;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Acquire, Row, Sqlite, SqliteConnection, SqlitePool, Transaction};

pub type Connection = SqliteConnection;
pub type Pool = SqlitePool;

pub async fn create_messages(connection: &mut Connection) -> Result<()> {
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
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `message_id` \
        ON `Messages`(`message_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `actor_id` \
        ON `Messages`(`actor_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn put_message(
    connection: &mut Connection,
    message: &str,
    message_id: &str,
    actor_id: &str,
) -> Result<()> {
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
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn has_message(connection: &mut Connection, id: &str) -> Result<bool> {
    let query = sqlx::query(
        "\
        SELECT 1 FROM `Messages` \
        WHERE `message_id` = $1 \
        LIMIT 1;\
        ",
    )
    .bind(id);
    Ok(query.fetch_optional(&mut *connection).await?.is_some())
}

pub async fn create_messages_audiences(connection: &mut Connection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `MessagesAudiences` \
        (\
            `message_id` TEXT NOT NULL, \
            `audience_id` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `message_id` \
        ON `MessagesAudiences`(`message_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `audience_id` \
        ON `MessagesAudiences`(`audience_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn put_message_audience(
    connection: &mut Connection,
    message_id: &str,
    audience_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT INTO `MessagesAudiences` \
        (`message_id`, `audience_id`) \
        VALUES($1, $2)\
        ",
    )
    .bind(message_id)
    .bind(audience_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_message_audiences(
    connection: &mut Connection,
    message_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `audience_id` FROM `messagesAudiences` \
        WHERE `message_id` = $1;\
        ",
    )
    .bind(message_id);
    let mut audiences_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let audience_id: &str = row.try_get("audience_id")?;
        audiences_id.push(audience_id.to_string());
    }
    Ok(audiences_id)
}

pub async fn create_actors_audiences(connection: &mut Connection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `ActorsAudiences` \
        (\
            `actor_id` TEXT NOT NULL, \
            `audience_id` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `actor_id` \
        ON `ActorsAudiences`(`actor_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `audience_id` \
        ON `ActorsAudiences`(`audience_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn put_actor_audience(
    connection: &mut Connection,
    actor_id: &str,
    audience_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT INTO `ActorsAudiences` \
        (`actor_id`, `audience_id`) \
        VALUES($1, $2);\
        ",
    )
    .bind(actor_id)
    .bind(audience_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_actor_audiences(
    connection: &mut Connection,
    actor_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `audience_id` FROM `ActorsAudiences` \
        WHERE `actor_id` = $1;\
        ",
    )
    .bind(actor_id);
    let mut audiences_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let audience_id: &str = row.try_get("audience_id")?;
        audiences_id.push(audience_id.to_string());
    }
    Ok(audiences_id)
}

pub async fn create_actors_contacts(connection: &mut Connection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `ActorsContacts` \
        (\
            `actor_id` TEXT NOT NULL, \
            `contact_id` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `actor_id` \
        ON `ActorsContacts`(`actor_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `contact_id` \
        ON `ActorsContacts`(`contact_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn put_actor_contact(
    connection: &mut Connection,
    actor_id: &str,
    contact_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT INTO `ActorsContacts` \
        (`actor_id`, `contact_id`) \
        VALUES($1, $2);\
        ",
    )
    .bind(actor_id)
    .bind(contact_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_actor_contacts(
    connection: &mut Connection,
    actor_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `contact_id` FROM `ActorsContacts` \
        WHERE `actor_id` = $1;\
        ",
    )
    .bind(actor_id);
    let mut contacts_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let contact_id: &str = row.try_get("contact_id")?;
        contacts_id.push(contact_id.to_string());
    }
    Ok(contacts_id)
}

pub async fn get_inbox_for_actor(
    connection: &mut Connection,
    actor_id: &str,
    count: i64,
    after: Option<&str>,
) -> Result<Vec<String>> {
    let query_str_1: &'static str = "\
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
        ";
    let query_str_2: &'static str = "\
        ORDER BY `idx` DESC
        LIMIT $2;\
        ";
    let query_str_3: &'static str = "\
        AND `idx` < (\
            SELECT `idx` FROM `Messages` \
            WHERE `message_id` = $3\
        ) \
        ";
    let query_str_1_2: String = format!("{}{}", query_str_1, query_str_2);
    let query_str_1_3_2: String = format!("{}{}{}", query_str_1, query_str_3, query_str_2);
    let query = match after {
        Some(after) => sqlx::query(&query_str_1_3_2)
            .bind(actor_id)
            .bind(count)
            .bind(after),
        None => sqlx::query(&query_str_1_2).bind(actor_id).bind(count),
    };
    let mut messages = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let message: &str = row.try_get("message")?;
        messages.push(message.to_string());
    }
    Ok(messages)
}

pub async fn create_objects(connection: &mut Connection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `Objects` \
        (\
            `object_id` TEXT PRIMARY KEY, \
            `object` TEXT\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn has_object(connection: &mut Connection, id: &str) -> Result<bool> {
    let query = sqlx::query(
        "\
        SELECT 1 FROM `Objects` \
        WHERE `object_id` = $1 \
        LIMIT 1;\
        ",
    )
    .bind(id);
    Ok(query.fetch_optional(&mut *connection).await?.is_some())
}

pub async fn put_or_update_object(
    connection: &mut Connection,
    object_id: &str,
    object: Option<&str>,
) -> Result<()> {
    let has_object = has_object(&mut *connection, object_id).await?;
    // insert if object not yet known
    if !has_object {
        sqlx::query(
            "\
            INSERT INTO `Objects` \
            (`object_id`, `object`) \
            VALUES($1, $2);\
            ",
        )
        .bind(object_id)
        .bind(object)
        .execute(&mut *connection)
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
        .bind(object_id)
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

pub async fn get_object(connection: &mut Connection, object_id: &str) -> Result<Option<String>> {
    Ok(sqlx::query(
        "\
        SELECT `object` FROM `Objects` \
        WHERE `object_id` = $1;\
        ",
    )
    .bind(object_id)
    .fetch_one(connection)
    .await?
    .get("object"))
}

/**
 * Provides connections to a DB instance.
 * 
 * Provides mutable and immutable borrow methods to help with book keeping.
 * But both provide a read-write connection to the underlying database.
 */
pub struct Connector {
    pool: Pool,
}

impl Connector {
    pub async fn new(url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .connect_with(SqliteConnectOptions::from_str(url)?.read_only(false))
            .await?;

        let mut transaction = pool.begin().await?;
        let connection = transaction.acquire().await?;
        create_messages(connection).await?;
        create_messages_audiences(connection).await?;
        create_actors_audiences(connection).await?;
        create_actors_contacts(connection).await?;
        create_objects(connection).await?;
        transaction.commit().await?;

        Ok(Connector { pool })
    }

    pub async fn connection(&self) -> Result<PoolConnection<Sqlite>> {
        Ok(self.pool.acquire().await?)
    }

    pub async fn connection_mut(&mut self) -> Result<PoolConnection<Sqlite>> {
        Ok(self.pool.acquire().await?)
    }

    pub async fn transaction_mut(&mut self) -> Result<Transaction<Sqlite>> {
        self.pool.begin().await.map_err(Error::new)
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn puts_and_has_message() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_message(&mut connection, "message", "id:1", "did:1/actor")
            .await
            .unwrap();
        put_message(&mut connection, "message", "id:2", "did:1/actor")
            .await
            .unwrap();
        assert!(has_message(&mut connection, "id:1").await.unwrap());
        assert!(has_message(&mut connection, "id:2").await.unwrap());
        assert!(!has_message(&mut connection, "id:3").await.unwrap());
    }

    #[tokio::test]
    async fn puts_and_gets_actor_audiences() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_audience(&mut connection, "did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        put_actor_audience(&mut connection, "did:1/actor", "tag:1/followers")
            .await
            .unwrap();
        put_actor_audience(&mut connection, "did:2/actor", "tag:1/followers")
            .await
            .unwrap();
        assert_eq!(
            get_actor_audiences(&mut connection, "did:1/actor")
                .await
                .unwrap(),
            ["did:2/actor/followers", "tag:1/followers"]
        );
        assert_eq!(
            get_actor_audiences(&mut connection, "did:2/actor")
                .await
                .unwrap(),
            ["tag:1/followers"]
        );
    }

    #[tokio::test]
    async fn puts_and_gets_actor_contacts() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_contact(&mut connection, "did:1/actor", "did:2/actor")
            .await
            .unwrap();
        put_actor_contact(&mut connection, "did:2/actor", "did:1/actor")
            .await
            .unwrap();
        put_actor_contact(&mut connection, "did:2/actor", "did:3/actor")
            .await
            .unwrap();
        assert_eq!(
            get_actor_contacts(&mut connection, "did:1/actor")
                .await
                .unwrap(),
            ["did:2/actor"]
        );
        assert_eq!(
            get_actor_contacts(&mut connection, "did:2/actor")
                .await
                .unwrap(),
            ["did:1/actor", "did:3/actor"]
        );
    }

    #[tokio::test]
    async fn puts_and_gets_message_audiences() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_message_audience(&mut connection, "id:1", "did:2/actor/followers")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:1", "tag:1/followers")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:2", "tag:1/followers")
            .await
            .unwrap();
        assert_eq!(
            get_message_audiences(&mut connection, "id:1")
                .await
                .unwrap(),
            ["did:2/actor/followers", "tag:1/followers"]
        );
        assert_eq!(
            get_message_audiences(&mut connection, "id:2")
                .await
                .unwrap(),
            ["tag:1/followers"]
        );
    }

    #[tokio::test]
    async fn db_gets_inbox_for_actor() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();

        put_message(&mut connection, "message 1", "id:1", "did:1/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:1", "did:1/actor")
            .await
            .unwrap();
        put_message(&mut connection, "message 2", "id:2", "did:1/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:2", "tag:1/followers")
            .await
            .unwrap();
        put_message(&mut connection, "message 3", "id:3", "did:2/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:3", "tag:1/followers")
            .await
            .unwrap();
        put_message(&mut connection, "message 4", "id:4", "did:2/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:4", "tag:2/followers")
            .await
            .unwrap();

        // did:1 gets messages addressed to self
        assert_eq!(
            get_inbox_for_actor(&mut connection, "did:1/actor", 3, None)
                .await
                .unwrap(),
            ["message 1"]
        );

        // did:1 follows tag:1
        put_actor_audience(&mut connection, "did:1/actor", "tag:1/followers")
            .await
            .unwrap();
        assert_eq!(
            get_inbox_for_actor(&mut connection, "did:1/actor", 3, None)
                .await
                .unwrap(),
            ["message 2", "message 1"]
        );

        // did:1 follows did:2
        put_actor_audience(&mut connection, "did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        // but not a contact of did:2 so can't get messages
        assert_eq!(
            get_inbox_for_actor(&mut connection, "did:1/actor", 1, None)
                .await
                .unwrap(),
            ["message 2"]
        );

        // did:1 adds did:2 as a contact
        put_actor_contact(&mut connection, "did:1/actor", "did:2/actor")
            .await
            .unwrap();
        assert_eq!(
            get_inbox_for_actor(&mut connection, "did:1/actor", 3, None)
                .await
                .unwrap(),
            ["message 3", "message 2", "message 1"]
        );

        // can paginate
        assert_eq!(
            get_inbox_for_actor(&mut connection, "did:1/actor", 3, Some("id:3"))
                .await
                .unwrap(),
            ["message 2", "message 1"]
        );

        // can paginate empty
        assert!(
            get_inbox_for_actor(&mut connection, "did:1/actor", 3, Some("id:1"))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn db_puts_and_gets_an_object() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_or_update_object(&mut connection, "id:1", None)
            .await
            .unwrap();
        assert_eq!(get_object(&mut connection, "id:1").await.unwrap(), None);
        put_or_update_object(&mut connection, "id:1", Some("object"))
            .await
            .unwrap();
        assert_eq!(
            get_object(&mut connection, "id:1").await.unwrap(),
            Some("object".to_string())
        );
    }
}
