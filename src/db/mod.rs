use std::str::FromStr;

use anyhow::Result;
use futures::TryStreamExt;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Acquire, Row, Sqlite, SqliteConnection, SqlitePool};

mod actor_audience;
mod contact;
mod message;
mod message_audience;
mod object;

pub use actor_audience::*;
pub use contact::*;
pub use message::*;
pub use message_audience::*;
pub use object::*;

pub async fn get_inbox_for_actor(
    connection: &mut SqliteConnection,
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

/**
 * Provides connections to a DB instance.
 *
 * Provides mutable and immutable borrow methods to help with book keeping.
 * But both provide a read-write connection to the underlying database.
 */
pub struct Connector {
    pool: SqlitePool,
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
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

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
}
