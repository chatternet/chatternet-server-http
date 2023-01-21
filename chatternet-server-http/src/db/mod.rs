use std::str::FromStr;

use anyhow::Result;
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use sqlx::pool::PoolConnection;
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, Sqlite, SqliteConnection, SqlitePool, Transaction};

mod actor_audience;
mod actor_following;
mod documents;
mod message;
mod message_audience;
mod message_body;
mod mutable_modified;

pub use actor_audience::*;
pub use actor_following::*;
pub use documents::*;
pub use message::*;
pub use message_audience::*;
pub use message_body::*;
pub use mutable_modified::*;

fn joint_id(ids: &[&str]) -> String {
    // IDs are generic, one ID could contain many IDs, so need to use a
    // separator not included in the IDs. JSON does this by encoding the IDs
    // into `"` escaped strings.
    let ids = serde_json::to_string(ids).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(ids.as_bytes());
    let hash = hasher.finalize();
    base64::encode(hash)
}

#[derive(Debug)]
pub struct CollectionPageOut {
    pub items: Vec<String>,
    pub low_idx: u64,
    pub high_idx: u64,
}

async fn build_inbox_messages<'a>(
    query: Query<'a, Sqlite, SqliteArguments<'a>>,
    connection: &mut SqliteConnection,
) -> Result<Option<CollectionPageOut>> {
    let mut messages = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    let mut first_idx: Option<u64> = None;
    let mut last_idx: Option<u64> = None;
    while let Some(row) = rows.try_next().await? {
        let message: &str = row.try_get("document")?;
        let idx: u32 = row.try_get("idx")?;
        messages.push(message.to_string());
        first_idx = first_idx.map(|x| x.min(idx as u64)).or(Some(idx as u64));
        last_idx = last_idx.map(|x| x.max(idx as u64)).or(Some(idx as u64));
    }
    Ok(match (first_idx, last_idx) {
        (Some(first_idx), Some(last_idx)) => Some(CollectionPageOut {
            items: messages,
            low_idx: first_idx,
            high_idx: last_idx,
        }),
        _ => None,
    })
}

pub async fn get_inbox_for_actor(
    connection: &mut SqliteConnection,
    actor_id: &str,
    count: u64,
    start_idx: Option<u64>,
) -> Result<Option<CollectionPageOut>> {
    let query_str = format!(
        "\
        SELECT `idx`, `document` FROM `Documents` \
        INNER JOIN `Messages` \
        ON `Documents`.`document_id` = `Messages`.`message_id` \
        WHERE (\
            `Messages`.`actor_id` = $1
            OR `Messages`.`actor_id` IN (\
                SELECT `following_id` FROM `ActorsFollowings` \
                WHERE `ActorsFollowings`.`actor_id` = $1
            )\
        )\
        AND `Messages`.`message_id` IN (\
            SELECT `message_id` FROM `MessagesAudiences` \
            WHERE `MessagesAudiences`.`audience_id` = $1
            OR `MessagesAudiences`.`audience_id` IN (\
                SELECT `audience_id` FROM `ActorsAudiences` \
                WHERE `ActorsAudiences`.`actor_id` = $1
            )\
        ) \
        {} \
        ORDER BY `idx` DESC \
        LIMIT $2;\
        ",
        if start_idx.is_some() {
            "\
            AND `idx` <= $3 \
            "
        } else {
            ""
        }
    );
    let query = match start_idx {
        Some(start_idx) => sqlx::query(&query_str)
            .bind(actor_id)
            .bind(i64::try_from(count)?)
            .bind(u32::try_from(start_idx)?),
        None => sqlx::query(&query_str)
            .bind(actor_id)
            .bind(i64::try_from(count)?),
    };
    build_inbox_messages(query, connection).await
}

pub async fn get_inbox_from_actor(
    connection: &mut SqliteConnection,
    for_actor_id: &str,
    from_actor_id: &str,
    count: u64,
    start_idx: Option<u64>,
) -> Result<Option<CollectionPageOut>> {
    let query_str = format!(
        "\
        SELECT `idx`, `document` FROM `Documents` \
        INNER JOIN `Messages` \
        ON `Documents`.`document_id` = `Messages`.`message_id` \
        WHERE `Messages`.`actor_id` = $2 \
        AND `Messages`.`message_id` IN (\
            SELECT `message_id` FROM `MessagesAudiences` \
            WHERE `MessagesAudiences`.`audience_id` = $1
            OR `MessagesAudiences`.`audience_id` = $2 || '/followers' \
            OR `MessagesAudiences`.`audience_id` IN (\
                SELECT `audience_id` FROM `ActorsAudiences` \
                WHERE `ActorsAudiences`.`actor_id` = $1
            )\
        ) \
        {} \
        ORDER BY `idx` DESC \
        LIMIT $3;\
        ",
        if start_idx.is_some() {
            "\
            AND `idx` <= $4 \
            "
        } else {
            ""
        }
    );
    let query = match start_idx {
        Some(start_idx) => sqlx::query(&query_str)
            .bind(for_actor_id)
            .bind(from_actor_id)
            .bind(i64::try_from(count)?)
            .bind(u32::try_from(start_idx)?),
        None => sqlx::query(&query_str)
            .bind(for_actor_id)
            .bind(from_actor_id)
            .bind(i64::try_from(count)?),
    };
    build_inbox_messages(query, connection).await
}

pub async fn inbox_contains_message(
    connection: &mut SqliteConnection,
    actor_id: &str,
    message_id: &str,
) -> Result<bool> {
    let query = sqlx::query(
        "\
        SELECT 1 FROM `Messages` \
        WHERE `message_id` = $2 \
        AND ( \
            `Messages`.`actor_id` = $1
            OR `actor_id` IN (\
                SELECT `following_id` FROM `ActorsFollowings` \
                WHERE `ActorsFollowings`.`actor_id` = $1 \
            )
        ) \
        AND `message_id` IN (\
            SELECT `message_id` FROM `MessagesAudiences` \
            WHERE `MessagesAudiences`.`audience_id` = $1
            OR `MessagesAudiences`.`audience_id` IN (\
                SELECT `audience_id` FROM `ActorsAudiences` \
                WHERE `ActorsAudiences`.`actor_id` = $1
            )\
        ) \
        LIMIT 1;\
        ",
    )
    .bind(actor_id)
    .bind(message_id);
    Ok(query.fetch_optional(&mut *connection).await?.is_some())
}

#[derive(Debug)]
pub struct Connector {
    pool_read: Option<SqlitePool>,
    pool_write: SqlitePool,
}

impl Connector {
    pub async fn new(url: &str) -> Result<Self> {
        let pool_write = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::from_str(url)?
                    .create_if_missing(true)
                    .read_only(false),
            )
            .await?;

        let mut connection = pool_write.acquire().await?;
        create_messages(&mut *connection).await?;
        create_messages_audiences(&mut *connection).await?;
        create_messages_bodies(&mut *connection).await?;
        create_actors_audiences(&mut *connection).await?;
        create_actor_following(&mut *connection).await?;
        create_documents(&mut *connection).await?;
        create_mutable_modified(&mut *connection).await?;

        let pool_read = if url == "sqlite::memory:" {
            None
        } else {
            Some(
                SqlitePoolOptions::new()
                    .connect_with(
                        SqliteConnectOptions::from_str(url)?
                            .create_if_missing(false)
                            .read_only(true),
                    )
                    .await?,
            )
        };

        Ok(Connector {
            pool_read,
            pool_write,
        })
    }

    pub async fn connection(&self) -> Result<PoolConnection<Sqlite>> {
        Ok(self
            .pool_read
            .as_ref()
            .unwrap_or(&self.pool_write)
            .acquire()
            .await?)
    }

    pub async fn connection_mut(&mut self) -> Result<PoolConnection<Sqlite>> {
        Ok(self.pool_write.acquire().await?)
    }

    pub async fn transaction(&mut self) -> Result<Transaction<'_, Sqlite>> {
        Ok(self.pool_write.begin().await?)
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[test]
    fn builds_a_joint_id() {
        let id = joint_id(&["a", "b"]);
        // re-calculates the same id
        assert_eq!(id, joint_id(&["a", "b"]));
        // differs from simply joining the IDs
        assert_ne!(id, joint_id(&["ab"]));
    }

    #[tokio::test]
    async fn db_gets_inbox_and_has_message_for_actor() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();

        put_document(&mut connection, "id:1", "message 1")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:1", "did:1/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:1", "did:1/actor")
            .await
            .unwrap();

        put_document(&mut connection, "id:2", "message 2")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:2", "did:1/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:2", "tag:1/followers")
            .await
            .unwrap();

        put_document(&mut connection, "id:3", "message 3")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:3", "did:2/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:3", "tag:1/followers")
            .await
            .unwrap();

        put_document(&mut connection, "id:4", "message 4")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:4", "did:2/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:4", "tag:2/followers")
            .await
            .unwrap();

        // did:1 gets messages addressed to self
        let out = get_inbox_for_actor(&mut connection, "did:1/actor", 3, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["message 1"]);
        assert_eq!(out.low_idx, 1);
        assert_eq!(out.high_idx, 1);

        // did:1 follows tag:1
        put_actor_audience(&mut connection, "did:1/actor", "tag:1/followers")
            .await
            .unwrap();
        let out = get_inbox_for_actor(&mut connection, "did:1/actor", 3, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["message 2", "message 1"]);
        assert_eq!(out.low_idx, 1);
        assert_eq!(out.high_idx, 2);

        // did:1 follows did:2
        put_actor_audience(&mut connection, "did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        let out = get_inbox_for_actor(&mut connection, "did:1/actor", 1, None)
            .await
            .unwrap()
            .unwrap();
        // but not a contact of did:2 so can't get messages
        assert_eq!(out.items, ["message 2"]);
        assert_eq!(out.low_idx, 2);
        assert_eq!(out.high_idx, 2);

        // did:1 adds did:2 as a contact
        put_actor_following(&mut connection, "did:1/actor", "did:2/actor")
            .await
            .unwrap();
        let out = get_inbox_for_actor(&mut connection, "did:1/actor", 3, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["message 3", "message 2", "message 1"]);
        assert_eq!(out.low_idx, 1);
        assert_eq!(out.high_idx, 3);

        // identifies which messages are in the inbox
        assert!(
            inbox_contains_message(&mut connection, "did:1/actor", "id:1")
                .await
                .unwrap()
        );
        assert!(
            inbox_contains_message(&mut connection, "did:1/actor", "id:2")
                .await
                .unwrap()
        );
        assert!(
            inbox_contains_message(&mut connection, "did:1/actor", "id:3")
                .await
                .unwrap()
        );
        assert!(
            !inbox_contains_message(&mut connection, "did:1/actor", "id:4")
                .await
                .unwrap()
        );
        assert!(
            !inbox_contains_message(&mut connection, "did:2/actor", "id:1")
                .await
                .unwrap()
        );

        let out = get_inbox_for_actor(&mut connection, "did:1/actor", 3, Some(2))
            .await
            .unwrap()
            .unwrap();
        // can paginate
        assert_eq!(out.items, ["message 2", "message 1"]);
        assert_eq!(out.low_idx, 1);
        assert_eq!(out.high_idx, 2);
    }

    #[tokio::test]
    async fn db_gets_inbox_from_actor() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();

        put_document(&mut connection, "id:1", "message 1")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:1", "did:1/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:1", "did:1/actor/followers")
            .await
            .unwrap();

        put_document(&mut connection, "id:2", "message 2")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:2", "did:1/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:2", "tag:1/followers")
            .await
            .unwrap();

        let out = get_inbox_from_actor(&mut connection, "did:2/actor", "did:1/actor", 3, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["message 1"]);

        put_actor_audience(&mut connection, "did:2/actor", "tag:1/followers")
            .await
            .unwrap();

        let out = get_inbox_from_actor(&mut connection, "did:2/actor", "did:1/actor", 3, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["message 2", "message 1"]);

        let out = get_inbox_from_actor(&mut connection, "did:2/actor", "did:1/actor", 3, Some(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["message 1"]);
    }
}
