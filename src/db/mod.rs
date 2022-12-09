use std::str::FromStr;

use anyhow::Result;
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, Sqlite, SqliteConnection, SqlitePool};

mod actor_audience;
mod actor_following;
mod message;
mod message_audience;
mod message_body;
mod object;

pub use actor_audience::*;
pub use actor_following::*;
pub use message::*;
pub use message_audience::*;
pub use message_body::*;
pub use object::*;

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

pub async fn get_inbox_for_actor(
    connection: &mut SqliteConnection,
    actor_id: &str,
    count: i64,
    after: Option<&str>,
) -> Result<Vec<String>> {
    let query_str = format!(
        "\
        SELECT `object` FROM `Objects` \
        INNER JOIN `Messages` \
        ON `Objects`.`object_id` = `Messages`.`message_id` \
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
        if after.is_some() {
            "\
            AND `idx` < (\
                SELECT `idx` FROM `Messages` \
                WHERE `message_id` = $3\
            )\
            "
        } else {
            ""
        }
    );
    let query = match after {
        Some(after) => sqlx::query(&query_str)
            .bind(actor_id)
            .bind(count)
            .bind(after),
        None => sqlx::query(&query_str).bind(actor_id).bind(count),
    };
    let mut messages = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let message: &str = row.try_get("object")?;
        messages.push(message.to_string());
    }
    Ok(messages)
}

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
        create_objects(&mut *connection).await?;

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
    async fn db_gets_inbox_for_actor() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();

        put_object(&mut connection, "id:1", "message 1")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:1", "did:1/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:1", "did:1/actor")
            .await
            .unwrap();

        put_object(&mut connection, "id:2", "message 2")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:2", "did:1/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:2", "tag:1/followers")
            .await
            .unwrap();

        put_object(&mut connection, "id:3", "message 3")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:3", "did:2/actor")
            .await
            .unwrap();
        put_message_audience(&mut connection, "id:3", "tag:1/followers")
            .await
            .unwrap();

        put_object(&mut connection, "id:4", "message 4")
            .await
            .unwrap();
        put_message_id(&mut connection, "id:4", "did:2/actor")
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
        put_actor_following(&mut connection, "did:1/actor", "did:2/actor")
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
