use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Row, SqliteConnection};

use super::joint_id;

pub async fn create_messages_audiences(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `MessagesAudiences` \
        (\
            `joint_id` TEXT PRIMARY KEY, \
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
    connection: &mut SqliteConnection,
    message_id: &str,
    audience_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT OR IGNORE INTO `MessagesAudiences` \
        (`joint_id`, `message_id`, `audience_id`) \
        VALUES($1, $2, $3)\
        ",
    )
    .bind(joint_id(&[message_id, audience_id]))
    .bind(message_id)
    .bind(audience_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_message_audiences(
    connection: &mut SqliteConnection,
    message_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `audience_id` FROM `MessagesAudiences` \
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

pub async fn delete_message_audiences(
    connection: &mut SqliteConnection,
    message_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        DELETE FROM `MessagesAudiences` \
        WHERE `message_id` = $1;\
        ",
    )
    .bind(message_id)
    .execute(connection)
    .await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
    use super::*;

    #[tokio::test]
    async fn puts_gets_deletes_message_audiences() {
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
        delete_message_audiences(&mut connection, "id:1")
            .await
            .unwrap();
        assert!(get_message_audiences(&mut connection, "id:1")
            .await
            .unwrap()
            .is_empty());
    }
}
