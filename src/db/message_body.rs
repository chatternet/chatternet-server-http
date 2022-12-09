use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Row, SqliteConnection};

use super::joint_id;

pub async fn create_messages_bodies(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `MessagesBodies` \
        (\
            `joint_id` TEXT PRIMARY KEY, \
            `message_id` TEXT NOT NULL, \
            `body_id` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `message_id` \
        ON `MessagesBodies`(`message_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `body_id` \
        ON `MessagesBodies`(`body_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn put_message_body(
    connection: &mut SqliteConnection,
    message_id: &str,
    body_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT OR IGNORE INTO `MessagesBodies` \
        (`joint_id`, `message_id`, `body_id`) \
        VALUES($1, $2, $3);\
        ",
    )
    .bind(joint_id(&[message_id, body_id]))
    .bind(message_id)
    .bind(body_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_message_bodies(
    connection: &mut SqliteConnection,
    message_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `body_id` FROM `MessagesBodies` \
        WHERE `message_id` = $1;\
        ",
    )
    .bind(message_id);
    let mut bodies_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let body_id: &str = row.try_get("body_id")?;
        bodies_id.push(body_id.to_string());
    }
    Ok(bodies_id)
}

pub async fn get_body_messages(
    connection: &mut SqliteConnection,
    body_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `message_id` FROM `MessagesBodies` \
        WHERE `body_id` = $1;\
        ",
    )
    .bind(body_id);
    let mut messages_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let message_id: &str = row.try_get("message_id")?;
        messages_id.push(message_id.to_string());
    }
    Ok(messages_id)
}

pub async fn has_message_with_body(
    connection: &mut SqliteConnection,
    body_id: &str,
) -> Result<bool> {
    let query = sqlx::query(
        "\
        SELECT 1 FROM `MessagesBodies` \
        WHERE `body_id` = $1 \
        LIMIT 1;\
        ",
    )
    .bind(body_id);
    Ok(query.fetch_optional(&mut *connection).await?.is_some())
}

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
    use super::*;

    #[tokio::test]
    async fn gets_has_messages_and_bodies() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_message_body(&mut connection, "urn:cid:message1", "urn:cid:body1")
            .await
            .unwrap();
        put_message_body(&mut connection, "urn:cid:message2", "urn:cid:body1")
            .await
            .unwrap();
        put_message_body(&mut connection, "urn:cid:message2", "urn:cid:body2")
            .await
            .unwrap();
        put_message_body(&mut connection, "urn:cid:message3", "urn:cid:body3")
            .await
            .unwrap();
        assert_eq!(
            get_message_bodies(&mut connection, "urn:cid:message2")
                .await
                .unwrap(),
            ["urn:cid:body1", "urn:cid:body2"]
        );
        assert_eq!(
            get_body_messages(&mut connection, "urn:cid:body1")
                .await
                .unwrap(),
            ["urn:cid:message1", "urn:cid:message2"]
        );
        assert!(has_message_with_body(&mut connection, "urn:cid:body1")
            .await
            .unwrap());
        assert!(has_message_with_body(&mut connection, "urn:cid:body2")
            .await
            .unwrap());
        assert!(has_message_with_body(&mut connection, "urn:cid:body3")
            .await
            .unwrap());
        assert!(!has_message_with_body(&mut connection, "urn:cid:body4")
            .await
            .unwrap());
    }
}
