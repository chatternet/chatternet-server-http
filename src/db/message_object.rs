use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Row, SqliteConnection};

pub async fn create_messages_objects(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `MessagesObjects` \
        (\
            `message_id` TEXT NOT NULL, \
            `object_id` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `message_id` \
        ON `MessagesObjects`(`message_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `object_id` \
        ON `MessagesObjects`(`object_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn put_message_object(
    connection: &mut SqliteConnection,
    message_id: &str,
    object_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT INTO `MessagesObjects` \
        (`message_id`, `object_id`) \
        VALUES($1, $2);\
        ",
    )
    .bind(message_id)
    .bind(object_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_message_objects(
    connection: &mut SqliteConnection,
    message_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `object_id` FROM `MessagesObjects` \
        WHERE `message_id` = $1;\
        ",
    )
    .bind(message_id);
    let mut objects_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let object_id: &str = row.try_get("object_id")?;
        objects_id.push(object_id.to_string());
    }
    Ok(objects_id)
}

pub async fn get_object_messages(
    connection: &mut SqliteConnection,
    object_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `message_id` FROM `MessagesObjects` \
        WHERE `object_id` = $1;\
        ",
    )
    .bind(object_id);
    let mut messages_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let message_id: &str = row.try_get("message_id")?;
        messages_id.push(message_id.to_string());
    }
    Ok(messages_id)
}

pub async fn has_message_with_object(
    connection: &mut SqliteConnection,
    object_id: &str,
) -> Result<bool> {
    let query = sqlx::query(
        "\
        SELECT 1 FROM `MessagesObjects` \
        WHERE `object_id` = $1 \
        LIMIT 1;\
        ",
    )
    .bind(object_id);
    Ok(query.fetch_optional(&mut *connection).await?.is_some())
}

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
    use super::*;

    #[tokio::test]
    async fn gets_has_messages_and_objects() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_message_object(&mut connection, "urn:cid:message1", "urn:cid:object1")
            .await
            .unwrap();
        put_message_object(&mut connection, "urn:cid:message2", "urn:cid:object1")
            .await
            .unwrap();
        put_message_object(&mut connection, "urn:cid:message2", "urn:cid:object2")
            .await
            .unwrap();
        put_message_object(&mut connection, "urn:cid:message3", "urn:cid:object3")
            .await
            .unwrap();
        assert_eq!(
            get_message_objects(&mut connection, "urn:cid:message2")
                .await
                .unwrap(),
            ["urn:cid:object1", "urn:cid:object2"]
        );
        assert_eq!(
            get_object_messages(&mut connection, "urn:cid:object1")
                .await
                .unwrap(),
            ["urn:cid:message1", "urn:cid:message2"]
        );
        assert!(has_message_with_object(&mut connection, "urn:cid:object1")
            .await
            .unwrap());
        assert!(has_message_with_object(&mut connection, "urn:cid:object2")
            .await
            .unwrap());
        assert!(has_message_with_object(&mut connection, "urn:cid:object3")
            .await
            .unwrap());
        assert!(!has_message_with_object(&mut connection, "urn:cid:object4")
            .await
            .unwrap());
    }
}
