use anyhow::Result;
use sqlx::SqliteConnection;

pub async fn create_messages(connection: &mut SqliteConnection) -> Result<()> {
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
    connection: &mut SqliteConnection,
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

pub async fn has_message(connection: &mut SqliteConnection, id: &str) -> Result<bool> {
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

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
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
}
