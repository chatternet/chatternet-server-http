use anyhow::Result;
use sqlx::{Row, SqliteConnection};

pub async fn create_objects(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `Objects` \
        (\
            `object_id` TEXT PRIMARY KEY, \
            `object` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn put_object_if_new(
    connection: &mut SqliteConnection,
    object_id: &str,
    object: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT OR IGNORE INTO `Objects` \
        (`object_id`, `object`) \
        VALUES($1, $2);\
        ",
    )
    .bind(object_id)
    .bind(object)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn put_object(
    connection: &mut SqliteConnection,
    object_id: &str,
    object: &str,
) -> Result<()> {
    sqlx::query(
        "\
        REPLACE INTO `Objects` \
        (`object_id`, `object`) \
        VALUES($1, $2);\
        ",
    )
    .bind(object_id)
    .bind(object)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_object(
    connection: &mut SqliteConnection,
    object_id: &str,
) -> Result<Option<String>> {
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

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
    use super::*;

    #[tokio::test]
    async fn db_puts_and_gets_an_object() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_object(&mut connection, "id:1", "object").await.unwrap();
        assert_eq!(
            get_object(&mut connection, "id:1").await.unwrap(),
            Some("object".to_string())
        );
        put_object(&mut connection, "id:1", "object2")
            .await
            .unwrap();
        assert_eq!(
            get_object(&mut connection, "id:1").await.unwrap(),
            Some("object2".to_string())
        );
    }

    #[tokio::test]
    async fn db_puts_an_object_if_new() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_object_if_new(&mut connection, "id:1", "object")
            .await
            .unwrap();
        assert_eq!(
            get_object(&mut connection, "id:1").await.unwrap(),
            Some("object".to_string())
        );
        put_object_if_new(&mut connection, "id:1", "object2")
            .await
            .unwrap();
        assert_eq!(
            get_object(&mut connection, "id:1").await.unwrap(),
            Some("object".to_string())
        );
    }
}
