use anyhow::Result;
use sqlx::{Row, SqliteConnection};

pub async fn create_objects(connection: &mut SqliteConnection) -> Result<()> {
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

pub async fn has_object(connection: &mut SqliteConnection, id: &str) -> Result<bool> {
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
    connection: &mut SqliteConnection,
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
