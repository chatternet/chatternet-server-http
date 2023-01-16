use anyhow::Result;
use sqlx::{Row, SqliteConnection};

pub async fn create_mutable_modified(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `MutableModified` \
        (\
            `id` TEXT PRIMARY KEY, \
            `timestamp_millis` BIGINT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn put_mutable_modified(
    connection: &mut SqliteConnection,
    id: &str,
    timestamp_millis: i64,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT OR REPLACE INTO `MutableModified` \
        (`id`, `timestamp_millis`) \
        VALUES($1, $2)
        ",
    )
    .bind(id)
    .bind(timestamp_millis)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_mutable_modified(
    connection: &mut SqliteConnection,
    id: &str,
) -> Result<Option<i64>> {
    Ok(sqlx::query(
        "\
        SELECT `timestamp_millis` FROM `MutableModified` \
        WHERE `id` = $1;\
        ",
    )
    .bind(id)
    .fetch_optional(&mut *connection)
    .await?
    .map(|x| x.get(0)))
}

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
    use super::*;

    #[tokio::test]
    async fn puts_and_gets_mutable_modified() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_mutable_modified(&mut connection, "id:1", 1)
            .await
            .unwrap();
        put_mutable_modified(&mut connection, "id:2", 2)
            .await
            .unwrap();
        assert_eq!(
            get_mutable_modified(&mut connection, "id:1").await.unwrap(),
            Some(1)
        );
        assert_eq!(
            get_mutable_modified(&mut connection, "id:2").await.unwrap(),
            Some(2)
        );
        assert!(get_mutable_modified(&mut connection, "id:3")
            .await
            .unwrap()
            .is_none());
    }
}
