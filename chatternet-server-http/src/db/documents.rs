use anyhow::Result;
use sqlx::{Row, SqliteConnection};

pub async fn create_documents(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `Documents` \
        (\
            `document_id` TEXT PRIMARY KEY, \
            `document` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn put_document_if_new(
    connection: &mut SqliteConnection,
    document_id: &str,
    document: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT OR IGNORE INTO `Documents` \
        (`document_id`, `document`) \
        VALUES($1, $2);\
        ",
    )
    .bind(document_id)
    .bind(document)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn put_document(
    connection: &mut SqliteConnection,
    document_id: &str,
    document: &str,
) -> Result<()> {
    sqlx::query(
        "\
        REPLACE INTO `Documents` \
        (`document_id`, `document`) \
        VALUES($1, $2);\
        ",
    )
    .bind(document_id)
    .bind(document)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_document(
    connection: &mut SqliteConnection,
    document_id: &str,
) -> Result<Option<String>> {
    Ok(sqlx::query(
        "\
        SELECT `document` FROM `Documents` \
        WHERE `document_id` = $1;\
        ",
    )
    .bind(document_id)
    .fetch_optional(connection)
    .await?
    .and_then(|x| x.get("document")))
}

pub async fn delete_document(connection: &mut SqliteConnection, document_id: &str) -> Result<()> {
    sqlx::query(
        "\
        DELETE FROM `Documents` \
        WHERE `document_id` = $1;\
        ",
    )
    .bind(document_id)
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
    async fn db_puts_gets_deletes_document() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_document(&mut connection, "id:1", "document")
            .await
            .unwrap();
        assert_eq!(
            get_document(&mut connection, "id:1").await.unwrap(),
            Some("document".to_string())
        );
        put_document(&mut connection, "id:1", "document2")
            .await
            .unwrap();
        assert_eq!(
            get_document(&mut connection, "id:1").await.unwrap(),
            Some("document2".to_string())
        );
        delete_document(&mut connection, "id:1").await.unwrap();
        assert!(get_document(&mut connection, "id:1")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn db_puts_document_if_new() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_document_if_new(&mut connection, "id:1", "document")
            .await
            .unwrap();
        assert_eq!(
            get_document(&mut connection, "id:1").await.unwrap(),
            Some("document".to_string())
        );
        put_document_if_new(&mut connection, "id:1", "document2")
            .await
            .unwrap();
        assert_eq!(
            get_document(&mut connection, "id:1").await.unwrap(),
            Some("document".to_string())
        );
    }
}
