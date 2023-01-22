use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Row, SqliteConnection};

use super::joint_id;

pub async fn create_message_documents(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `MessageDocuments` \
        (\
            `joint_id` TEXT PRIMARY KEY, \
            `message_id` TEXT NOT NULL, \
            `document_id` TEXT NOT NULL, \
            `created_by` TEXT\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `message_id` \
        ON `MessageDocuments`(`message_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `document_id` \
        ON `MessageDocuments`(`document_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `created_by` \
        ON `MessageDocuments`(`created_by`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn put_message_document(
    connection: &mut SqliteConnection,
    message_id: &str,
    document_id: &str,
    created_by: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT OR IGNORE INTO `MessageDocuments` \
        (`joint_id`, `message_id`, `document_id`, `created_by`) \
        VALUES($1, $2, $3, $4);\
        ",
    )
    .bind(joint_id(&[message_id, document_id]))
    .bind(message_id)
    .bind(document_id)
    .bind(created_by)
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
        SELECT `document_id` FROM `MessageDocuments` \
        WHERE `message_id` = $1;\
        ",
    )
    .bind(message_id);
    let mut documents_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let document_id: &str = row.try_get("document_id")?;
        documents_id.push(document_id.to_string());
    }
    Ok(documents_id)
}

pub async fn get_document_messages(
    connection: &mut SqliteConnection,
    document_id: &str,
    created_by: Option<&str>,
) -> Result<Vec<String>> {
    let query = if let Some(created_by) = created_by {
        sqlx::query(
            "\
            SELECT `message_id` FROM `MessageDocuments` \
            WHERE `document_id` = $1 \
            AND `created_by` = $2;\
            ",
        )
        .bind(document_id)
        .bind(created_by)
    } else {
        sqlx::query(
            "\
            SELECT `message_id` FROM `MessageDocuments` \
            WHERE `document_id` = $1;\
            ",
        )
        .bind(document_id)
    };
    let mut messages_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let message_id: &str = row.try_get("message_id")?;
        messages_id.push(message_id.to_string());
    }
    Ok(messages_id)
}

pub async fn has_message_with_document(
    connection: &mut SqliteConnection,
    document_id: &str,
) -> Result<bool> {
    let query = sqlx::query(
        "\
        SELECT 1 FROM `MessageDocuments` \
        WHERE `document_id` = $1 \
        LIMIT 1;\
        ",
    )
    .bind(document_id);
    Ok(query.fetch_optional(&mut *connection).await?.is_some())
}

pub async fn delete_message_documents(
    connection: &mut SqliteConnection,
    message_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        DELETE FROM `MessageDocuments` \
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
    async fn gets_has_deletes_messages_and_bodies() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_message_document(
            &mut connection,
            "urn:cid:message1",
            "urn:cid:document1",
            Some("did:example:a"),
        )
        .await
        .unwrap();
        put_message_document(
            &mut connection,
            "urn:cid:message2",
            "urn:cid:document1",
            Some("did:example:b"),
        )
        .await
        .unwrap();
        put_message_document(
            &mut connection,
            "urn:cid:message2",
            "urn:cid:document2",
            None,
        )
        .await
        .unwrap();
        put_message_document(
            &mut connection,
            "urn:cid:message3",
            "urn:cid:document3",
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            get_message_bodies(&mut connection, "urn:cid:message2")
                .await
                .unwrap(),
            ["urn:cid:document1", "urn:cid:document2"]
        );
        assert_eq!(
            get_document_messages(&mut connection, "urn:cid:document1", None)
                .await
                .unwrap(),
            ["urn:cid:message1", "urn:cid:message2"]
        );
        assert_eq!(
            get_document_messages(&mut connection, "urn:cid:document1", Some("did:example:a"))
                .await
                .unwrap(),
            ["urn:cid:message1"]
        );
        assert!(
            has_message_with_document(&mut connection, "urn:cid:document1")
                .await
                .unwrap()
        );
        assert!(
            has_message_with_document(&mut connection, "urn:cid:document2")
                .await
                .unwrap()
        );
        assert!(
            has_message_with_document(&mut connection, "urn:cid:document3")
                .await
                .unwrap()
        );
        assert!(
            !has_message_with_document(&mut connection, "urn:cid:document4")
                .await
                .unwrap()
        );
        delete_message_documents(&mut connection, "urn:cid:message3")
            .await
            .unwrap();
        assert!(
            !has_message_with_document(&mut connection, "urn:cid:document3")
                .await
                .unwrap()
        );
        delete_message_documents(&mut connection, "urn:cid:message1")
            .await
            .unwrap();
        assert!(get_message_bodies(&mut connection, "urn:cid:message1")
            .await
            .unwrap()
            .is_empty());
    }
}
