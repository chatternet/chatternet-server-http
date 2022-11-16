use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Row, SqliteConnection};

pub async fn create_actors_contacts(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `ActorsContacts` \
        (\
            `actor_id` TEXT NOT NULL, \
            `contact_id` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `actor_id` \
        ON `ActorsContacts`(`actor_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `contact_id` \
        ON `ActorsContacts`(`contact_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn put_actor_contact(
    connection: &mut SqliteConnection,
    actor_id: &str,
    contact_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT INTO `ActorsContacts` \
        (`actor_id`, `contact_id`) \
        VALUES($1, $2);\
        ",
    )
    .bind(actor_id)
    .bind(contact_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_actor_contacts(
    connection: &mut SqliteConnection,
    actor_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `contact_id` FROM `ActorsContacts` \
        WHERE `actor_id` = $1;\
        ",
    )
    .bind(actor_id);
    let mut contacts_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let contact_id: &str = row.try_get("contact_id")?;
        contacts_id.push(contact_id.to_string());
    }
    Ok(contacts_id)
}

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
    use super::*;

    #[tokio::test]
    async fn puts_and_gets_actor_contacts() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_contact(&mut connection, "did:1/actor", "did:2/actor")
            .await
            .unwrap();
        put_actor_contact(&mut connection, "did:2/actor", "did:1/actor")
            .await
            .unwrap();
        put_actor_contact(&mut connection, "did:2/actor", "did:3/actor")
            .await
            .unwrap();
        assert_eq!(
            get_actor_contacts(&mut connection, "did:1/actor")
                .await
                .unwrap(),
            ["did:2/actor"]
        );
        assert_eq!(
            get_actor_contacts(&mut connection, "did:2/actor")
                .await
                .unwrap(),
            ["did:1/actor", "did:3/actor"]
        );
    }
}
