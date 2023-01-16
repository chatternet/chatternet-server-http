use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Row, SqliteConnection};

use super::joint_id;

pub async fn create_actors_audiences(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `ActorsAudiences` \
        (\
            `joint_id` TEXT PRIMARY KEY, \
            `actor_id` TEXT NOT NULL, \
            `audience_id` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `actor_id` \
        ON `ActorsAudiences`(`actor_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `audience_id` \
        ON `ActorsAudiences`(`audience_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn put_actor_audience(
    connection: &mut SqliteConnection,
    actor_id: &str,
    audience_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT OR IGNORE INTO `ActorsAudiences` \
        (`joint_id`, `actor_id`, `audience_id`) \
        VALUES($1, $2, $3);\
        ",
    )
    .bind(joint_id(&[actor_id, audience_id]))
    .bind(actor_id)
    .bind(audience_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn delete_actor_audience(
    connection: &mut SqliteConnection,
    actor_id: &str,
    audience_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        DELETE FROM `ActorsAudiences` \
        WHERE `joint_id` = $1;\
        ",
    )
    .bind(joint_id(&[actor_id, audience_id]))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn delete_actor_all_audiences(
    connection: &mut SqliteConnection,
    actor_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        DELETE FROM `ActorsAudiences` \
        WHERE `actor_id` = $1;\
        ",
    )
    .bind(actor_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_actor_audiences(
    connection: &mut SqliteConnection,
    actor_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `audience_id` FROM `ActorsAudiences` \
        WHERE `actor_id` = $1;\
        ",
    )
    .bind(actor_id);
    let mut audiences_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let audience_id: &str = row.try_get("audience_id")?;
        audiences_id.push(audience_id.to_string());
    }
    Ok(audiences_id)
}

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
    use super::*;

    #[tokio::test]
    async fn puts_and_gets_actor_audiences() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_audience(&mut connection, "did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        put_actor_audience(&mut connection, "did:1/actor", "tag:1/followers")
            .await
            .unwrap();
        put_actor_audience(&mut connection, "did:2/actor", "tag:1/followers")
            .await
            .unwrap();
        assert_eq!(
            get_actor_audiences(&mut connection, "did:1/actor")
                .await
                .unwrap(),
            ["did:2/actor/followers", "tag:1/followers"]
        );
        assert_eq!(
            get_actor_audiences(&mut connection, "did:2/actor")
                .await
                .unwrap(),
            ["tag:1/followers"]
        );
    }

    #[tokio::test]
    async fn deletes_actor_audiences() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_audience(&mut connection, "did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        put_actor_audience(&mut connection, "did:1/actor", "tag:1/followers")
            .await
            .unwrap();
        delete_actor_audience(&mut connection, "did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        assert_eq!(
            get_actor_audiences(&mut connection, "did:1/actor")
                .await
                .unwrap(),
            ["tag:1/followers"]
        );
    }

    #[tokio::test]
    async fn deletes_actor_all_audiences() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_audience(&mut connection, "did:1/actor", "did:2/actor/followers")
            .await
            .unwrap();
        put_actor_audience(&mut connection, "did:1/actor", "tag:1/followers")
            .await
            .unwrap();
        delete_actor_all_audiences(&mut connection, "did:1/actor")
            .await
            .unwrap();
        assert!(get_actor_audiences(&mut connection, "did:1/actor")
            .await
            .unwrap()
            .is_empty());
    }
}
