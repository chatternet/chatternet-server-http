use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Row, SqliteConnection};

use super::joint_id;

pub async fn create_actor_following(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `ActorsFollowings` \
        (\
            `joint_id` TEXT PRIMARY KEY, \
            `actor_id` TEXT NOT NULL, \
            `following_id` TEXT NOT NULL\
        );\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `actor_id` \
        ON `ActorsFollowings`(`actor_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;
    sqlx::query(
        "\
        CREATE INDEX IF NOT EXISTS `following_id` \
        ON `ActorsFollowings`(`following_id`);\
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn put_actor_following(
    connection: &mut SqliteConnection,
    actor_id: &str,
    following_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        INSERT OR IGNORE INTO `ActorsFollowings` \
        (`joint_id`, `actor_id`, `following_id`) \
        VALUES($1, $2, $3);\
        ",
    )
    .bind(joint_id(&[actor_id, following_id]))
    .bind(actor_id)
    .bind(following_id)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn get_actor_followings(
    connection: &mut SqliteConnection,
    actor_id: &str,
) -> Result<Vec<String>> {
    let query = sqlx::query(
        "\
        SELECT `following_id` FROM `ActorsFollowings` \
        WHERE `actor_id` = $1;\
        ",
    )
    .bind(actor_id);
    let mut followings_id = Vec::new();
    let mut rows = query.fetch(&mut *connection);
    while let Some(row) = rows.try_next().await? {
        let following_id: &str = row.try_get("following_id")?;
        followings_id.push(following_id.to_string());
    }
    Ok(followings_id)
}

#[cfg(test)]
mod test {
    use tokio;

    use super::super::Connector;
    use super::*;

    #[tokio::test]
    async fn puts_and_gets_actor_followings() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_following(&mut connection, "did:1/actor", "did:2/actor")
            .await
            .unwrap();
        put_actor_following(&mut connection, "did:2/actor", "did:1/actor")
            .await
            .unwrap();
        put_actor_following(&mut connection, "did:2/actor", "did:3/actor")
            .await
            .unwrap();
        assert_eq!(
            get_actor_followings(&mut connection, "did:1/actor")
                .await
                .unwrap(),
            ["did:2/actor"]
        );
        assert_eq!(
            get_actor_followings(&mut connection, "did:2/actor")
                .await
                .unwrap(),
            ["did:1/actor", "did:3/actor"]
        );
    }
}
