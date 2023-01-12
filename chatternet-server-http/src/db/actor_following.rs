use anyhow::Result;
use futures::TryStreamExt;
use sqlx::{Row, SqliteConnection};

use super::{joint_id, CollectionPageOut};

pub async fn create_actor_following(connection: &mut SqliteConnection) -> Result<()> {
    sqlx::query(
        "\
        CREATE TABLE IF NOT EXISTS `ActorsFollowings` \
        (\
            `idx` INTEGER PRIMARY KEY AUTOINCREMENT,
            `joint_id` TEXT UNIQUE NOT NULL, \
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

pub async fn delete_actor_following(
    connection: &mut SqliteConnection,
    actor_id: &str,
    following_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        DELETE FROM `ActorsFollowings` \
        WHERE `joint_id` = $1;\
        ",
    )
    .bind(joint_id(&[actor_id, following_id]))
    .execute(&mut *connection)
    .await?;
    Ok(())
}

pub async fn delete_actor_all_following(
    connection: &mut SqliteConnection,
    actor_id: &str,
) -> Result<()> {
    sqlx::query(
        "\
        DELETE FROM `ActorsFollowings` \
        WHERE `actor_id` = $1;\
        ",
    )
    .bind(actor_id)
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

pub async fn get_actor_followers(
    connection: &mut SqliteConnection,
    actor_id: &str,
    count: u64,
    start_idx: Option<u64>,
) -> Result<Option<CollectionPageOut>> {
    let query_str = format!(
        "\
        SELECT `idx`, `actor_id` FROM `ActorsFollowings` \
        WHERE `following_id` = $1 \
        {} \
        ORDER BY `idx` DESC \
        LIMIT $2;\
        ",
        if start_idx.is_some() {
            "AND `idx` <= $3 "
        } else {
            ""
        }
    );
    let query = match start_idx {
        Some(start_idx) => sqlx::query(&query_str)
            .bind(actor_id)
            .bind(i64::try_from(count)?)
            .bind(u32::try_from(start_idx)?),
        None => sqlx::query(&query_str)
            .bind(actor_id)
            .bind(i64::try_from(count)?),
    };
    let mut rows = query.fetch(&mut *connection);
    let mut ids = Vec::new();
    let mut first_idx: Option<u64> = None;
    let mut last_idx: Option<u64> = None;
    while let Some(row) = rows.try_next().await? {
        let id: &str = row.try_get("actor_id")?;
        let idx: u32 = row.try_get("idx")?;
        ids.push(id.to_string());
        first_idx = first_idx.map(|x| x.min(idx as u64)).or(Some(idx as u64));
        last_idx = last_idx.map(|x| x.max(idx as u64)).or(Some(idx as u64));
    }
    Ok(match (first_idx, last_idx) {
        (Some(first_idx), Some(last_idx)) => Some(CollectionPageOut {
            items: ids,
            low_idx: first_idx,
            high_idx: last_idx,
        }),
        _ => None,
    })
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
            get_actor_followings(&mut connection, "did:2/actor")
                .await
                .unwrap(),
            ["did:1/actor", "did:3/actor"]
        );
    }

    #[tokio::test]
    async fn deletes_actor_following() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_following(&mut connection, "did:2/actor", "did:1/actor")
            .await
            .unwrap();
        put_actor_following(&mut connection, "did:2/actor", "did:3/actor")
            .await
            .unwrap();
        delete_actor_following(&mut connection, "did:2/actor", "did:1/actor")
            .await
            .unwrap();
        assert_eq!(
            get_actor_followings(&mut connection, "did:2/actor")
                .await
                .unwrap(),
            ["did:3/actor"]
        );
    }

    #[tokio::test]
    async fn deletes_actor_all_following() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_following(&mut connection, "did:2/actor", "did:1/actor")
            .await
            .unwrap();
        put_actor_following(&mut connection, "did:2/actor", "did:3/actor")
            .await
            .unwrap();
        delete_actor_all_following(&mut connection, "did:2/actor")
            .await
            .unwrap();
        assert!(get_actor_followings(&mut connection, "did:2/actor")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn gets_followers() {
        let connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection().await.unwrap();
        put_actor_following(&mut connection, "did:1/actor", "did:3/actor")
            .await
            .unwrap();
        put_actor_following(&mut connection, "did:2/actor", "did:3/actor")
            .await
            .unwrap();
        // can list all followers
        let out = get_actor_followers(&mut connection, "did:3/actor", 3, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["did:2/actor", "did:1/actor"]);
        assert_eq!(out.high_idx, 2);
        assert_eq!(out.low_idx, 1);
        // can list some followers
        let out = get_actor_followers(&mut connection, "did:3/actor", 1, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["did:2/actor"]);
        assert_eq!(out.high_idx, 2);
        assert_eq!(out.low_idx, 2);
        // can start before end
        let out = get_actor_followers(&mut connection, "did:3/actor", 3, Some(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.items, ["did:1/actor"]);
        assert_eq!(out.high_idx, 1);
        assert_eq!(out.low_idx, 1);
        // 1 has no followers
        assert!(get_actor_followers(&mut connection, "did:1/actor", 3, None)
            .await
            .unwrap()
            .is_none());
    }
}
