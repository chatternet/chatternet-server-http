/// Serialize to and from text and bytes formats. Leave further casting to
/// the caller. In some instances a serialized format is preferred, so don't
/// force casting into the interface.
use anyhow::Result;
use futures::TryStreamExt;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;

fn build_in_parameters(num_parameters: usize, start: Option<usize>) -> String {
    let start = start.unwrap_or(1);
    (start..start + num_parameters)
        .map(|x| format!("${}", x))
        .collect::<Vec<String>>()
        .join(",")
}

pub struct Db {
    pool: SqlitePool,
}

pub const NO_TAGS: Option<&[&str]> = None;

impl Db {
    pub async fn new(url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new().connect(url).await?;
        let mut conn = pool.acquire().await?;

        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `Messages` \
            (\
                `message` TEXT NOT NULL, \
                `message_id` TEXT PRIMARY KEY, \
                `timestamp_micros` BIGINT NOT NULL, \
                `issuer_did` TEXT NOT NULL\
            );\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `timestamp_micros` \
            ON `Messages`(`timestamp_micros`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `issuer_did` \
            ON `Messages`(`issuer_did`);\
            ",
        )
        .execute(&mut conn)
        .await?;

        sqlx::query(
            "\
            CREATE TABLE IF NOT EXISTS `MessagesTags` \
            (\
                `message_id` TEXT NOT NULL, \
                `tag_id` TEXT NOT NULL, \
                `timestamp_micros` BIGINT NOT NULL\
            );\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `message_id` \
            ON `MessagesTags`(`message_id`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `tag_id` \
            ON `MessagesTags`(`tag_id`);\
            ",
        )
        .execute(&mut conn)
        .await?;
        sqlx::query(
            "\
            CREATE INDEX IF NOT EXISTS `timestamp_micros` \
            ON `MessagesTags`(`timestamp_micros`);\
            ",
        )
        .execute(&mut conn)
        .await?;

        Ok(Db { pool })
    }

    pub async fn put_message(
        &self,
        message: &str,
        message_id: &str,
        timestamp_micros: i64,
        issuer_did: &str,
        tags_id: Option<&[impl AsRef<str>]>,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query(
            "\
            INSERT INTO `Messages` \
            (`message`, `message_id`, `timestamp_micros`, `issuer_did`) \
            VALUES($1, $2, $3, $4)\
            ",
        )
        .bind(message)
        .bind(message_id)
        .bind(timestamp_micros)
        .bind(issuer_did)
        .execute(&mut conn)
        .await?;
        if let Some(tags_id) = tags_id {
            for tag_id in tags_id {
                sqlx::query(
                    "\
                    INSERT INTO `MessagesTags` \
                    (`message_id`, `tag_id`, `timestamp_micros`) \
                    VALUES($1, $2, $3);\
                    ",
                )
                .bind(message_id)
                .bind(tag_id.as_ref())
                .bind(timestamp_micros)
                .execute(&mut conn)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn get_issuers_messages(
        &self,
        issuers_did: &Vec<String>,
        start_timestamp_micros: i64,
        tags_id: Option<&[impl AsRef<str>]>,
    ) -> Result<Vec<String>> {
        let mut conn = self.pool.acquire().await?;
        let parameters_issuer = build_in_parameters(issuers_did.len(), Some(2));
        let query_str = if let Some(tags_id) = tags_id {
            let parameters_tag = build_in_parameters(tags_id.len(), Some(2 + issuers_did.len()));
            format!(
                "\
                SELECT `message` FROM `Messages` \
                WHERE `timestamp_micros` >= $1 \
                AND `issuer_did` IN ({}) \
                AND `message_id` IN (\
                    SELECT `message_id` FROM `MessagesTags` \
                    WHERE `timestamp_micros` >= $1 \
                    AND `MessagesTags`.`tag_id` IN ({})\
                ) \
                ORDER BY `timestamp_micros`;\
                ",
                parameters_issuer, parameters_tag,
            )
        } else {
            format!(
                "\
                SELECT `message` FROM `Messages` \
                WHERE `timestamp_micros` >= $1 \
                AND `issuer_did` IN ({}) \
                ORDER BY `timestamp_micros`;\
                ",
                parameters_issuer,
            )
        };
        let mut query = sqlx::query(&query_str);
        query = query.bind(start_timestamp_micros);
        for issuer_did in issuers_did {
            query = query.bind(issuer_did);
        }
        if let Some(tags_id) = tags_id {
            for tag_id in tags_id {
                query = query.bind(tag_id.as_ref());
            }
        }
        let mut messages = Vec::new();
        let mut rows = query.fetch(&mut conn);
        while let Some(row) = rows.try_next().await? {
            let message: &str = row.try_get("message")?;
            messages.push(message.to_string());
        }
        Ok(messages)
    }

    pub async fn filter_has_messages(
        &self,
        ids: &[impl AsRef<str>],
        start_timestamp_micros: i64,
    ) -> Result<Vec<String>> {
        let mut conn = self.pool.acquire().await?;
        let parameters = build_in_parameters(ids.len(), Some(2));
        let query_str = format!(
            "\
            SELECT `message_id` FROM `Messages` \
            WHERE `timestamp_micros` >= $1 AND `message_id` IN ({});\
            ",
            parameters
        );
        let mut query = sqlx::query(&query_str);
        query = query.bind(start_timestamp_micros);
        for message_id in ids {
            query = query.bind(message_id.as_ref());
        }
        let mut filtered_ids = Vec::new();
        let mut rows = query.fetch(&mut conn);
        while let Some(row) = rows.try_next().await? {
            let message_id: &str = row.try_get("message_id")?;
            filtered_ids.push(message_id.to_string());
        }
        Ok(filtered_ids)
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn db_new_is_ok() {
        let _ = Db::new("sqlite::memory:").await.unwrap();
    }

    #[tokio::test]
    async fn db_puts_message() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_message("abc", "a:b", 10, "did:a", NO_TAGS)
            .await
            .unwrap();
        db.put_message("abc", "a:c", 10, "did:a", Some(&["tag:a"]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn db_gets_issuers_messages() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        // too early
        db.put_message("a1", "a:b", 10, "did:key:a", NO_TAGS)
            .await
            .unwrap();
        db.put_message("a2", "a:c", 11, "did:key:a", NO_TAGS)
            .await
            .unwrap();
        db.put_message("a3", "a:d", 12, "did:key:b", NO_TAGS)
            .await
            .unwrap();
        // doesn't have requested issuer
        db.put_message("a4", "a:e", 13, "did:key:c", NO_TAGS)
            .await
            .unwrap();
        let messages = db
            .get_issuers_messages(
                &vec!["did:key:a".to_string(), "did:key:b".to_string()],
                11,
                NO_TAGS,
            )
            .await
            .unwrap();
        assert_eq!(messages, ["a2", "a3"]);
    }

    #[tokio::test]
    async fn db_gets_issuers_messages_with_tags() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        // too early
        db.put_message("a1", "a:b", 10, "did:key:a", Some(&["tag:a"]))
            .await
            .unwrap();
        db.put_message("a2", "a:c", 11, "did:key:a", Some(&["tag:a"]))
            .await
            .unwrap();
        db.put_message("a3", "a:d", 12, "did:key:b", Some(&["tag:b", "tag:c"]))
            .await
            .unwrap();
        // doesn't have requested tags
        db.put_message("a4", "a:e", 12, "did:key:b", Some(&["tag:c"]))
            .await
            .unwrap();
        // doesn't have requested issuer
        db.put_message("a5", "a:f", 13, "did:key:c", Some(&["tag:a"]))
            .await
            .unwrap();
        let messages = db
            .get_issuers_messages(
                &vec!["did:key:a".to_string(), "did:key:b".to_string()],
                11,
                Some(&["tag:a", "tag:b"]),
            )
            .await
            .unwrap();
        assert_eq!(messages, ["a2", "a3"]);
    }

    #[tokio::test]
    async fn db_filters_has_messages() {
        let db = Db::new("sqlite::memory:").await.unwrap();
        db.put_message("", "a:b", 10, "", NO_TAGS).await.unwrap();
        db.put_message("", "a:c", 11, "", NO_TAGS).await.unwrap();
        db.put_message("", "a:d", 12, "", NO_TAGS).await.unwrap();
        db.put_message("", "a:e", 13, "", NO_TAGS).await.unwrap();
        let has = db
            .filter_has_messages(&["a:b", "a:c", "a:d", "a:f"], 11)
            .await
            .unwrap();
        assert_eq!(has, ["a:c", "a:d"]);
    }
}
