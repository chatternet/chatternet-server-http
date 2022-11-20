use anyhow::{Error as AnyError, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::Rejection;

use super::error::Error;
use crate::chatternet::activities::{actor_id_from_did, Inbox, Message};
use crate::db::{self, Connector};

#[derive(Deserialize, Serialize)]
pub struct DidInboxQuery {
    after: String,
}

pub async fn handle_did_inbox(
    did: String,
    query: Option<DidInboxQuery>,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(|_| Error::DidNotValid)?;
    // read only
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| Error::DbConnectionFailed)?;
    let after = query.as_ref().map(|x| x.after.as_str());
    let messages = db::get_inbox_for_actor(
        &mut connection,
        &actor_id,
        32,
        query.as_ref().map(|x| x.after.as_str()),
    )
    .await
    .map_err(|_| Error::DbQueryFailed)?;
    let messages = messages
        .iter()
        .map(|x| serde_json::from_str(x).map_err(AnyError::new))
        .collect::<Result<Vec<Message>>>()
        .map_err(|_| Error::DbQueryFailed)?;
    let inbox = Inbox::new(&actor_id, messages, after).map_err(|_| Error::DbQueryFailed)?;
    Ok(warp::reply::json(&inbox))
}

#[cfg(test)]
mod test {
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::chatternet::activities::Inbox;
    use crate::chatternet::didkey;
    use crate::db::Connector;

    use super::super::build_api;
    use super::super::test::{build_follow, build_message};
    use super::*;

    const NO_VEC: Option<&Vec<String>> = None;

    #[tokio::test]
    async fn api_inbox_returns_messages() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());

        let jwk_1 = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did_1 = didkey::did_from_jwk(&jwk_1).unwrap();

        let jwk_2 = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did_2 = didkey::did_from_jwk(&jwk_2).unwrap();

        // did_1 will see because follows self and this is addressed to self
        assert_eq!(
            request()
                .method("POST")
                .path(&format!("/{}/actor/outbox", did_1))
                .json(
                    &build_message(
                        "id:1",
                        Some(&[format!("{}/actor", did_1)]),
                        NO_VEC,
                        NO_VEC,
                        &jwk_1
                    )
                    .await
                )
                .reply(&api)
                .await
                .status(),
            StatusCode::OK
        );
        // did_1 won't see because follows did_2 but not addressed to an audience with did_1
        assert_eq!(
            request()
                .method("POST")
                .path(&format!("/{}/actor/outbox", did_1))
                .json(
                    &build_message(
                        "id:2",
                        Some(&[format!("{}/actor", did_2)]),
                        NO_VEC,
                        NO_VEC,
                        &jwk_1
                    )
                    .await
                )
                .reply(&api)
                .await
                .status(),
            StatusCode::OK
        );
        // did_1 will see because follows did_2 and in did_2 follower collection
        assert_eq!(
            request()
                .method("POST")
                .path(&format!("/{}/actor/outbox", did_1))
                .json(
                    &build_message(
                        "id:3",
                        Some(&[format!("{}/actor/followers", did_2)]),
                        NO_VEC,
                        NO_VEC,
                        &jwk_1
                    )
                    .await
                )
                .reply(&api)
                .await
                .status(),
            StatusCode::OK
        );

        // did_1 sees only own content addressed to self because not following others
        let response = request()
            .method("GET")
            .path(&format!("/{}/actor/inbox", did_1))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let inbox: Inbox = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            inbox
                .ordered_items
                .iter()
                .map(|x| x.object.iter().map(|x| x.as_str()))
                .flatten()
                .collect::<Vec<&str>>(),
            ["id:1"]
        );

        // did_1 follows did_2, gets added to did_2 followers
        assert_eq!(
            request()
                .method("POST")
                .path(&format!("/{}/actor/outbox", did_1))
                .json(&build_follow(&[&format!("{}/actor", did_2)], &jwk_1).await)
                .reply(&api)
                .await
                .status(),
            StatusCode::OK
        );

        let response = request()
            .method("GET")
            .path(&format!("/{}/actor/inbox", did_1))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let inbox: Inbox = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            inbox
                .ordered_items
                .iter()
                .map(|x| x.object.iter().map(|x| x.as_str()))
                .flatten()
                .collect::<Vec<&str>>(),
            ["id:3", "id:1"]
        );
    }
}
