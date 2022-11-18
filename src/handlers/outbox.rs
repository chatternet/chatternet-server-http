use anyhow::{anyhow, Result};
use sqlx::SqliteConnection;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::http::StatusCode;
use warp::Rejection;

use super::error::Error;
use crate::chatternet::activities::{actor_id_from_did, ActivityType, Message};
use crate::db::{self, Connector};

pub fn build_audiences_id(message: &Message) -> Result<Vec<String>> {
    if message
        .members
        .as_ref()
        .map(|x| x.contains_key("bcc"))
        .unwrap_or(false)
    {
        Err(anyhow!("message contains bcc which cannot be handled"))?;
    }
    if message
        .members
        .as_ref()
        .map(|x| x.contains_key("bto"))
        .unwrap_or(false)
    {
        Err(anyhow!("message contains bto which cannot be handled"))?;
    }

    let tos_id: Option<Vec<String>> = message
        .members
        .as_ref()
        .and_then(|x| x.get("to"))
        .and_then(|x| x.as_array())
        .map(|x| {
            x.iter()
                .filter_map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Vec<String>>()
        });

    let ccs_id = message
        .members
        .as_ref()
        .and_then(|x| x.get("cc"))
        .and_then(|x| x.as_array())
        .map(|x| {
            x.iter()
                .flat_map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Vec<String>>()
        });

    let audiences_id = message
        .members
        .as_ref()
        .and_then(|x| x.get("audience"))
        .and_then(|x| x.as_array())
        .map(|x| {
            x.iter()
                .flat_map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Vec<String>>()
        });

    let chained = std::iter::empty::<String>()
        .chain(tos_id.unwrap_or(Vec::new()).into_iter())
        .chain(ccs_id.unwrap_or(Vec::new()).into_iter())
        .chain(audiences_id.unwrap_or(Vec::new()).into_iter());
    Ok(chained.collect())
}

async fn handle_follow(
    message: &Message,
    connection: &mut SqliteConnection,
) -> Result<(), Rejection> {
    let actor_id = message.actor.as_str();
    let objects_id: Vec<&str> = message.object.iter().map(|x| x.as_str()).collect();
    for object_id in objects_id {
        // following a did, it as a contact for filtering
        if object_id.starts_with("did:") {
            db::put_actor_contact(&mut *connection, &actor_id, &object_id)
                .await
                .map_err(|_| Error::DbQueryFailed)?;
        }
        // following any id, add to its followers collection
        db::put_actor_audience(
            &mut *connection,
            &actor_id,
            &format!("{}/followers", object_id),
        )
        .await
        .map_err(|_| Error::DbQueryFailed)?;
    }
    Ok(())
}

pub async fn handle_did_outbox(
    did: String,
    message: Message,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(|_| Error::DidNotValid)?;
    if actor_id != message.actor.as_str() {
        Err(Error::ActorIdWrong)?;
    }

    // read write
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| Error::DbConnectionFailed)?;

    // if already known, take no actions
    let message_id = message.verify().await.map_err(|_| Error::MessageNotValid)?;
    if db::has_message(&mut *connection, &message_id)
        .await
        .map_err(|_| Error::DbQueryFailed)?
    {
        return Ok(StatusCode::ACCEPTED);
    };

    // run type-dependent side effects
    match message.message_type {
        // activity expresses a follow relationship
        ActivityType::Follow => handle_follow(&message, &mut *connection).await?,
        _ => (),
    }

    // store this message id for its audiences
    let audiences_id = build_audiences_id(&message).map_err(|_| Error::MessageNotValid)?;
    for audience_id in audiences_id {
        db::put_message_audience(&mut *connection, &message_id, &audience_id)
            .await
            .map_err(|_| Error::DbQueryFailed)?;
    }

    // create empty objects in the DB which can be updated later
    let objects_id: Vec<&str> = message.object.iter().map(|x| x.as_str()).collect();
    for object_id in objects_id {
        db::put_or_update_object(&mut *connection, object_id, None)
            .await
            .map_err(|_| Error::DbQueryFailed)?;
    }

    // create an empty object for the actor which can be updated later
    db::put_or_update_object(&mut *connection, message.actor.as_str(), None)
        .await
        .map_err(|_| Error::DbQueryFailed)?;

    // store the message itself
    let message = serde_json::to_string(&message).map_err(|_| Error::MessageNotValid)?;
    db::put_message(&mut *connection, &message, &message_id, &actor_id)
        .await
        .map_err(|_| Error::DbQueryFailed)?;

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use ssi::vc::URI;
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::chatternet::didkey;
    use crate::db::Connector;

    use super::super::build_api;
    use super::super::test::{build_follow, build_message};
    use super::*;

    const NO_VEC: Option<&Vec<String>> = None;

    #[tokio::test]
    async fn builds_audiences_id() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let audiences_id = build_audiences_id(
            &build_message(
                "id:1",
                Some(&["did:example:a", "did:example:b"]),
                Some(&["did:example:c"]),
                Some(&["did:example:d"]),
                &jwk,
            )
            .await,
        )
        .unwrap();
        assert_eq!(
            audiences_id,
            [
                "did:example:a",
                "did:example:b",
                "did:example:c",
                "did:example:d"
            ]
        );
    }

    #[tokio::test]
    async fn api_outbox_handles_message() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        // second post returns status accepted
        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn api_outbox_rejects_wrong_did() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path("/did:example:a/actor/outbox")
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_outbox_rejects_invalid_message() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let mut message_2 = message.clone();
        message_2.id = Some(URI::from_str("id:a").unwrap());
        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message_2)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_outbox_rejects_with_bcc() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;
        message.members.as_mut().and_then(|x| {
            x.insert(
                "bcc".to_string(),
                serde_json::to_value("did:example:a").unwrap(),
            )
        });

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_outbox_rejects_with_bto() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;
        message.members.as_mut().and_then(|x| {
            x.insert(
                "bto".to_string(),
                serde_json::to_value("did:example:a").unwrap(),
            )
        });

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_outbox_handles_follow() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        assert_eq!(
            request()
                .method("POST")
                .path(&format!("/{}/actor/outbox", did))
                .json(&build_follow(&["tag:1", "tag:2"], &jwk).await)
                .reply(&api)
                .await
                .status(),
            StatusCode::OK
        );

        let response = request()
            .method("GET")
            .path(&format!("/{}/actor/following", did))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let ids: Vec<String> = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(ids, ["tag:1", "tag:2"]);
    }
}
