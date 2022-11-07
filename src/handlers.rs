use anyhow::{anyhow, Result};
use did_method_key::DIDKey;
use ssi::did_resolve::{DIDResolver, ResolutionInputMetadata};
use std::sync::Arc;
use warp::http::StatusCode;
use warp::{Filter, Rejection};

use crate::chatternet::activities::{actor_id_from_did, Message, MessageType};
use crate::db::{
    Connection, DbPool, TableActorsAudiences, TableActorsContacts, TableMessages,
    TableMessagesAudiences, TableObjects, TablesActivityPub,
};
use crate::errors::Error;

fn build_audiences_id(message: &Message) -> Result<Vec<String>> {
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

async fn handle_follow(message: &Message, connection: &mut Connection) -> Result<()> {
    let actor_id = message.actor.as_str();
    let object_id = message.object.as_str();
    // following a did, use it as a contact for filtering
    if object_id.starts_with("did:") {
        connection.put_actor_contact(&actor_id, &object_id).await?;
    }
    // following any id, add to its followers collection
    connection
        .put_actor_audience(&actor_id, &format!("{}/followers", object_id))
        .await?;
    Ok(())
}

async fn handle_did_outbox(
    did: String,
    message: Message,
    db_pool: Arc<DbPool>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(Error)?;
    if actor_id != message.actor.as_str() {
        Err(anyhow!("posting to the wrong outbox for the message actor")).map_err(Error)?;
    }

    let mut connection = db_pool
        .acquire()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;

    // if already known, take no actions
    let message_id = message.verify().await.map_err(Error)?;
    if connection.has_message(&message_id).await.map_err(Error)? {
        return Ok(StatusCode::ACCEPTED);
    };

    // run type-dependent side effects
    match message.message_type {
        // activity expresses a follow relationship
        MessageType::Follow => handle_follow(&message, &mut connection)
            .await
            .map_err(Error)?,
        _ => (),
    }

    // store this message id for its audiences
    let audiences_id = build_audiences_id(&message).map_err(Error)?;
    for audience_id in audiences_id {
        connection
            .put_message_audience(&message_id, &audience_id)
            .await
            .map_err(Error)?;
    }

    // create an empty object in the DB which can be updated later
    connection
        .put_or_update_object(message.object.as_str(), None)
        .await
        .map_err(Error)?;

    // store the message itself
    let message = serde_json::to_string(&message).map_err(|x| Error(anyhow!(x)))?;
    connection
        .put_message(&message, &message_id, &actor_id)
        .await
        .map_err(Error)?;

    Ok(StatusCode::OK)
}

async fn handle_did_inbox(
    did: String,
    db_pool: Arc<DbPool>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(Error)?;
    let mut connection = db_pool
        .acquire()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;
    let messages = connection
        .get_inbox_for_actor(&actor_id, 32)
        .await
        .map_err(Error)?;
    let messages = messages
        .iter()
        .map(|x| serde_json::from_str(x).map_err(|x| anyhow!(x)))
        .collect::<Result<Vec<Message>>>()
        .map_err(Error)?;
    Ok(warp::reply::json(&messages))
}

fn id_from_followers(followers_id: &str) -> Result<String> {
    let (id, path) = followers_id
        .split_once("/")
        .ok_or(anyhow!("followers ID is not a path"))?;
    if path != "followers" {
        Err(anyhow!("followers ID is not a followers path"))?;
    }
    Ok(id.to_string())
}

async fn handle_did_following(
    did: String,
    db_pool: Arc<DbPool>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(Error)?;
    let mut connection = db_pool
        .acquire()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;
    let ids = connection
        .get_actor_audiences(&actor_id)
        .await
        .map_err(Error)?;
    let ids = ids
        .iter()
        .map(|x| id_from_followers(x))
        .collect::<Result<Vec<String>>>()
        .map_err(Error)?;
    Ok(warp::reply::json(&ids))
}

async fn handle_did_document(did: String) -> Result<impl warp::Reply, Rejection> {
    let (_, document, _) = DIDKey
        .resolve(&did, &ResolutionInputMetadata::default())
        .await;
    let document = document
        .ok_or(anyhow!("unable to interpret DID"))
        .map_err(Error)?;
    Ok(warp::reply::json(&document))
}

fn with_resource<T: Clone + Send>(
    x: T,
) -> impl Filter<Extract = (T,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || x.clone())
}

pub fn build_api(
    db_pool: Arc<DbPool>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    let route_version = warp::get().and(warp::path("version")).map(|| VERSION);
    let route_did_outbox = warp::post()
        .and(warp::path!("did" / String / "actor" / "outbox"))
        .and(warp::body::json())
        .and(with_resource(db_pool.clone()))
        .and_then(handle_did_outbox);
    let route_did_inbox = warp::get()
        .and(warp::path!("did" / String / "actor" / "inbox"))
        .and(with_resource(db_pool.clone()))
        .and_then(handle_did_inbox);
    let route_did_following = warp::get()
        .and(warp::path!("did" / String / "actor" / "following"))
        .and(with_resource(db_pool.clone()))
        .and_then(handle_did_following);
    let route_did_document = warp::get()
        .and(warp::path!("did" / String))
        .and_then(handle_did_document);
    route_version
        .or(route_did_outbox)
        .or(route_did_inbox)
        .or(route_did_following)
        .or(route_did_document)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use serde::Serialize;
    use serde_json::json;
    use ssi::jwk::JWK;
    use ssi::vc::URI;
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::chatternet::activities::{Message, MessageType};
    use crate::chatternet::didkey;
    use crate::db::new_db_pool;

    use super::*;

    const NO_VEC: Option<&Vec<String>> = None;

    async fn build_message(
        object_id: &str,
        to: Option<&impl Serialize>,
        cc: Option<&impl Serialize>,
        audience: Option<&impl Serialize>,
        jwk: &JWK,
    ) -> Message {
        let did = didkey::did_from_jwk(jwk).unwrap();
        let members = json!({
            "to": to,
            "cc": cc,
            "audience": audience,
        })
        .as_object()
        .unwrap()
        .to_owned();
        Message::new(&did, object_id, MessageType::Create, Some(members), &jwk)
            .await
            .unwrap()
    }

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
    async fn api_handles_version() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = request().method("GET").path("/version").reply(&api).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), VERSION);
    }

    #[tokio::test]
    async fn api_outbox_handles_message() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/did/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        // second post returns status accepted
        let response = request()
            .method("POST")
            .path(&format!("/did/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn api_outbox_rejects_wrong_did() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path("/did/did:example:a/actor/outbox")
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_outbox_rejects_invalid_message() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let mut message_2 = message.clone();
        message_2.id = Some(URI::from_str("id:a").unwrap());
        let response = request()
            .method("POST")
            .path(&format!("/did/{}/actor/outbox", did))
            .json(&message_2)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_outbox_rejects_with_bcc() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);

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
            .path(&format!("/did/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_outbox_rejects_with_bto() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);

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
            .path(&format!("/did/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    async fn build_follow(follow_id: &str, jwk: &JWK) -> Message {
        let did = didkey::did_from_jwk(jwk).unwrap();
        Message::new(&did, follow_id, MessageType::Follow, None, &jwk)
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn api_outbox_handles_follow() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        assert_eq!(
            request()
                .method("POST")
                .path(&format!("/did/{}/actor/outbox", did))
                .json(&build_follow("tag:1", &jwk).await)
                .reply(&api)
                .await
                .status(),
            StatusCode::OK
        );

        let response = request()
            .method("GET")
            .path(&format!("/did/{}/actor/following", did))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let ids: Vec<String> = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(ids, ["tag:1"]);
    }

    #[tokio::test]
    async fn api_inbox_returns_messages() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);

        let jwk_1 = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did_1 = didkey::did_from_jwk(&jwk_1).unwrap();

        let jwk_2 = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did_2 = didkey::did_from_jwk(&jwk_2).unwrap();

        // let jwk_3 = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        // let did_3 = didkey::did_from_jwk(&jwk_3).unwrap();

        // did_1 will see because follows self and this is addressed to self
        assert_eq!(
            request()
                .method("POST")
                .path(&format!("/did/{}/actor/outbox", did_1))
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
                .path(&format!("/did/{}/actor/outbox", did_1))
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
                .path(&format!("/did/{}/actor/outbox", did_1))
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
            .path(&format!("/did/{}/actor/inbox", did_1))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let messages: Vec<Message> = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            messages
                .iter()
                .map(|x| x.object.as_str())
                .collect::<Vec<&str>>(),
            ["id:1"]
        );

        // did_1 follows did_2, gets added to did_2 followers
        assert_eq!(
            request()
                .method("POST")
                .path(&format!("/did/{}/actor/outbox", did_1))
                .json(&build_follow(&format!("{}/actor", did_2), &jwk_1).await)
                .reply(&api)
                .await
                .status(),
            StatusCode::OK
        );

        let response = request()
            .method("GET")
            .path(&format!("/did/{}/actor/inbox", did_1))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let messages: Vec<Message> = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            messages
                .iter()
                .map(|x| x.object.as_str())
                .collect::<Vec<&str>>(),
            ["id:3", "id:1"]
        );
    }

    #[tokio::test]
    async fn api_did_document_build_document() {
        let db_pool = Arc::new(new_db_pool("sqlite::memory:").await.unwrap());
        let api = build_api(db_pool);
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let response = request()
            .method("GET")
            .path(&format!("/did/{}", did))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let document: serde_json::Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            document
                .as_object()
                .unwrap()
                .get("id")
                .unwrap()
                .as_str()
                .unwrap(),
            did
        );
    }
}
