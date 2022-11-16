use anyhow::{anyhow, Result};
use did_method_key::DIDKey;
use serde::{Deserialize, Serialize};
use sqlx::Acquire;
use ssi::did_resolve::{DIDResolver, ResolutionInputMetadata};
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::http::StatusCode;
use warp::hyper::Method;
use warp::{Filter, Rejection};

use crate::chatternet::activities::{
    actor_id_from_did, ActivityType, Actor, Inbox, Message, Object,
};
use crate::db::{self, Connection, Connector};
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
    let objects_id: Vec<&str> = message.object.iter().map(|x| x.as_str()).collect();
    for object_id in objects_id {
        // following a did, it as a contact for filtering
        if object_id.starts_with("did:") {
            db::put_actor_contact(&mut *connection, &actor_id, &object_id).await?;
        }
        // following any id, add to its followers collection
        db::put_actor_audience(
            &mut *connection,
            &actor_id,
            &format!("{}/followers", object_id),
        )
        .await?;
    }
    Ok(())
}

async fn handle_did_outbox(
    did: String,
    message: Message,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(Error)?;
    if actor_id != message.actor.as_str() {
        Err(anyhow!("posting to the wrong outbox for the message actor")).map_err(Error)?;
    }

    // read write
    let mut connector = connector.write().await;
    let mut transaction = connector.transaction_mut().await.map_err(Error)?;
    let connection = transaction
        .acquire()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;

    // if already known, take no actions
    let message_id = message.verify().await.map_err(Error)?;
    if db::has_message(&mut *connection, &message_id)
        .await
        .map_err(Error)?
    {
        return Ok(StatusCode::ACCEPTED);
    };

    // run type-dependent side effects
    match message.message_type {
        // activity expresses a follow relationship
        ActivityType::Follow => handle_follow(&message, &mut *connection)
            .await
            .map_err(Error)?,
        _ => (),
    }

    // store this message id for its audiences
    let audiences_id = build_audiences_id(&message).map_err(Error)?;
    for audience_id in audiences_id {
        db::put_message_audience(&mut *connection, &message_id, &audience_id)
            .await
            .map_err(Error)?;
    }

    // create empty objects in the DB which can be updated later
    let objects_id: Vec<&str> = message.object.iter().map(|x| x.as_str()).collect();
    for object_id in objects_id {
        db::put_or_update_object(&mut *connection, object_id, None)
            .await
            .map_err(Error)?;
    }

    // create an empty object for the actor which can be updated later
    db::put_or_update_object(&mut *connection, message.actor.as_str(), None)
        .await
        .map_err(Error)?;

    // store the message itself
    let message = serde_json::to_string(&message).map_err(|x| Error(anyhow!(x)))?;
    db::put_message(&mut *connection, &message, &message_id, &actor_id)
        .await
        .map_err(Error)?;

    transaction
        .commit()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;

    Ok(StatusCode::OK)
}

#[derive(Deserialize, Serialize)]
struct DidInboxQuery {
    after: String,
}

async fn handle_did_inbox(
    did: String,
    query: Option<DidInboxQuery>,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(Error)?;
    // read only
    let connector = connector.read().await;
    let mut connection = connector.connection().await.map_err(Error)?;
    let after = query.as_ref().map(|x| x.after.as_str());
    let messages = db::get_inbox_for_actor(
        &mut connection,
        &actor_id,
        32,
        query.as_ref().map(|x| x.after.as_str()),
    )
    .await
    .map_err(Error)?;
    let messages = messages
        .iter()
        .map(|x| serde_json::from_str(x).map_err(|x| anyhow!(x)))
        .collect::<Result<Vec<Message>>>()
        .map_err(Error)?;
    let inbox = Inbox::new(&actor_id, messages, after).map_err(Error)?;
    Ok(warp::reply::json(&inbox))
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
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(Error)?;
    // read only
    let connector = connector.read().await;
    let mut connection = connector.connection().await.map_err(Error)?;
    let ids = db::get_actor_audiences(&mut *connection, &actor_id)
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

async fn handle_object_get(
    object_id: String,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    // read only
    let connector = connector.read().await;
    let mut connection = connector.connection().await.map_err(Error)?;
    if !db::has_object(&mut connection, &object_id)
        .await
        .map_err(Error)?
    {
        Err(Error(anyhow!("requested object is not known")))?;
    }
    let object = db::get_object(&mut connection, &object_id)
        .await
        .map_err(Error)?;
    match object {
        Some(object) => {
            let object: Object = serde_json::from_str(&object)
                .map_err(|x| anyhow!(x))
                .map_err(Error)?;
            Ok(warp::reply::json(&object))
        }
        None => Ok(warp::reply::json(&serde_json::Value::Null)),
    }
}

async fn handle_object_post(
    object_id: String,
    object: Object,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    // read write
    let mut connector = connector.write().await;
    let mut transaction = connector.transaction_mut().await.map_err(Error)?;
    let connection = transaction
        .acquire()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;
    if object_id.is_empty()
        || object
            .id
            .as_ref()
            .map(|x| x.as_str() != object_id)
            .unwrap_or(true)
    {
        Err(Error(anyhow!("posted object has wrong ID")))?;
    }
    if !db::has_object(connection, &object_id)
        .await
        .map_err(Error)?
    {
        Err(Error(anyhow!("posted object is not known")))?;
    }
    if !object.verify().await.is_ok() {
        Err(Error(anyhow!("posted object ID doesn't match contents")))?;
    }
    let object = serde_json::to_string(&object).map_err(|x| Error(anyhow!(x)))?;
    db::put_or_update_object(connection, &object_id, Some(&object))
        .await
        .map_err(Error)?;
    transaction
        .commit()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;
    Ok(StatusCode::OK)
}

async fn handle_did_actor_get(
    did: String,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(Error)?;
    // read only
    let connector = connector.read().await;
    let mut connection = connector.connection().await.map_err(Error)?;
    if !db::has_object(&mut connection, &actor_id)
        .await
        .map_err(Error)?
    {
        Err(Error(anyhow!("requested actor is not known")))?;
    }
    let actor = db::get_object(&mut connection, &actor_id)
        .await
        .map_err(Error)?;
    match actor {
        Some(actor) => {
            let actor: Actor = serde_json::from_str(&actor)
                .map_err(|x| anyhow!(x))
                .map_err(Error)?;
            Ok(warp::reply::json(&actor))
        }
        None => Ok(warp::reply::json(&serde_json::Value::Null)),
    }
}

async fn handle_did_actor_post(
    did: String,
    actor: Actor,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(Error)?;
    // read write
    let mut connector = connector.write().await;
    let mut transaction = connector.transaction_mut().await.map_err(Error)?;
    let connection = transaction
        .acquire()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;
    if actor_id.as_str() != actor_id {
        Err(Error(anyhow!("posted actor has wrong ID")))?;
    }
    if !db::has_object(connection, &actor_id).await.map_err(Error)? {
        Err(Error(anyhow!("posted actor is not known")))?;
    }
    if !actor.verify().await.is_ok() {
        Err(Error(anyhow!("posted actor ID contents are invalid")))?;
    }
    let actor = serde_json::to_string(&actor).map_err(|x| Error(anyhow!(x)))?;
    db::put_or_update_object(connection, &actor_id, Some(&actor))
        .await
        .map_err(Error)?;
    transaction
        .commit()
        .await
        .map_err(|x| anyhow!(x))
        .map_err(Error)?;
    Ok(StatusCode::OK)
}

fn with_resource<T: Clone + Send>(
    x: T,
) -> impl Filter<Extract = (T,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || x.clone())
}

pub fn build_api(
    connector: Arc<RwLock<Connector>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    let route_version = warp::get().and(warp::path("version")).map(|| VERSION);
    let route_did_outbox = warp::post()
        .and(warp::path!(String / "actor" / "outbox"))
        .and(warp::body::json())
        .and(with_resource(connector.clone()))
        .and_then(handle_did_outbox);
    let route_did_inbox = warp::get()
        .and(warp::path!(String / "actor" / "inbox"))
        .and(with_resource(connector.clone()))
        .and_then(|did, connector| handle_did_inbox(did, None, connector));
    let route_did_inbox_query = warp::get()
        .and(warp::path!(String / "actor" / "inbox"))
        .and(warp::query::<DidInboxQuery>())
        .and(with_resource(connector.clone()))
        .and_then(|did, query, connector| handle_did_inbox(did, Some(query), connector));
    let route_did_following = warp::get()
        .and(warp::path!(String / "actor" / "following"))
        .and(with_resource(connector.clone()))
        .and_then(handle_did_following);
    let route_did_document = warp::get()
        .and(warp::path!(String))
        .and_then(handle_did_document);
    let route_object_get = warp::get()
        .and(warp::path!(String))
        .and(with_resource(connector.clone()))
        .and_then(handle_object_get);
    let route_object_post = warp::post()
        .and(warp::path!(String))
        .and(warp::body::json())
        .and(with_resource(connector.clone()))
        .and_then(handle_object_post);
    let route_did_actor_get = warp::get()
        .and(warp::path!(String / "actor"))
        .and(with_resource(connector.clone()))
        .and_then(handle_did_actor_get);
    let route_did_actor_post = warp::post()
        .and(warp::path!(String / "actor"))
        .and(warp::body::json())
        .and(with_resource(connector.clone()))
        .and_then(handle_did_actor_post);
    let cors = warp::cors()
        .allow_any_origin()
        .allow_header("content-type")
        .allow_methods(&[Method::GET, Method::POST, Method::OPTIONS]);
    let log = warp::log("chatternet::api");
    route_version
        .or(route_did_outbox)
        .or(route_did_inbox)
        .or(route_did_inbox_query)
        .or(route_did_following)
        .or(route_did_document)
        .or(route_object_get)
        .or(route_object_post)
        .or(route_did_actor_get)
        .or(route_did_actor_post)
        .with(cors)
        .with(log)
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

    use crate::chatternet::activities::{ActivityType, ActorType, Message, ObjectType};
    use crate::chatternet::didkey;
    use crate::db::Connector;

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
        Message::new(
            &did,
            &[object_id],
            ActivityType::Create,
            None,
            Some(members),
            &jwk,
        )
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
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = request().method("GET").path("/version").reply(&api).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), VERSION);
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
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
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
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
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
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
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
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    async fn build_follow(follows_id: &[&str], jwk: &JWK) -> Message {
        let did = didkey::did_from_jwk(jwk).unwrap();
        Message::new(&did, follows_id, ActivityType::Follow, None, None, &jwk)
            .await
            .unwrap()
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

    #[tokio::test]
    async fn api_inbox_returns_messages() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

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

    #[tokio::test]
    async fn api_did_document_build_document() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let response = request()
            .method("GET")
            .path(&format!("/{}", did))
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

    #[tokio::test]
    async fn api_object_updates_and_gets() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
        let message = build_message(object_id, NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("GET")
            .path(&format!("/{}", object_id))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let object_back: Option<Object> = serde_json::from_slice(response.body()).unwrap();
        assert!(object_back.is_none());

        let response = request()
            .method("POST")
            .path(&format!("/{}", object_id))
            .json(&object)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("GET")
            .path(&format!("/{}", object_id))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let object_back: Option<Object> = serde_json::from_slice(response.body()).unwrap();
        let object_back = object_back.unwrap();
        assert_eq!(object_back.id, object.id);
    }

    #[tokio::test]
    async fn api_object_wont_update_invalid_object() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
        let message = build_message(object_id, NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("POST")
            .path("/id:wrong")
            .json(&object)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let mut object_invalid = object.clone();
        object_invalid.members = Some(json!({"content": "abcd"}).as_object().unwrap().to_owned());
        let response = request()
            .method("POST")
            .path(&format!("/{}", object_id))
            .json(&object_invalid)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_object_wont_get_unknown() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);
        let response = request().method("GET").path("/id:1").reply(&api).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_object_wont_post_unknown() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);
        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
        let response = request()
            .method("POST")
            .path(&format!("/{}", object_id))
            .json(&object)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_actor_updates_and_gets() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let actor = Actor::new(
            did.to_string(),
            ActorType::Person,
            Some(members),
            Some(&jwk),
        )
        .await
        .unwrap();
        let actor_id = actor.id.as_str();
        let message = build_message(actor_id, NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("GET")
            .path(&format!("/{}/actor", did))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let actor_back: Option<Actor> = serde_json::from_slice(response.body()).unwrap();
        assert!(actor_back.is_none());

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor", did))
            .json(&actor)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("GET")
            .path(&format!("/{}/actor", did))
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let actor_back: Option<Actor> = serde_json::from_slice(response.body()).unwrap();
        let actor_back = actor_back.unwrap();
        assert_eq!(actor_back.id, actor.id);
    }

    #[tokio::test]
    async fn api_actor_wont_update_invalid_actor() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let actor = Actor::new(
            did.to_string(),
            ActorType::Person,
            Some(members),
            Some(&jwk),
        )
        .await
        .unwrap();
        let actor_id = actor.id.as_str();
        let message = build_message(actor_id, NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/{}/actor/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("POST")
            .path("/did:example:a/actor")
            .json(&actor)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let mut actor_invalid = actor.clone();
        actor_invalid.members = Some(json!({"name": "abcd"}).as_object().unwrap().to_owned());
        let response = request()
            .method("POST")
            .path(&format!("/{}/actor", did))
            .json(&actor_invalid)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_actor_wont_get_unknown() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);
        let response = request()
            .method("GET")
            .path("/did:example:a/actor")
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_actor_wont_post_unknown() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let actor = Actor::new(did.to_string(), ActorType::Person, None, None)
            .await
            .unwrap();
        let response = request()
            .method("POST")
            .path(&format!("/{}/actor", did))
            .json(&actor)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
