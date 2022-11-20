use anyhow::Result;
use did_method_key::DIDKey;
use ssi::did_resolve::{DIDResolver, ResolutionInputMetadata};
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::http::StatusCode;
use warp::Rejection;

use super::error::Error;
use crate::chatternet::activities::{actor_id_from_did, Actor, Collection, CollectionType};
use crate::db::{self, Connector};

pub async fn handle_did_document(did: String) -> Result<impl warp::Reply, Rejection> {
    let (_, document, _) = DIDKey
        .resolve(&did, &ResolutionInputMetadata::default())
        .await;
    let document = document.ok_or(Error::DidNotValid)?;
    Ok(warp::reply::json(&document))
}

pub async fn handle_did_actor_get(
    did: String,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(|_| Error::DidNotValid)?;
    // read only
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| Error::DbConnectionFailed)?;
    if !db::has_object(&mut connection, &actor_id)
        .await
        .map_err(|_| Error::DbQueryFailed)?
    {
        Err(Error::ActorNotKnown)?;
    }
    let actor = db::get_object(&mut connection, &actor_id)
        .await
        .map_err(|_| Error::DbQueryFailed)?;
    match actor {
        Some(actor) => {
            let actor: Actor = serde_json::from_str(&actor).map_err(|_| Error::ActorNotValid)?;
            Ok(warp::reply::json(&actor))
        }
        None => Ok(warp::reply::json(&serde_json::Value::Null)),
    }
}

pub async fn handle_did_actor_post(
    did: String,
    actor: Actor,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(|_| Error::DidNotValid)?;
    // read write
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| Error::DbConnectionFailed)?;
    if actor_id.as_str() != actor_id {
        Err(Error::ActorIdWrong)?;
    }
    if !db::has_object(&mut *connection, &actor_id)
        .await
        .map_err(|_| Error::DbQueryFailed)?
    {
        Err(Error::ActorNotKnown)?;
    }
    if !actor.verify().await.is_ok() {
        Err(Error::ActorNotValid)?;
    }
    let actor = serde_json::to_string(&actor).map_err(|_| Error::ActorNotValid)?;
    db::put_or_update_object(&mut *connection, &actor_id, Some(&actor))
        .await
        .map_err(|_| Error::DbQueryFailed)?;
    Ok(StatusCode::OK)
}

pub async fn handle_did_following(
    did: String,
    connector: Arc<RwLock<Connector>>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_id = actor_id_from_did(&did).map_err(|_| Error::DidNotValid)?;
    // read only
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| Error::DbConnectionFailed)?;
    let ids = {
        let mut ids = db::get_actor_audiences(&mut *connection, &actor_id)
            .await
            .map_err(|_| Error::DbQueryFailed)?;
        let mut ids_contact = db::get_actor_contacts(&mut *connection, &actor_id)
            .await
            .map_err(|_| Error::DbQueryFailed)?;
        ids.append(&mut ids_contact);
        ids
    };
    let following = Collection::new(
        &format!("{}/following", actor_id),
        CollectionType::Collection,
        ids,
    )
    .map_err(|_| Error::DbQueryFailed)?;
    Ok(warp::reply::json(&following))
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::chatternet::activities::{Actor, ActorType};
    use crate::chatternet::didkey;
    use crate::db::Connector;

    use super::super::build_api;
    use super::super::test::build_message;
    use super::*;

    const NO_VEC: Option<&Vec<String>> = None;

    #[tokio::test]
    async fn api_did_document_build_document() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());
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
    async fn api_actor_updates_and_gets() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());

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
        let api = build_api(connector, "did:example:server".to_string());

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
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let mut actor_invalid = actor.clone();
        actor_invalid.members = Some(json!({"name": "abcd"}).as_object().unwrap().to_owned());
        let response = request()
            .method("POST")
            .path(&format!("/{}/actor", did))
            .json(&actor_invalid)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_actor_wont_get_unknown() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());
        let response = request()
            .method("GET")
            .path("/did:example:a/actor")
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn api_actor_wont_post_unknown() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector, "did:example:server".to_string());
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
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
