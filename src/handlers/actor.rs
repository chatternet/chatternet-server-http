use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use chatternet::didkey::actor_id_from_did;
use chatternet::model::{Actor, Collection, CollectionType};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::error::AppError;
use crate::db::{self, Connector};

/// Get the Actor Object with `did` using a DB connection obtained from
/// `connector`.
pub async fn handle_actor_get(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
) -> Result<Json<Actor>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let actor = db::get_object(&mut connection, &actor_id).await;
    match actor {
        Ok(Some(actor)) => {
            let actor: Actor = serde_json::from_str(&actor).map_err(|_| AppError::ActorNotValid)?;
            Ok(Json(actor))
        }
        _ => Err(AppError::ActorNotKnown),
    }
}

/// Post an Actor Object `actor` for the actor with `did`. Stores the object
/// using a DB connection obtained from `connector`.
pub async fn handle_actor_post(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
    Json(actor): Json<Actor>,
) -> Result<StatusCode, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    // the posted Actor Object must have the same ID as that in the path
    if actor.no_proof.id.as_str() != actor_id {
        Err(AppError::ActorIdWrong)?;
    }
    if !actor.verify().await.is_ok() {
        Err(AppError::ActorNotValid)?;
    }
    let actor = serde_json::to_string(&actor).map_err(|_| AppError::ActorNotValid)?;
    db::put_object(&mut *connection, &actor_id, &actor)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    Ok(StatusCode::OK)
}

/// Get the collection of IDs followed by the actor with `did`.
pub async fn handle_actor_following(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
) -> Result<Json<Collection<String>>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let ids = db::get_actor_followings(&mut *connection, &actor_id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    let following = Collection::new(
        &format!("{}/following", actor_id),
        CollectionType::Collection,
        ids,
    )
    .map_err(|_| AppError::DbQueryFailed)?;
    Ok(Json(following))
}

#[cfg(test)]
mod test {
    use axum::http::StatusCode;
    use serde_json::json;
    use tokio;
    use tower::ServiceExt;

    use chatternet::didkey::{build_jwk, did_from_jwk};
    use chatternet::model::{Actor, ActorNoProof, ActorType};

    use super::super::test_utils::*;

    #[tokio::test]
    async fn updates_and_gets_actor() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let actor = Actor::new(did.to_string(), ActorType::Person, &jwk, Some(members))
            .await
            .unwrap();

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor", did),
                &actor,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty("GET", &format!("/api/ap/{}/actor", did)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let actor_back: Option<Actor> = get_body(response).await;
        let actor_back = actor_back.unwrap();
        assert_eq!(actor_back.no_proof.id, actor.no_proof.id);
    }

    #[tokio::test]
    async fn wont_update_invalid_actor() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let actor = Actor::new(did.to_string(), ActorType::Person, &jwk, Some(members))
            .await
            .unwrap();

        // did doesn't match actor ID
        let response = api
            .clone()
            .oneshot(request_json("POST", "/api/ap/did:example:a/actor", &actor))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // build an invalid actor
        let members = json!({"name": "abcd"}).as_object().unwrap().to_owned();
        let actor = Actor {
            proof: actor.proof,
            no_proof: ActorNoProof {
                members: Some(members),
                ..actor.no_proof
            },
        };

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor", did),
                &actor,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn wont_get_unknown_actor() {
        let api = build_test_api().await;
        let response = api
            .clone()
            .oneshot(request_empty("GET", "/api/ap/did:example:a/actor"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
