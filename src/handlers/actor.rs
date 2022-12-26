use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use chatternet::didkey::actor_id_from_did;
use chatternet::model::{Actor, ActorFields, CollectionFields, CollectionType};
use ssi::vc::URI;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::error::AppError;
use crate::db::{self, Connector};

/// Get the Actor document with `did` using a DB connection obtained from
/// `connector`.
pub async fn handle_actor_get(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
) -> Result<Json<ActorFields>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let actor = db::get_document(&mut connection, &actor_id).await;
    match actor {
        Ok(Some(actor)) => {
            let actor: ActorFields =
                serde_json::from_str(&actor).map_err(|_| AppError::ActorNotValid)?;
            Ok(Json(actor))
        }
        _ => Err(AppError::ActorNotKnown),
    }
}

/// Post an Actor `actor` for the actor with `did`. Stores the document using
/// a DB connection obtained from `connector`.
pub async fn handle_actor_post(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
    Json(actor): Json<ActorFields>,
) -> Result<StatusCode, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    // the posted Actor must have the same ID as that in the path
    if actor.id().as_str() != actor_id {
        Err(AppError::ActorIdWrong)?;
    }
    if !actor.verify().await.is_ok() {
        Err(AppError::ActorNotValid)?;
    }
    let actor = serde_json::to_string(&actor).map_err(|_| AppError::ActorNotValid)?;
    db::put_document(&mut *connection, &actor_id, &actor)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    Ok(StatusCode::OK)
}

/// Get the collection of IDs followed by the actor with `did`.
pub async fn handle_actor_following(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
) -> Result<Json<CollectionFields<String>>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let ids = db::get_actor_followings(&mut *connection, &actor_id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    let uri =
        URI::try_from(format!("{}/following", actor_id)).map_err(|_| AppError::ActorIdWrong)?;
    let following = CollectionFields::new(uri, CollectionType::Collection, ids);
    Ok(Json(following))
}

#[cfg(test)]
mod test {
    use axum::http::StatusCode;
    use tap::Pipe;
    use tokio;
    use tower::ServiceExt;

    use chatternet::didkey::{build_jwk, did_from_jwk};
    use chatternet::model::{Actor, ActorFields, ActorType};

    use super::super::test_utils::*;

    #[tokio::test]
    async fn updates_and_gets_actor() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let actor = ActorFields::new(&jwk, ActorType::Person, Some("abc".to_string()), None)
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
        let actor_back: Option<ActorFields> = get_body(response).await;
        let actor_back = actor_back.unwrap();
        assert_eq!(actor_back.id(), actor.id());
    }

    #[tokio::test]
    async fn wont_update_invalid_actor() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let actor = ActorFields::new(&jwk, ActorType::Person, Some("abc".to_string()), None)
            .await
            .unwrap();

        // did doesn't match actor ID
        let response = api
            .clone()
            .oneshot(request_json("POST", "/api/ap/did:example:a/actor", &actor))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let mut invalid = serde_json::to_value(&actor).unwrap();
        invalid.get_mut("name").map(|x| {
            *x = "abcd"
                .to_string()
                .pipe(Some)
                .pipe(serde_json::to_value)
                .unwrap()
        });

        // build an invalid actor
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor", did),
                &invalid,
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
