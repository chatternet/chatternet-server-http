//! Handle modifying object state.
//!
//! Objects include messages and bodies. Actors are handled separately.

use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use did_method_key::DIDKey;
use serde_json::Value;
use ssi::did_resolve::{DIDResolver, ResolutionInputMetadata};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::error::AppError;
use crate::db::{self, Connector};
use chatternet::model::Body;

/// Handle a get request for an object with ID `id`.
///
/// Generates a DID document if a the ID is a DID, otherwise will lookup
/// the ID in the object table which contains messages and bodies.
pub async fn handle_object_get(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    // if this is just a DID, generate its corresponding DID document
    // note that IDs with the `/actor` suffix is handled in a different route
    if id.starts_with("did:key:") {
        let (_, document, _) = DIDKey
            .resolve(&id, &ResolutionInputMetadata::default())
            .await;
        let document = document.ok_or(AppError::DidNotValid)?;
        return Ok(Json(
            serde_json::to_value(&document).map_err(|_| AppError::DidNotValid)?,
        ));
    }

    // otherwise will need read-only access to retrieve from DB
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;

    let object = db::get_object(&mut connection, &id).await;
    if let Ok(Some(object)) = object {
        let object: Value = serde_json::from_str(&object).map_err(|_| AppError::ObjectNotValid)?;
        return Ok(Json(object));
    }

    Err(AppError::ObjectNotKnown)?
}

/// Handle a post request for an object `object` with ID `id`.
pub async fn handle_object_post(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(id): Path<String>,
    Json(object): Json<Body>,
) -> Result<StatusCode, AppError> {
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;

    if object.id.as_str() != id {
        Err(AppError::ObjectIdWrong)?;
    }
    // only accept object if a known (signed) message is associated with it
    if !db::has_message_with_body(&mut *connection, &id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?
    {
        Err(AppError::ObjectNotKnown)?;
    }
    // validate the object CID
    if !object.verify().await.is_ok() {
        Err(AppError::ObjectNotValid)?;
    }
    let object = serde_json::to_string(&object).map_err(|_| AppError::ObjectNotValid)?;
    db::put_object(&mut *connection, &id, &object)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod test {
    use axum::body::Body as HttpBody;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio;
    use tower::ServiceExt;

    use chatternet::didkey::{build_jwk, did_from_jwk};
    use chatternet::model::{Body, BodyNoId, BodyType};

    use super::super::test_utils::*;

    #[tokio::test]
    async fn builds_did_document() {
        let api = build_test_api().await;
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let response = api
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/ap/{}", did))
                    .body(HttpBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let document: serde_json::Value = get_body(response).await;
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
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let object = Body::new(BodyType::Note, None).await.unwrap();
        let object_id = object.id.as_str();
        let message = build_message(object_id, NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        // post a message so that the server knows about the object
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // now possible to post the object on the server
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", object_id),
                &object,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // server returns the object
        let response = api
            .clone()
            .oneshot(request_empty("GET", &format!("/api/ap/{}", object_id)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let object_back: Option<Body> = get_body(response).await;
        let object_back = object_back.unwrap();
        assert_eq!(object_back.id, object.id);
    }

    #[tokio::test]
    async fn api_object_wont_update_invalid_object() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let object = Body::new(BodyType::Note, None).await.unwrap();
        let object_id = object.id.as_str();
        let message = build_message(object_id, NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // posting to wrong ID
        let response = api
            .clone()
            .oneshot(request_json("POST", "/api/ap/urn:cid:invalid", &object))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // object contents don't match ID
        let members = json!({"content": "abcd"}).as_object().unwrap().to_owned();
        let object = Body {
            id: object.id.clone(),
            no_id: BodyNoId {
                members: Some(members),
                ..object.no_id
            },
        };

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", object_id),
                &object,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_object_wont_get_unknown() {
        let api = build_test_api().await;
        let response = api
            .clone()
            .oneshot(request_empty("GET", "/api/ap/urn:cid:invalid"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn api_object_wont_post_unknown() {
        let api = build_test_api().await;
        let object = Body::new(BodyType::Note, None).await.unwrap();
        let object_id = object.id.as_str();
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", object_id),
                &object,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
