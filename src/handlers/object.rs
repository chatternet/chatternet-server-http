use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use did_method_key::DIDKey;
use serde_json::Value;
use ssi::did_resolve::{DIDResolver, ResolutionInputMetadata};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::error::AppError;
use crate::chatternet::activities::Object;
use crate::db::{self, Connector};

pub async fn handle_object_get(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    // if this is just a DID, generate its corresponding DID document
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

    // objects (without an `/actor` path) are stored in the `Message` or
    // `Object` tables, check each for the ID and return if found

    let object = db::get_message(&mut connection, &id).await;
    if let Ok(Some(object)) = object {
        let object: Value = serde_json::from_str(&object).map_err(|_| AppError::MessageNotValid)?;
        return Ok(Json(object));
    }

    let object = db::get_object(&mut connection, &id).await;
    if let Ok(Some(object)) = object {
        let object: Value = serde_json::from_str(&object).map_err(|_| AppError::ObjectNotValid)?;
        return Ok(Json(object));
    }

    Err(AppError::ObjectNotKnown)?
}

pub async fn handle_object_post(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(id): Path<String>,
    Json(object): Json<Object>,
) -> Result<StatusCode, AppError> {
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;

    if object.id.as_ref().map(|x| x.as_str() != id).unwrap_or(true) {
        Err(AppError::ObjectIdWrong)?;
    }
    // server doesn't accept unsigned data, since objects are unsigned, the
    // server must first know about a (signed) message that has that object
    if !db::has_object(&mut *connection, &id)
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
    db::put_or_update_object(&mut *connection, &id, Some(&object))
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod test {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tokio;
    use tower::ServiceExt;

    use crate::chatternet::activities::{Object, ObjectType};
    use crate::chatternet::didkey;

    use super::super::test_utils::*;

    #[tokio::test]
    async fn builds_did_document() {
        let api = build_test_api().await;
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let response = api
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/api/ap/{}", did))
                    .body(Body::empty())
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

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
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
        let object_back: Option<Object> = get_body(response).await;
        let object_back = object_back.unwrap();
        assert_eq!(object_back.id, object.id);
    }

    #[tokio::test]
    async fn api_object_wont_update_invalid_object() {
        let api = build_test_api().await;

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
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
        let mut object_invalid = object.clone();
        object_invalid.members = Some(json!({"content": "abcd"}).as_object().unwrap().to_owned());
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", object_id),
                &object_invalid,
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
        let object = Object::new(ObjectType::Note, None).await.unwrap();
        let object_id = object.id.as_ref().unwrap().as_str();
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
