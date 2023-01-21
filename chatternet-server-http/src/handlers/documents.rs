//! Handle modifying documents.
//!
//! Documents include messages and bodies. Actors are handled separately.

use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use did_method_key::DIDKey;
use serde_json::Value;
use ssi::did_resolve::{DIDResolver, ResolutionInputMetadata};
use tap::Pipe;

use super::error::AppError;
use super::AppState;
use crate::db::{self};
use chatternet::model::{NoteMd1k, NoteMd1kFields};

/// Handle a get request for a document with ID `id`.
///
/// Generates a DID document if a the ID is a DID, otherwise will lookup
/// the ID in the document table.
pub async fn handle_document_get(
    State(AppState { connector, .. }): State<AppState>,
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

    db::get_document(&mut connection, &id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?
        .map(|x| serde_json::from_str::<Value>(&x).map_err(|_| AppError::DocumentNotValid))
        .transpose()?
        .ok_or(AppError::DocumentNotKnown)?
        .pipe(Json)
        .pipe(Ok)
}

/// Handle a post request for a message body `body` with ID `id`.
pub async fn handle_body_post(
    State(AppState { connector, .. }): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<NoteMd1kFields>,
) -> Result<StatusCode, AppError> {
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    if body.id().as_str() != id {
        Err(AppError::DocumentIdWrong)?;
    }
    // only accept body if a known (signed) message is associated with it
    if !db::has_message_with_body(&mut *connection, &id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?
    {
        Err(AppError::DocumentNotKnown)?;
    }
    // validate the body CID
    if !body.verify().await.is_ok() {
        Err(AppError::DocumentNotValid)?;
    }
    let body = serde_json::to_string(&body).map_err(|_| AppError::DocumentNotValid)?;
    db::put_document(&mut *connection, &id, &body)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    Ok(StatusCode::OK)
}

/// Handle a get request for a the create message for document with ID `id`.
///
/// Returns the last create message by the actor with `did`.
pub async fn handle_document_get_create(
    State(AppState { connector, .. }): State<AppState>,
    Path((id, did)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let actor_id = format!("{}/actor", did);
    let message_id = db::get_body_messages(&mut connection, &id, Some(&actor_id))
        .await
        .map_err(|_| AppError::DbQueryFailed)?
        .pipe(|x| x.into_iter().last());
    if let Some(message_id) = message_id {
        db::get_document(&mut connection, &message_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?
            .map(|x| serde_json::from_str::<Value>(&x).map_err(|_| AppError::DocumentNotValid))
            .transpose()?
            .ok_or(AppError::DocumentNotKnown)?
            .pipe(Json)
            .pipe(Ok)
    } else {
        Err(AppError::DocumentNotKnown)?
    }
}

#[cfg(test)]
mod test {
    use axum::body::Body as HttpBody;
    use axum::http::{Request, StatusCode};
    use tap::Pipe;
    use tokio;
    use tower::ServiceExt;

    use chatternet::didkey::{build_jwk, did_from_jwk};
    use chatternet::model::{Message, MessageFields, NoteMd1k, NoteMd1kFields, NoteType};

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
    async fn body_updates_and_gets() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let body = NoteMd1kFields::new(
            NoteType::Note,
            "abc".to_string(),
            "did:example:a".to_string().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let body_id = body.id().as_str();
        let message = build_message(&jwk, body_id, None).await;

        // post a message so that the server knows about the body
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

        // now possible to post the body on the server
        let response = api
            .clone()
            .oneshot(request_json("POST", &format!("/api/ap/{}", body_id), &body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // server returns the body
        let response = api
            .clone()
            .oneshot(request_empty("GET", &format!("/api/ap/{}", body_id)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body_back: Option<NoteMd1kFields> = get_body(response).await;
        let body_back = body_back.unwrap();
        assert_eq!(body_back.id(), body.id());
    }

    #[tokio::test]
    async fn body_gets_message_from_creator() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let actor_id = format!("{}/actor", did);

        let body = NoteMd1kFields::new(
            NoteType::Note,
            "abc".to_string(),
            actor_id.clone().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let body_id = body.id().as_str();
        let message = build_message(&jwk, body_id, None).await;

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

        let response = api
            .clone()
            .oneshot(request_json("POST", &format!("/api/ap/{}", body_id), &body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // server returns the message fo the body
        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}/createdBy/{}", body_id, actor_id),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let message_back: Option<MessageFields> = get_body(response).await;
        let message_back = message_back.unwrap();
        assert_eq!(message_back.id(), message.id());
    }

    #[tokio::test]
    async fn wont_update_invalid_body() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let body = NoteMd1kFields::new(
            NoteType::Note,
            "abc".to_string(),
            "did:example:a".to_string().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let body_id = body.id().as_str();
        let message = build_message(&jwk, body_id, None).await;

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
            .oneshot(request_json("POST", "/api/ap/urn:cid:invalid", &body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // body contents don't match ID
        let mut invalid = serde_json::to_value(&body).unwrap();
        invalid.get_mut("content").map(|x| {
            *x = "abcd"
                .to_string()
                .pipe(Some)
                .pipe(serde_json::to_value)
                .unwrap()
        });

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", body_id),
                &invalid,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn wont_get_unknown() {
        let api = build_test_api().await;
        let response = api
            .clone()
            .oneshot(request_empty("GET", "/api/ap/urn:cid:invalid"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn wont_post_unknown() {
        let api = build_test_api().await;
        let body = NoteMd1kFields::new(
            NoteType::Note,
            "abc".to_string(),
            "did:example:a".to_string().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let document_id = body.id().as_str();
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", document_id),
                &body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
