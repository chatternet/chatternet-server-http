//! Handle modifying documents.
//!
//! Documents include messages and bodies. Actors are handled separately.

use anyhow::Result;
use async_trait::async_trait;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use chatternet::didkey::is_valid_did;
use did_method_key::DIDKey;
use serde_json::Value;
use ssi::did_resolve::{DIDResolver, ResolutionInputMetadata};
use tap::Pipe;

use super::error::AppError;
use super::AppState;
use crate::db::{self};
use chatternet::model::{Document, NoteMd1kFields, Tag30Fields, Uri};

use serde::{Deserialize, Serialize};

/// The document types with CID identifiers handled by the server.
///
/// Must list from most specific to most generic as serde will try to
/// deserialize into each type in order since the variants are untagged.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServerCidDocument {
    Tag30(Tag30Fields),
    NoteMd1k(NoteMd1kFields),
}

#[async_trait]
impl Document for ServerCidDocument {
    fn id(&self) -> &Uri {
        match self {
            ServerCidDocument::Tag30(x) => x.id(),
            ServerCidDocument::NoteMd1k(x) => x.id(),
        }
    }
    async fn verify(&self) -> Result<()> {
        match self {
            ServerCidDocument::Tag30(x) => x.verify().await,
            ServerCidDocument::NoteMd1k(x) => x.verify().await,
        }
    }
}

/// Handle a get request for a document with ID `id`.
///
/// Generates a DID document if a the ID is a DID, otherwise will lookup
/// the ID in the document table.
pub async fn handle_document_get(
    State(AppState { connector, .. }): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    // if this is DID, generate its corresponding DID document
    if is_valid_did(id.as_str()) {
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

/// Handle a post request for a message `document` with ID `id`.
pub async fn handle_document_post(
    State(AppState { connector, .. }): State<AppState>,
    Path(id): Path<String>,
    Json(document): Json<ServerCidDocument>,
) -> Result<StatusCode, AppError> {
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    // this handler handles only CID documents only
    if !id.starts_with("urn:cid:") {
        Err(AppError::DocumentIdWrong)?;
    }
    if document.id().as_str() != id {
        Err(AppError::DocumentIdWrong)?;
    }
    // only accept document if a known (signed) message is associated with it
    if !db::has_message_with_document(&mut *connection, &id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?
    {
        Err(AppError::DocumentNotKnown)?;
    }
    if !document.verify().await.is_ok() {
        Err(AppError::DocumentNotValid)?;
    }
    let document = serde_json::to_string(&document).map_err(|_| AppError::DocumentNotValid)?;
    // this handler handles only CID documents whose content cannot change
    // (since it is encoded in the ID), so there is no need to update
    db::put_document_if_new(&mut *connection, &id, &document)
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
    let message_id = db::get_document_messages(&mut connection, &id, Some(&actor_id))
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
    use chatternet::model::{Document, Message, MessageFields, NoteMd1kFields, Tag30Fields};

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
    async fn document_updates_and_gets() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let object = NoteMd1kFields::new(
            "abc".to_string(),
            "did:example:a".to_string().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let object_id = object.id().as_str();

        let tag = Tag30Fields::new("abc".to_string()).await.unwrap();
        let tag_id = tag.id().as_str();

        let message = build_message(&jwk, object_id, Some(vec![tag_id.to_string()])).await;

        // post a message so that the server knows about the document
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
        let object_back: Option<NoteMd1kFields> = get_body(response).await;
        let object_back = object_back.unwrap();
        assert_eq!(object_back.id(), object.id());

        // now possible to post the tag on the server
        let response = api
            .clone()
            .oneshot(request_json("POST", &format!("/api/ap/{}", tag_id), &tag))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // server returns the tag
        let response = api
            .clone()
            .oneshot(request_empty("GET", &format!("/api/ap/{}", tag_id)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let tag_back: Option<Tag30Fields> = get_body(response).await;
        let tag_back = tag_back.unwrap();
        assert_eq!(tag_back.id(), tag.id());
    }

    #[tokio::test]
    async fn document_gets_message_from_creator() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let actor_id = format!("{}/actor", did);

        let document = NoteMd1kFields::new(
            "abc".to_string(),
            actor_id.clone().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let document_id = document.id().as_str();
        let message = build_message(&jwk, document_id, None).await;

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
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", document_id),
                &document,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // server returns the message fo the document
        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}/createdBy/{}", document_id, actor_id),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let message_back: Option<MessageFields> = get_body(response).await;
        let message_back = message_back.unwrap();
        assert_eq!(message_back.id(), message.id());
    }

    #[tokio::test]
    async fn wont_update_invalid_document() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let document = NoteMd1kFields::new(
            "abc".to_string(),
            "did:example:a".to_string().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let document_id = document.id().as_str();
        let message = build_message(&jwk, document_id, None).await;

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
            .oneshot(request_json("POST", "/api/ap/urn:cid:invalid", &document))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // document contents don't match ID
        let mut invalid = serde_json::to_value(&document).unwrap();
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
                &format!("/api/ap/{}", document_id),
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
        let document = NoteMd1kFields::new(
            "abc".to_string(),
            "did:example:a".to_string().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let document_id = document.id().as_str();
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", document_id),
                &document,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
