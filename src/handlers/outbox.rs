use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use chatternet::didkey::actor_id_from_did;
use chatternet::model::{ActivityType, Message, MessageFields};
use sqlx::{Connection, SqliteConnection};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::error::AppError;
use crate::db::{self, Connector};

pub fn build_audiences_id(message: &MessageFields) -> Result<Vec<String>> {
    let tos_id: Option<Vec<String>> = message
        .to()
        .as_ref()
        .map(|x| x.iter().map(|x| x.to_string()).collect());
    let ccs_id: Option<Vec<String>> = message
        .cc()
        .as_ref()
        .map(|x| x.iter().map(|x| x.to_string()).collect());
    let audiences_id: Option<Vec<String>> = message
        .audience()
        .as_ref()
        .map(|x| x.iter().map(|x| x.to_string()).collect());
    let chained = std::iter::empty::<String>()
        .chain(tos_id.unwrap_or(Vec::new()).into_iter())
        .chain(ccs_id.unwrap_or(Vec::new()).into_iter())
        .chain(audiences_id.unwrap_or(Vec::new()).into_iter());
    Ok(chained.collect())
}

async fn handle_follow(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let actor_id = message.actor().as_str();
    let objects_id: Vec<&str> = message.object().iter().map(|x| x.as_str()).collect();
    for object_id in objects_id {
        db::put_actor_following(&mut *connection, &actor_id, &object_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
        // also store the audience form of this follow for quick lookup
        db::put_actor_audience(
            &mut *connection,
            &actor_id,
            &format!("{}/followers", object_id),
        )
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    }
    Ok(())
}

async fn handle_delete(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let object_id = message.object().first().ok_or(AppError::MessageNotValid)?;
    if message.object().len() != 1 {
        Err(AppError::MessageNotValid)?
    }
    let object = match db::get_document(&mut *connection, object_id.as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?
    {
        Some(object) => object,
        None => return Ok(()),
    };
    let object: MessageFields =
        serde_json::from_str(&object).map_err(|_| AppError::MessageNotValid)?;
    if object.actor() != message.actor() {
        Err(AppError::MessageNotValid)?
    }
    db::delete_message(&mut *connection, object.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::delete_message_audiences(&mut *connection, object.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::delete_message_body(&mut *connection, object.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::delete_document(&mut *connection, object_id.as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    Ok(())
}

pub async fn handle_actor_outbox(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
    Json(message): Json<MessageFields>,
) -> Result<StatusCode, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    if actor_id != message.actor().as_str() {
        Err(AppError::ActorIdWrong)?;
    }

    // read write
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let mut connection = connection
        .begin()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;

    // if already known, take no actions
    let message_id = message.id().to_string();
    message
        .verify()
        .await
        .map_err(|_| AppError::MessageNotValid)?;
    if db::has_message(&mut *connection, &message_id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?
    {
        return Ok(StatusCode::ACCEPTED);
    };

    // run type-dependent side effects
    match message.type_() {
        // activity expresses a follow relationship
        ActivityType::Follow => handle_follow(&message, &mut *connection).await?,
        ActivityType::Delete => handle_delete(&message, &mut *connection).await?,
        _ => (),
    }

    // store this message id for its audiences
    let audiences_id = build_audiences_id(&message).map_err(|_| AppError::MessageNotValid)?;
    for audience_id in audiences_id {
        db::put_message_audience(&mut *connection, &message_id, &audience_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
    }

    // associate this message with its objects so they can be stored later
    let objects_id: Vec<&str> = message.object().iter().map(|x| x.as_str()).collect();
    for object_id in objects_id {
        db::put_message_body(&mut *connection, &message_id, object_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
    }

    // store the message itself
    let message = serde_json::to_string(&message).map_err(|_| AppError::MessageNotValid)?;
    db::put_document_if_new(&mut *connection, &message_id, &message)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::put_message_id(&mut *connection, &message_id, &actor_id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;

    connection
        .commit()
        .await
        .map_err(|_| AppError::DbQueryFailed)?;

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod test {
    use chatternet::model::{Collection, CollectionFields};
    use tokio;
    use tower::ServiceExt;

    use chatternet::didkey::{build_jwk, did_from_jwk};

    use super::super::test_utils::*;
    use super::*;

    #[tokio::test]
    async fn builds_audiences_id() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let audiences_id = build_audiences_id(
            &build_message(
                &jwk,
                "id:1",
                Some(vec![
                    "did:example:a".to_string(),
                    "did:example:b".to_string(),
                ]),
                Some(vec!["did:example:c".to_string()]),
                Some(vec!["did:example:d".to_string()]),
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
    async fn handles_message() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let message = build_message(&jwk, "id:1", None, None, None).await;

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

        // second post returns status accepted
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}", &message.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let message_back: Option<MessageFields> = get_body(response).await;
        assert_eq!(
            serde_json::to_string(&message).unwrap(),
            serde_json::to_string(&message_back.unwrap()).unwrap()
        );
    }

    #[tokio::test]
    async fn rejects_wrong_did() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let message = build_message(&jwk, "id:1", None, None, None).await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                "/api/ap/did:example:a/actor/outbox",
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_invalid_message() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let message = build_message(&jwk, "id:1", None, None, None).await;

        let mut invalid = serde_json::to_value(&message).unwrap();
        *invalid.get_mut("id").unwrap() = serde_json::to_value("id:a").unwrap();
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &invalid,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn handles_follow_get_following() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let message = build_follow(&["tag:1", "tag:2"], &jwk).await;
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
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}/actor/following", did),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let following: CollectionFields<String> = get_body(response).await;
        assert_eq!(following.items(), &vec!["tag:1", "tag:2"]);
    }

    #[tokio::test]
    async fn deletes_message() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let message = build_message(&jwk, "id:1", None, None, None).await;
        let message_delete = build_message_with_type(
            &jwk,
            ActivityType::Delete,
            message.id().as_str(),
            None,
            None,
            None,
        )
        .await;

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
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}", &message.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message_delete,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}", &message.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn doesnt_delete_others_messages() {
        let api = build_test_api().await;

        let jwk_1 = build_jwk(&mut rand::thread_rng()).unwrap();
        let did_1 = did_from_jwk(&jwk_1).unwrap();
        let message = build_message(&jwk_1, "id:1", None, None, None).await;

        let jwk_2 = build_jwk(&mut rand::thread_rng()).unwrap();
        let did_2 = did_from_jwk(&jwk_2).unwrap();
        let message_delete = build_message_with_type(
            &jwk_2,
            ActivityType::Delete,
            message.id().as_str(),
            None,
            None,
            None,
        )
        .await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did_1),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did_2),
                &message_delete,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}", &message.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
