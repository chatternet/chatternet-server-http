use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use chatternet::didkey::{actor_id_from_did, did_from_jwk};
use chatternet::model::{ActivityType, Message, MessageFields, URI};
use sqlx::{Connection, SqliteConnection};
use ssi::jwk::JWK;

use super::error::AppError;
use super::AppState;
use crate::db::{self};

pub fn build_audiences_id(message: &MessageFields) -> Result<Vec<String>> {
    let tos_id: Option<Vec<String>> = message
        .to()
        .as_ref()
        .map(|x| x.as_vec().iter().map(|x| x.to_string()).collect());
    let ccs_id: Option<Vec<String>> = message
        .cc()
        .as_ref()
        .map(|x| x.as_vec().iter().map(|x| x.to_string()).collect());
    let audiences_id: Option<Vec<String>> = message
        .audience()
        .as_ref()
        .map(|x| x.as_vec().iter().map(|x| x.to_string()).collect());
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
    let objects_id: Vec<&str> = message
        .object()
        .as_vec()
        .iter()
        .map(|x| x.as_str())
        .collect();
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

async fn handle_unfollow(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let actor_id = message.actor().as_str();
    let objects_id: Vec<&str> = message
        .object()
        .as_vec()
        .iter()
        .map(|x| x.as_str())
        .collect();
    for object_id in objects_id {
        db::delete_actor_following(&mut *connection, &actor_id, &object_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
        // also delete the audience form of this follow for quick lookup
        db::delete_actor_audience(
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
    // can delete only one document at a time
    let document_id = message
        .object()
        .as_vec()
        .first()
        .ok_or(AppError::MessageNotValid)?;
    if message.object().as_vec().len() != 1 {
        Err(AppError::MessageNotValid)?
    }
    // the document to delete
    let document = match db::get_document(&mut *connection, document_id.as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?
    {
        Some(object) => object,
        None => return Ok(()),
    };
    // can delete only messages
    let message_to_delete: MessageFields =
        serde_json::from_str(&document).map_err(|_| AppError::MessageNotValid)?;
    // only the creator of a message can delete that message
    if message_to_delete.actor() != message.actor() {
        Err(AppError::MessageNotValid)?
    }

    // side effects of delete
    match message_to_delete.type_() {
        ActivityType::Follow => handle_unfollow(&message_to_delete, connection).await?,
        _ => (),
    }

    // delete the message document
    db::delete_document(&mut *connection, message_to_delete.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;

    // delete the message associations
    db::delete_message_audiences(&mut *connection, message_to_delete.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::delete_message_body(&mut *connection, message_to_delete.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::delete_message(&mut *connection, message_to_delete.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;

    // delete the body documents if they are not associated to any other message
    for body_id in message_to_delete.object().as_vec() {
        if db::has_message_with_body(&mut *connection, body_id.as_str())
            .await
            .map_err(|_| AppError::DbQueryFailed)?
        {
            continue;
        }
        db::delete_document(&mut *connection, body_id.as_str())
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
    }

    Ok(())
}

async fn store_message(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let message_id = message.id().as_str();
    let actor_id = message.actor().as_str();

    // store this message id for its audiences
    let audiences_id = build_audiences_id(&message).map_err(|_| AppError::MessageNotValid)?;
    for audience_id in &audiences_id {
        db::put_message_audience(&mut *connection, &message_id, &audience_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
    }

    // associate this message with its objects so they can be stored later
    let objects_id: Vec<&str> = message
        .object()
        .as_vec()
        .iter()
        .map(|x| x.as_str())
        .collect();
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

    Ok(())
}

async fn handle_view(
    message: &MessageFields,
    connection: &mut SqliteConnection,
    jwk: &JWK,
) -> Result<(), AppError> {
    let server_did = did_from_jwk(&jwk).map_err(|_| AppError::ServerMisconfigured)?;
    let server_actor_id =
        actor_id_from_did(&server_did).map_err(|_| AppError::ServerMisconfigured)?;
    if message.actor().as_str() == server_actor_id.as_str() {
        return Ok(());
    }
    if !db::inbox_contains_message(&mut *connection, &server_actor_id, message.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?
    {
        return Ok(());
    }

    let server_followers = format!("{}/followers", server_actor_id);
    let view_message = MessageFields::new(
        &jwk,
        ActivityType::View,
        message.object().as_vec().clone(),
        None,
        None,
        Some(vec![
            URI::try_from(server_followers).map_err(|_| AppError::ServerMisconfigured)?
        ]),
        Some(message.id().to_owned()),
    )
    .await
    .map_err(|_| AppError::ServerMisconfigured)?;

    store_message(&view_message, &mut *connection).await?;

    Ok(())
}

pub async fn handle_actor_outbox(
    State(AppState { connector, jwk }): State<AppState>,
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

    store_message(&message, &mut *connection).await?;

    // run type-dependent side effects
    match message.type_() {
        // activity expresses a follow relationship
        ActivityType::Follow => handle_follow(&message, &mut *connection).await?,
        ActivityType::Delete => handle_delete(&message, &mut *connection).await?,
        _ => (),
    }
    if message.type_() != ActivityType::View {
        handle_view(&message, &mut *connection, &jwk).await?;
    }

    connection
        .commit()
        .await
        .map_err(|_| AppError::DbQueryFailed)?;

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use chatternet::model::{
        Collection, CollectionFields, CollectionPage, CollectionPageFields, Note1k, Note1kFields,
        NoteType,
    };
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

        let message = build_follow(
            vec![
                URI::from_str("tag:1").unwrap(),
                URI::from_str("tag:2").unwrap(),
            ],
            &jwk,
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
        let body = Note1kFields::new(NoteType::Note, "abc".to_string(), None, None, None)
            .await
            .unwrap();
        let message = build_message(&jwk, body.id().as_str(), None, None, None).await;
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
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", body.id().as_str()),
                &body,
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
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}", &body.id().as_str()),
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

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}", &body.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deletes_message_and_keeps_shared_body() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let body = Note1kFields::new(NoteType::Note, "abc".to_string(), None, None, None)
            .await
            .unwrap();
        let message_1 = build_message(&jwk, body.id().as_str(), None, None, None).await;
        let message_2 = build_message(&jwk, body.id().as_str(), None, None, None).await;
        let message_delete = build_message_with_type(
            &jwk,
            ActivityType::Delete,
            message_1.id().as_str(),
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
                &message_1,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message_2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}", body.id().as_str()),
                &body,
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
                &format!("/api/ap/{}", &message_1.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}", &body.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
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

    #[tokio::test]
    async fn deletes_follow_and_unfollows() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let message_1 = build_follow(
            vec![
                URI::from_str("tag:1").unwrap(),
                URI::from_str("tag:2").unwrap(),
            ],
            &jwk,
        )
        .await;
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message_1,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let message_2 = build_follow(vec![URI::from_str("tag:3").unwrap()], &jwk).await;
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message_2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let message_delete = build_message_with_type(
            &jwk,
            ActivityType::Delete,
            message_1.id().as_str(),
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
                &message_delete,
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
        assert_eq!(following.items(), &vec!["tag:3"]);
    }

    #[tokio::test]
    async fn views_message_from_followed() {
        let jwk_server = build_jwk(&mut rand::thread_rng()).unwrap();
        let jwk_1 = build_jwk(&mut rand::thread_rng()).unwrap();
        let jwk_2 = build_jwk(&mut rand::thread_rng()).unwrap();

        let api = build_test_api_jwk(jwk_server.clone()).await;

        let did_server = did_from_jwk(&jwk_server).unwrap();
        let did_1 = did_from_jwk(&jwk_1).unwrap();
        let did_2 = did_from_jwk(&jwk_2).unwrap();

        // server follows 1
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did_server),
                &build_follow(
                    vec![URI::try_from(format!("{}/actor", did_1)).unwrap()],
                    &jwk_server,
                )
                .await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // from 1 to 1's followers
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did_1),
                &build_message(
                    &jwk_1,
                    "id:1",
                    Some(vec![format!("{}/actor/followers", did_1)]),
                    None,
                    None,
                )
                .await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // 2 has no messages (doesn't follow 1)
        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}/actor/inbox?pageSize=4", did_2),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let inbox: CollectionPageFields<MessageFields> = get_body(response).await;
        assert!(inbox.items().is_empty());

        // 2 follows server
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did_2),
                &build_follow(
                    vec![URI::try_from(format!("{}/actor", did_server)).unwrap()],
                    &jwk_2,
                )
                .await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // 2 follows server follows 1 so get 1's message
        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}/actor/inbox?pageSize=4", did_2),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let inbox: CollectionPageFields<MessageFields> = get_body(response).await;
        assert_eq!(
            inbox
                .items()
                .iter()
                .map(|x| x.object().as_vec().iter().map(|x| x.as_str()))
                .flatten()
                .collect::<Vec<&str>>(),
            ["id:1"]
        );
    }
}
