use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use chatternet::didkey::{actor_id_from_did, did_from_jwk};
use chatternet::model::{
    ActivityType, CtxStreamLast, Message, MessageBuilder, MessageFields, Uri, VecUris,
};
use sqlx::{Connection, SqliteConnection};
use ssi::jwk::JWK;
use tap::Pipe;

use super::error::AppError;
use super::{use_mutable, AppState};
use crate::db::{self};

pub fn build_audiences_id(message: &MessageFields) -> Vec<String> {
    if let Some(to) = message.to() {
        to.as_ref().iter().map(|x| x.to_string()).collect()
    } else {
        vec![]
    }
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

async fn handle_unfollow(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let actor_id = message.actor().as_str();
    let objects_id: Vec<&str> = message.object().iter().map(|x| x.as_str()).collect();
    for object_id in objects_id {
        db::delete_actor_following(&mut *connection, &actor_id, &object_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
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

async fn handle_add(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let target = match message.target() {
        Some(target) => target,
        None => return Err(AppError::MessageNotValid),
    };
    if target.len() != 1 {
        return Err(AppError::MessageNotValid);
    }
    let target = match target.first() {
        Some(target) => target,
        None => return Err(AppError::MessageNotValid),
    };
    if target.as_str() != format!("{}/following", message.actor().as_str()) {
        return Err(AppError::MessageNotValid);
    }
    use_mutable(
        target.as_str(),
        message.published().timestamp_millis(),
        &mut *connection,
    )
    .await?;
    handle_follow(message, connection).await?;
    Ok(())
}

async fn handle_remove(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let target = match message.target() {
        Some(target) => target,
        None => return Err(AppError::MessageNotValid),
    };
    if target.len() != 1 {
        return Err(AppError::MessageNotValid);
    }
    let target = match target.first() {
        Some(target) => target,
        None => return Err(AppError::MessageNotValid),
    };
    if target.as_str() != format!("{}/following", message.actor().as_str()) {
        return Err(AppError::MessageNotValid);
    }
    use_mutable(
        target.as_str(),
        message.published().timestamp_millis(),
        &mut *connection,
    )
    .await?;
    handle_unfollow(message, connection).await?;
    Ok(())
}

async fn clear_followings(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    db::delete_actor_all_following(&mut *connection, message.actor().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::delete_actor_all_audiences(&mut *connection, &message.actor().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    return Ok(());
}

async fn delete_message(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    // delete the message document
    db::delete_document(&mut *connection, message.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;

    // delete the message associations
    db::delete_message_audiences(&mut *connection, message.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::delete_message_documents(&mut *connection, message.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::delete_message(&mut *connection, message.id().as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?;

    // delete the document documents if they are not associated to any other message
    for document_id in message.object().iter() {
        if db::has_message_with_document(&mut *connection, document_id.as_str())
            .await
            .map_err(|_| AppError::DbQueryFailed)?
        {
            continue;
        }
        db::delete_document(&mut *connection, document_id.as_str())
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
    let document_id = message.object().first().ok_or(AppError::MessageNotValid)?;

    // object to delete is the followers collection
    if document_id.as_str() == format!("{}/following", message.actor().as_str()) {
        use_mutable(
            document_id.as_str(),
            message.published().timestamp_millis(),
            &mut *connection,
        )
        .await?;
        clear_followings(message, connection).await?;
        return Ok(());
    }

    let document = match db::get_document(&mut *connection, document_id.as_str())
        .await
        .map_err(|_| AppError::DbQueryFailed)?
    {
        Some(object) => object,
        None => return Ok(()),
    };

    if let Ok(message_to_delete) = serde_json::from_str::<MessageFields>(&document) {
        // only the creator of a message can delete that message
        if message_to_delete.actor() != message.actor() {
            Err(AppError::MessageNotValid)?
        }
        delete_message(&message_to_delete, connection).await?;
        return Ok(());
    }

    if let Ok(document_to_delete) = serde_json::from_str::<serde_json::Value>(&document) {
        // ensure the correct context to interpret the attributedTo member
        document_to_delete
            .get("@context")
            .and_then(|x| serde_json::from_value::<CtxStreamLast>(x.to_owned()).ok())
            .ok_or(AppError::MessageNotValid)?;
        // ensure attributed to the sender of the delete
        let attributed_to = document_to_delete
            .get("attributedTo")
            .and_then(|x| x.as_str())
            .and_then(|x| Uri::try_from(x).ok())
            .ok_or(AppError::MessageNotValid)?;
        if &attributed_to != message.actor() {
            Err(AppError::MessageNotValid)?
        }
        db::delete_document(&mut *connection, document_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
        return Ok(());
    };

    Err(AppError::MessageNotValid)
}

async fn store_message(
    message: &MessageFields,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let message_id = message.id().as_str();
    let actor_id = message.actor().as_str();

    // store this message id for its audiences
    let audiences_id = build_audiences_id(&message);
    for audience_id in &audiences_id {
        db::put_message_audience(&mut *connection, &message_id, &audience_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
    }

    // associate actor document with this message
    db::put_message_document(
        &mut *connection,
        &message_id,
        message.actor().as_str(),
        None,
    )
    .await
    .map_err(|_| AppError::DbQueryFailed)?;
    // associate object documents with this message
    let objects_id: Vec<&str> = message.object().iter().map(|x| x.as_str()).collect();
    // also associate actor that created the object document
    let created_by = if message.type_() == ActivityType::Create {
        Some(message.actor().as_str())
    } else {
        None
    };
    for document_id in objects_id {
        db::put_message_document(&mut *connection, &message_id, document_id, created_by)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
    }
    // associate audience tags with this message
    for audience_id in &audiences_id {
        let tag_id = if let Some(tag_id) = audience_id.strip_suffix("/followers") {
            tag_id
        } else {
            audience_id
        };
        db::put_message_document(&mut *connection, &message_id, tag_id, None)
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

    let server_followers: Uri = format!("{}/followers", server_actor_id)
        .try_into()
        .map_err(|_| AppError::ServerMisconfigured)?;
    let extended_to: VecUris = if let Some(previous_to) = message.to() {
        std::iter::once(&server_followers)
            .chain(previous_to.as_ref())
            .cloned()
            .collect::<Vec<Uri>>()
            .pipe(VecUris::from_truncate)
    } else {
        VecUris::from_truncate(vec![server_followers])
    };
    let view_message = MessageBuilder::new(&jwk, ActivityType::View, message.object().clone())
        .to(extended_to)
        .origin(vec![message.id().clone()].pipe(VecUris::from_truncate))
        .build()
        .await
        .map_err(|_| AppError::ServerMisconfigured)?;

    store_message(&view_message, &mut *connection).await?;

    Ok(())
}

pub async fn handle_outbox(
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

    // run type-dependent side effects
    match message.type_() {
        // activity expresses a follow relationship
        ActivityType::Delete => handle_delete(&message, &mut *connection).await?,
        ActivityType::Add => handle_add(&message, &mut *connection).await?,
        ActivityType::Remove => handle_remove(&message, &mut *connection).await?,
        _ => (),
    }

    store_message(&message, &mut *connection).await?;

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
    use chatternet::model::{
        Collection, CollectionFields, CollectionPage, CollectionPageFields, Document,
        NoteMd1kFields,
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
            )
            .await,
        );
        assert_eq!(audiences_id, ["did:example:a", "did:example:b",]);
    }

    #[tokio::test]
    async fn handles_message() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let message = build_message(&jwk, "id:1", None).await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
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
                &format!("/api/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &message.id().as_str()),
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
        let message = build_message(&jwk, "id:1", None).await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                "/api/did:example:a/actor/outbox",
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
        let message = build_message(&jwk, "id:1", None).await;

        let mut invalid = serde_json::to_value(&message).unwrap();
        *invalid.get_mut("id").unwrap() = serde_json::to_value("id:a").unwrap();
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &invalid,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn handles_follow_gets_following() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let message = build_follow(vec!["tag:1".to_string(), "tag:2".to_string()], &jwk).await;
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}/actor/following", did),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let following: CollectionFields<String> = get_body(response).await;
        assert_eq!(following.items(), &vec!["tag:1", "tag:2"]);
    }

    #[tokio::test]
    async fn removes_follow() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let message_1 = build_follow(vec!["tag:1".to_string(), "tag:2".to_string()], &jwk).await;
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_1,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let message_remove = MessageBuilder::new(
            &jwk,
            ActivityType::Remove,
            vec!["tag:2".try_into().unwrap()].try_into().unwrap(),
        )
        .target(
            vec![format!("{}/actor/following", did).try_into().unwrap()]
                .try_into()
                .unwrap(),
        )
        .build()
        .await
        .unwrap();
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_remove,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}/actor/following", did),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let following: CollectionFields<String> = get_body(response).await;
        assert_eq!(following.items(), &vec!["tag:1"]);
    }

    #[tokio::test]
    async fn doesnt_add_stale_following() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let message_1 = build_follow(vec!["tag:1".to_string()], &jwk).await;
        let message_2 = MessageBuilder::new(
            &jwk,
            ActivityType::Remove,
            vec!["tag:1".try_into().unwrap()].try_into().unwrap(),
        )
        .target(
            vec![format!("{}/actor/following", did).try_into().unwrap()]
                .try_into()
                .unwrap(),
        )
        .build()
        .await
        .unwrap();
        let message_3 = loop {
            let message = build_follow(vec!["tag:1".to_string()], &jwk).await;
            if message.published() > message_2.published() {
                break message;
            };
        };

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_3,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_1,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}/actor/following", did),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let following: CollectionFields<String> = get_body(response).await;
        assert_eq!(following.items(), &vec!["tag:1"]);
    }

    #[tokio::test]
    async fn deletes_message() {
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
        let message = build_message(&jwk, document.id().as_str(), None).await;
        let message_delete =
            build_message_with_type(&jwk, ActivityType::Delete, message.id().as_str(), None).await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}", document.id().as_str()),
                &document,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &message.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &document.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_delete,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &message.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &document.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deletes_message_and_keeps_shared_document() {
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
        let message_1 = build_message(&jwk, document.id().as_str(), None).await;
        let message_2 = build_message(&jwk, document.id().as_str(), None).await;
        let message_delete =
            build_message_with_type(&jwk, ActivityType::Delete, message_1.id().as_str(), None)
                .await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_1,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}", document.id().as_str()),
                &document,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_delete,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &message_1.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &document.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn deletes_document() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let document = NoteMd1kFields::new(
            "abc".to_string(),
            format!("{}/actor", did).try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        let message = build_message(&jwk, document.id().as_str(), None).await;
        let message_delete =
            build_message_with_type(&jwk, ActivityType::Delete, document.id().as_str(), None).await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}", document.id().as_str()),
                &document,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &document.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_delete,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &document.id().as_str()),
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
        let message = build_message(&jwk_1, "id:1", None).await;

        let jwk_2 = build_jwk(&mut rand::thread_rng()).unwrap();
        let did_2 = did_from_jwk(&jwk_2).unwrap();
        let message_delete =
            build_message_with_type(&jwk_2, ActivityType::Delete, message.id().as_str(), None)
                .await;

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did_1),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did_2),
                &message_delete,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}", &message.id().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn delete_following_clears_following() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let message_1 = build_follow(vec!["tag:1".to_string(), "tag:2".to_string()], &jwk).await;
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_1,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let message_2 = build_message_with_type(
            &jwk,
            ActivityType::Delete,
            &format!("{}/actor/following", did),
            None,
        )
        .await;
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}/actor/following", did),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let following: CollectionFields<String> = get_body(response).await;
        assert!(following.items().is_empty());
    }

    #[tokio::test]
    async fn doesnt_delete_stale_following() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let message_1 = build_message_with_type(
            &jwk,
            ActivityType::Delete,
            &format!("{}/actor/following", did),
            None,
        )
        .await;
        let message_2 = loop {
            let message = build_follow(vec!["tag:1".to_string()], &jwk).await;
            if message.published() > message_1.published() {
                break message;
            };
        };

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did),
                &message_1,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
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
                &format!("/api/{}/actor/outbox", did_server),
                &build_follow(vec![format!("{}/actor", did_1)], &jwk_server).await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // from 1 to 1's followers
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did_1),
                &build_message(
                    &jwk_1,
                    "id:1",
                    Some(vec![format!("{}/actor/followers", did_1)]),
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
                &format!("/api/{}/actor/inbox?pageSize=4", did_2),
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
                &format!("/api/{}/actor/outbox", did_2),
                &build_follow(vec![format!("{}/actor", did_server)], &jwk_2).await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // 2 follows server follows 1 so get 1's message
        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}/actor/inbox?pageSize=4", did_2),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let inbox: CollectionPageFields<MessageFields> = get_body(response).await;
        assert_eq!(
            inbox
                .items()
                .iter()
                .map(|x| x.object().iter().map(|x| x.as_str()))
                .flatten()
                .collect::<Vec<&str>>(),
            ["id:1"]
        );
    }
}
