use anyhow::{anyhow, Result};
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use sqlx::SqliteConnection;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::error::AppError;
use crate::chatternet::activities::{actor_id_from_did, ActivityType, Message};
use crate::db::{self, Connector};

pub fn build_audiences_id(message: &Message) -> Result<Vec<String>> {
    if message
        .members
        .as_ref()
        .map(|x| x.contains_key("bcc"))
        .unwrap_or(false)
    {
        Err(anyhow!("message contains bcc which cannot be handled"))?;
    }
    if message
        .members
        .as_ref()
        .map(|x| x.contains_key("bto"))
        .unwrap_or(false)
    {
        Err(anyhow!("message contains bto which cannot be handled"))?;
    }

    let tos_id: Option<Vec<String>> = message
        .members
        .as_ref()
        .and_then(|x| x.get("to"))
        .and_then(|x| x.as_array())
        .map(|x| {
            x.iter()
                .filter_map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Vec<String>>()
        });

    let ccs_id = message
        .members
        .as_ref()
        .and_then(|x| x.get("cc"))
        .and_then(|x| x.as_array())
        .map(|x| {
            x.iter()
                .flat_map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Vec<String>>()
        });

    let audiences_id = message
        .members
        .as_ref()
        .and_then(|x| x.get("audience"))
        .and_then(|x| x.as_array())
        .map(|x| {
            x.iter()
                .flat_map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Vec<String>>()
        });

    let chained = std::iter::empty::<String>()
        .chain(tos_id.unwrap_or(Vec::new()).into_iter())
        .chain(ccs_id.unwrap_or(Vec::new()).into_iter())
        .chain(audiences_id.unwrap_or(Vec::new()).into_iter());
    Ok(chained.collect())
}

async fn handle_follow(
    message: &Message,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    let actor_id = message.actor.as_str();
    let objects_id: Vec<&str> = message.object.iter().map(|x| x.as_str()).collect();
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

pub async fn handle_actor_outbox(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
    Json(message): Json<Message>,
) -> Result<StatusCode, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    if actor_id != message.actor.as_str() {
        Err(AppError::ActorIdWrong)?;
    }

    // read write
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;

    // if already known, take no actions
    let message_id = message
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
    match message.message_type {
        // activity expresses a follow relationship
        ActivityType::Follow => handle_follow(&message, &mut *connection).await?,
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
    let objects_id: Vec<&str> = message.object.iter().map(|x| x.as_str()).collect();
    for object_id in objects_id {
        db::put_message_object(&mut *connection, &message_id, object_id)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
    }

    // store the message itself
    let message = serde_json::to_string(&message).map_err(|_| AppError::MessageNotValid)?;
    db::put_object(&mut *connection, &message_id, &message)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    db::put_message_id(&mut *connection, &message_id, &actor_id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use ssi::vc::URI;
    use tokio;
    use tower::ServiceExt;

    use crate::chatternet::activities::Collection;
    use crate::chatternet::didkey;

    use super::super::test_utils::*;
    use super::*;

    #[tokio::test]
    async fn builds_audiences_id() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let audiences_id = build_audiences_id(
            &build_message(
                "id:1",
                Some(&["did:example:a", "did:example:b"]),
                Some(&["did:example:c"]),
                Some(&["did:example:d"]),
                &jwk,
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

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

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
                &format!("/api/ap/{}", &message.id.as_ref().unwrap().as_str()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let message_back: Option<Message> = get_body(response).await;
        assert_eq!(
            serde_json::to_string(&message).unwrap(),
            serde_json::to_string(&message_back.unwrap()).unwrap()
        );
    }

    #[tokio::test]
    async fn rejects_wrong_did() {
        let api = build_test_api().await;

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

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

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;

        let mut message_2 = message.clone();
        message_2.id = Some(URI::from_str("id:a").unwrap());
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message_2,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_with_bcc() {
        let api = build_test_api().await;

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;
        message.members.as_mut().and_then(|x| {
            x.insert(
                "bcc".to_string(),
                serde_json::to_value("did:example:a").unwrap(),
            )
        });

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_with_bto() {
        let api = build_test_api().await;

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = build_message("id:1", NO_VEC, NO_VEC, NO_VEC, &jwk).await;
        message.members.as_mut().and_then(|x| {
            x.insert(
                "bto".to_string(),
                serde_json::to_value("did:example:a").unwrap(),
            )
        });

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did),
                &message,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn handles_follow_get_following() {
        let api = build_test_api().await;

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();

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
        let following: Collection<String> = get_body(response).await;
        assert_eq!(following.items, ["tag:1", "tag:2"]);
    }
}
