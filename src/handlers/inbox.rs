use anyhow::{Error as AnyError, Result};
use axum::extract::{Json, Path, Query, State};
use chatternet::{
    didkey::actor_id_from_did,
    model::{new_inbox, Collection, Message},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::error::AppError;
use crate::db::{self, Connector};

#[derive(Deserialize, Serialize)]
pub struct DidInboxQuery {
    after: Option<String>,
}

pub async fn handle_inbox(
    State(connector): State<Arc<RwLock<Connector>>>,
    Path(did): Path<String>,
    Query(query): Query<DidInboxQuery>,
) -> Result<Json<Collection<Message>>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let after = query.after.as_ref().map(|x| x.as_str());
    let messages = db::get_inbox_for_actor(&mut connection, &actor_id, 32, after)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    let messages = messages
        .iter()
        .map(|x| serde_json::from_str(x).map_err(AnyError::new))
        .collect::<Result<Vec<Message>>>()
        .map_err(|_| AppError::DbQueryFailed)?;
    let inbox = new_inbox(&format!("{}/inbox", actor_id), messages, after)
        .map_err(|_| AppError::ActorIdWrong)?;
    Ok(Json(inbox))
}

#[cfg(test)]
mod test {
    use axum::http::StatusCode;
    use chatternet::didkey::{build_jwk, did_from_jwk};
    use tokio;
    use tower::ServiceExt;

    use super::super::test_utils::*;
    use super::*;

    #[tokio::test]
    async fn api_inbox_returns_messages() {
        let api = build_test_api().await;

        let jwk_1 = build_jwk(&mut rand::thread_rng()).unwrap();
        let did_1 = did_from_jwk(&jwk_1).unwrap();

        let jwk_2 = build_jwk(&mut rand::thread_rng()).unwrap();
        let did_2 = did_from_jwk(&jwk_2).unwrap();

        // did_1 will see because follows self and this is addressed to self
        let message = build_message(
            "id:1",
            Some(&[format!("{}/actor", did_1)]),
            NO_VEC,
            NO_VEC,
            &jwk_1,
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

        // did_1 won't see because follows did_2 but not addressed to an audience with did_1
        let message = build_message(
            "id:2",
            Some(&[format!("{}/actor", did_2)]),
            NO_VEC,
            NO_VEC,
            &jwk_1,
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

        // did_1 will see because follows did_2 and in did_2 follower collection
        let message = build_message(
            "id:3",
            Some(&[format!("{}/actor/followers", did_2)]),
            NO_VEC,
            NO_VEC,
            &jwk_1,
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

        // did_1 sees only own content addressed to self because not following others
        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}/actor/inbox", did_1),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let inbox: Collection<Message> = get_body(response).await;
        assert_eq!(
            inbox
                .items
                .iter()
                .map(|x| x.no_id.no_proof.object.iter().map(|x| x.as_str()))
                .flatten()
                .collect::<Vec<&str>>(),
            ["id:1"]
        );

        // did_1 follows did_2, gets added to did_2 followers
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did_1),
                &build_follow(&[&format!("{}/actor", did_2)], &jwk_1).await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}/actor/inbox", did_1),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let inbox: Collection<Message> = get_body(response).await;
        assert_eq!(
            inbox
                .items
                .iter()
                .map(|x| x.no_id.no_proof.object.iter().map(|x| x.as_str()))
                .flatten()
                .collect::<Vec<&str>>(),
            ["id:3", "id:1"]
        );
    }
}
