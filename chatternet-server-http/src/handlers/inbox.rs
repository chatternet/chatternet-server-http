use anyhow::{Error as AnyError, Result};
use axum::extract::{Json, Path, Query, State};
use chatternet::{
    didkey::actor_id_from_did,
    model::{new_inbox, CollectionPageFields, MessageFields},
};

use super::{error::AppError, AppState, CollectionPageQuery};
use crate::db::{self, CollectionPageOut};

pub async fn handle_inbox(
    State(AppState { connector, .. }): State<AppState>,
    Path(did): Path<String>,
    Query(query): Query<CollectionPageQuery>,
) -> Result<Json<CollectionPageFields<MessageFields>>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let page_size = query.page_size.unwrap_or(32);
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let inbox_out = db::get_inbox_for_actor(&mut connection, &actor_id, page_size, query.start_idx)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    let inbox = match inbox_out {
        Some(CollectionPageOut {
            items: messages,
            low_idx,
            high_idx,
        }) => {
            let messages = messages
                .iter()
                .map(|x| serde_json::from_str(x).map_err(AnyError::new))
                .collect::<Result<Vec<MessageFields>>>()
                .map_err(|_| AppError::DbQueryFailed)?;
            let start_idx = query.start_idx.unwrap_or(high_idx);
            let next_start_idx = if low_idx > 0 { Some(low_idx - 1) } else { None };
            new_inbox(&actor_id, messages, page_size, start_idx, next_start_idx)
                .map_err(|_| AppError::ActorIdWrong)?
        }
        None => {
            new_inbox(&actor_id, vec![], page_size, 0, None).map_err(|_| AppError::ActorIdWrong)?
        }
    };
    Ok(Json(inbox))
}

pub async fn handle_inbox_from(
    State(AppState { connector, .. }): State<AppState>,
    Path((did, did_from)): Path<(String, String)>,
    Query(query): Query<CollectionPageQuery>,
) -> Result<Json<CollectionPageFields<MessageFields>>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let from_actor_id = actor_id_from_did(&did_from).map_err(|_| AppError::DidNotValid)?;
    let page_size = query.page_size.unwrap_or(32);
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let inbox_out = db::get_inbox_from_actor(
        &mut connection,
        &actor_id,
        from_actor_id.as_str(),
        page_size,
        query.start_idx,
    )
    .await
    .map_err(|_| AppError::DbQueryFailed)?;
    let inbox = match inbox_out {
        Some(CollectionPageOut {
            items: messages,
            low_idx,
            high_idx,
        }) => {
            let messages = messages
                .iter()
                .map(|x| serde_json::from_str(x).map_err(AnyError::new))
                .collect::<Result<Vec<MessageFields>>>()
                .map_err(|_| AppError::DbQueryFailed)?;
            let start_idx = query.start_idx.unwrap_or(high_idx);
            let next_start_idx = if low_idx > 0 && low_idx < start_idx {
                Some(low_idx - 1)
            } else {
                None
            };
            new_inbox(&actor_id, messages, page_size, start_idx, next_start_idx)
                .map_err(|_| AppError::ActorIdWrong)?
        }
        None => {
            new_inbox(&actor_id, vec![], page_size, 0, None).map_err(|_| AppError::ActorIdWrong)?
        }
    };
    Ok(Json(inbox))
}

#[cfg(test)]
mod test {
    use axum::http::StatusCode;
    use chatternet::didkey::{build_jwk, did_from_jwk};
    use chatternet::model::{CollectionPage, Message};
    use tokio;
    use tower::ServiceExt;

    use super::super::test_utils::*;
    use super::*;

    #[tokio::test]
    async fn api_inbox_returns_messages() {
        let api = build_test_api().await;

        let jwk_1 = build_jwk(&mut rand::thread_rng()).unwrap();
        let jwk_2 = build_jwk(&mut rand::thread_rng()).unwrap();
        let did_1 = did_from_jwk(&jwk_1).unwrap();
        let did_2 = did_from_jwk(&jwk_2).unwrap();

        // did_1 will see because follows self and this is addressed to self
        let message = build_message(&jwk_1, "id:1", Some(vec![format!("{}/actor", did_1)])).await;
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

        // did_1 won't see because not addressed to an audience with did_1
        let message = build_message(&jwk_1, "id:2", Some(vec![format!("{}/actor", did_2)])).await;
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

        // did_1 will see because follows did_2 and in did_2 follower collection
        let message = build_message(
            &jwk_2,
            "id:3",
            Some(vec![format!("{}/actor/followers", did_2)]),
        )
        .await;
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did_2),
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
                &format!("/api/{}/actor/inbox?pageSize=4", did_1),
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
        assert_eq!(
            inbox.next().as_ref().unwrap().as_str(),
            &format!("{}/actor/inbox?startIdx={}&pageSize=4", did_1, 0)
        );

        // did_1 follows did_2, gets added to did_2 followers
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/{}/actor/outbox", did_1),
                &build_follow(vec![format!("{}/actor", did_2)], &jwk_1).await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}/actor/inbox?pageSize=4", did_1),
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
            ["id:3", "id:1"]
        );

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}/actor/inbox?pageSize=1", did_1),
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
            ["id:3"]
        );
    }

    #[tokio::test]
    async fn api_inbox_returns_messages_from_actor() {
        let api = build_test_api().await;

        let jwk_1 = build_jwk(&mut rand::thread_rng()).unwrap();
        let jwk_x = build_jwk(&mut rand::thread_rng()).unwrap();
        let did_1 = did_from_jwk(&jwk_1).unwrap();
        let did_x = did_from_jwk(&jwk_x).unwrap();

        let message = build_message(
            &jwk_1,
            "id:1",
            Some(vec![format!("{}/actor/followers", did_1)]),
        )
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

        // did_x can see did_2 message when asking from did_2
        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/{}/actor/inbox/from/{}/actor?pageSize=4", did_x, did_1),
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
