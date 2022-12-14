use anyhow::Result;
use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use chatternet::didkey::actor_id_from_did;
use chatternet::model::{
    Actor, ActorFields, CollectionFields, CollectionPageFields, CollectionPageType, CollectionType,
    URI,
};
use tap::Pipe;

use super::error::AppError;
use super::{AppState, CollectionPageQuery};
use crate::db::{self};

/// Get the Actor document with `did` using a DB connection obtained from
/// `connector`.
pub async fn handle_actor_get(
    State(AppState { connector, .. }): State<AppState>,
    Path(did): Path<String>,
) -> Result<Json<ActorFields>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let actor = db::get_document(&mut connection, &actor_id).await;
    match actor {
        Ok(Some(actor)) => {
            let actor: ActorFields =
                serde_json::from_str(&actor).map_err(|_| AppError::ActorNotValid)?;
            Ok(Json(actor))
        }
        _ => Err(AppError::ActorNotKnown),
    }
}

/// Post an Actor `actor` for the actor with `did`. Stores the document using
/// a DB connection obtained from `connector`.
pub async fn handle_actor_post(
    State(AppState { connector, .. }): State<AppState>,
    Path(did): Path<String>,
    Json(actor): Json<ActorFields>,
) -> Result<StatusCode, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let mut connector = connector.write().await;
    let mut connection = connector
        .connection_mut()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    // the posted Actor must have the same ID as that in the path
    if actor.id().as_str() != actor_id {
        Err(AppError::ActorIdWrong)?;
    }
    if !actor.verify().await.is_ok() {
        Err(AppError::ActorNotValid)?;
    }
    let actor = serde_json::to_string(&actor).map_err(|_| AppError::ActorNotValid)?;
    db::put_document(&mut *connection, &actor_id, &actor)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    Ok(StatusCode::OK)
}

/// Get the collection of IDs followed by the actor with `did`.
pub async fn handle_actor_following(
    State(AppState { connector, .. }): State<AppState>,
    Path(did): Path<String>,
) -> Result<Json<CollectionFields<String>>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let ids = db::get_actor_followings(&mut *connection, &actor_id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    let uri =
        URI::try_from(format!("{}/following", actor_id)).map_err(|_| AppError::ActorIdWrong)?;
    let following = CollectionFields::new(uri, CollectionType::Collection, ids);
    Ok(Json(following))
}

/// Get the collection of IDs of follower of the actor with `did`.
pub async fn handle_actor_followers(
    State(AppState { connector, .. }): State<AppState>,
    Path(did): Path<String>,
    Query(query): Query<CollectionPageQuery>,
) -> Result<Json<CollectionPageFields<String>>, AppError> {
    let actor_id = actor_id_from_did(&did).map_err(|_| AppError::DidNotValid)?;
    let connector = connector.read().await;
    let mut connection = connector
        .connection()
        .await
        .map_err(|_| AppError::DbConnectionFailed)?;
    let collection_id =
        URI::try_from(format!("{}/following", actor_id)).map_err(|_| AppError::ActorIdWrong)?;
    let page_size = query.page_size.unwrap_or(32);
    let out = db::get_actor_followers(&mut *connection, &actor_id, page_size, query.start_idx)
        .await
        .map_err(|_| AppError::DbQueryFailed)?;
    Ok(Json(match out {
        Some(out) => {
            let start_idx = query.start_idx.unwrap_or(out.high_idx);
            let page_id = format!(
                "{}/?startIdx={}&pageSize={}",
                collection_id, start_idx, page_size
            )
            .pipe(URI::try_from)
            .map_err(|_| AppError::ServerMisconfigured)?;
            let next_page = if out.low_idx > 0 {
                Some(
                    format!(
                        "{}/?startIdx={}&pageSize={}",
                        collection_id,
                        out.low_idx - 1,
                        page_size
                    )
                    .pipe(URI::try_from)
                    .map_err(|_| AppError::ServerMisconfigured)?,
                )
            } else {
                None
            };
            CollectionPageFields::new(
                page_id,
                CollectionPageType::OrderedCollectionPage,
                out.items,
                collection_id,
                next_page,
            )
        }
        None => {
            let start_idx = 0;
            let page_id = format!(
                "{}/?startIdx={}&pageSize={}",
                collection_id, start_idx, page_size
            )
            .pipe(URI::try_from)
            .map_err(|_| AppError::ServerMisconfigured)?;
            CollectionPageFields::new(
                page_id,
                CollectionPageType::OrderedCollectionPage,
                vec![],
                collection_id,
                None,
            )
        }
    }))
}

#[cfg(test)]
mod test {
    use axum::http::StatusCode;
    use tap::Pipe;
    use tokio;
    use tower::ServiceExt;

    use chatternet::didkey::{build_jwk, did_from_jwk};
    use chatternet::model::{
        Actor, ActorFields, ActorType, CollectionPage, CollectionPageFields, URI,
    };

    use super::super::test_utils::*;

    #[tokio::test]
    async fn updates_and_gets_actor() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let actor = ActorFields::new(&jwk, ActorType::Person, Some("abc".to_string()), None)
            .await
            .unwrap();

        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor", did),
                &actor,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty("GET", &format!("/api/ap/{}/actor", did)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let actor_back: Option<ActorFields> = get_body(response).await;
        let actor_back = actor_back.unwrap();
        assert_eq!(actor_back.id(), actor.id());
    }

    #[tokio::test]
    async fn wont_update_invalid_actor() {
        let api = build_test_api().await;

        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();

        let actor = ActorFields::new(&jwk, ActorType::Person, Some("abc".to_string()), None)
            .await
            .unwrap();

        // did doesn't match actor ID
        let response = api
            .clone()
            .oneshot(request_json("POST", "/api/ap/did:example:a/actor", &actor))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let mut invalid = serde_json::to_value(&actor).unwrap();
        invalid.get_mut("name").map(|x| {
            *x = "abcd"
                .to_string()
                .pipe(Some)
                .pipe(serde_json::to_value)
                .unwrap()
        });

        // build an invalid actor
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor", did),
                &invalid,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn wont_get_unknown_actor() {
        let api = build_test_api().await;
        let response = api
            .clone()
            .oneshot(request_empty("GET", "/api/ap/did:key:za/actor"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn gets_following_followers() {
        let api = build_test_api().await;

        let jwk_1 = build_jwk(&mut rand::thread_rng()).unwrap();
        let jwk_2 = build_jwk(&mut rand::thread_rng()).unwrap();

        let did_1 = did_from_jwk(&jwk_1).unwrap();
        let did_2 = did_from_jwk(&jwk_2).unwrap();

        // 1 follows 1, 2 and a
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did_1),
                &build_follow(
                    vec![
                        URI::try_from(format!("{}/actor", did_1)).unwrap(),
                        URI::try_from(format!("{}/actor", did_2)).unwrap(),
                        URI::try_from("did:key:za/actor".to_string()).unwrap(),
                    ],
                    &jwk_1,
                )
                .await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // 2 follows self
        let response = api
            .clone()
            .oneshot(request_json(
                "POST",
                &format!("/api/ap/{}/actor/outbox", did_2),
                &build_follow(
                    vec![URI::try_from(format!("{}/actor", did_2)).unwrap()],
                    &jwk_2,
                )
                .await,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = api
            .clone()
            .oneshot(request_empty(
                "GET",
                &format!("/api/ap/{}/actor/followers", did_2),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let inbox: CollectionPageFields<String> = get_body(response).await;
        assert_eq!(
            inbox.items(),
            &[format!("{}/actor", did_2), format!("{}/actor", did_1),]
        );
    }
}
