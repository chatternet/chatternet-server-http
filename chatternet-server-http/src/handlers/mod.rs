use axum::http::{header, Method};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;
use ssi::jwk::JWK;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::db::{self, Connector};

mod actor;
mod documents;
mod error;
mod inbox;
mod outbox;

use actor::*;
use documents::*;
use inbox::*;
use outbox::*;

use self::error::AppError;

#[derive(Serialize)]
pub struct ErrorMessage {
    code: u16,
    message: String,
}

#[derive(Clone, Debug)]
pub struct AppState {
    pub connector: Arc<RwLock<Connector>>,
    pub jwk: Arc<JWK>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionPageQuery {
    page_size: Option<u64>,
    start_idx: Option<u64>,
}

pub fn build_api(state: AppState, prefix: &str, _did: &str) -> Router {
    const VERSION: &str = env!("CARGO_PKG_VERSION");

    // CORS layer adds headers to responses to tell the browser that cross
    // origin requests are allowed. Need to specify origins, methods and
    // headers on which to layer these headers:
    // https://developer.mozilla.org/en-US/docs/Glossary/CORS-safelisted_request_header

    Router::new()
        .nest(
            &format!("/{}", prefix),
            Router::new()
                .route(
                    "/version",
                    get(|| async { serde_json::to_string(VERSION).unwrap() }),
                )
                .nest(
                    "/ap",
                    Router::new()
                        // when there is a trailing `/actor`, interpret ID as DID and use
                        // actor-specific handlers
                        .route("/:id/actor", get(handle_actor_get).post(handle_actor_post))
                        .route("/:id/actor/following", get(handle_actor_following))
                        .route("/:id/actor/followers", get(handle_actor_followers))
                        .route("/:id/actor/outbox", post(handle_outbox))
                        .route("/:id/actor/inbox", get(handle_inbox))
                        .route("/:id/actor/inbox/from/:id2/actor", get(handle_inbox_from))
                        .route("/:id", get(handle_document_get).post(handle_body_post))
                        .route("/:id/createdBy/:id2/actor", get(handle_document_get_create)),
                ),
        )
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::any())
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers([
                    header::ACCEPT,
                    header::ACCEPT_LANGUAGE,
                    header::CONTENT_LANGUAGE,
                    header::CONTENT_TYPE,
                ]),
        )
        .with_state(state)
}

async fn use_mutable(
    id: &str,
    timestamp_millis: i64,
    connection: &mut SqliteConnection,
) -> Result<(), AppError> {
    if db::get_mutable_modified(&mut *connection, id)
        .await
        .map_err(|_| AppError::DbQueryFailed)?
        .map_or(false, |x| x > timestamp_millis)
    {
        Err(AppError::StaleMessage)?;
    } else {
        db::put_mutable_modified(&mut *connection, id, timestamp_millis)
            .await
            .map_err(|_| AppError::DbQueryFailed)?;
    }
    Ok(())
}

#[cfg(test)]
mod test_utils {
    use std::fmt::Debug;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{self, Request, Response};
    use axum::routing::Router;
    use chatternet::didkey::{build_jwk, did_from_jwk};
    use chatternet::model::{ActivityType, MessageBuilder, MessageFields};
    use hyper;
    use hyper::body::HttpBody;
    use mime;
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use ssi::jwk::JWK;
    use tokio::sync::RwLock;

    use super::{build_api, AppState};
    use crate::db::Connector;

    pub async fn build_message_with_type(
        jwk: &JWK,
        activity_type: ActivityType,
        document_id: &str,
        to: Option<Vec<String>>,
    ) -> MessageFields {
        let builder =
            MessageBuilder::new(&jwk, activity_type, vec![document_id.to_string()]).unwrap();
        let builder = if let Some(to) = to {
            builder.to(to).unwrap()
        } else {
            builder
        };
        builder.build().await.unwrap()
    }

    pub async fn build_message(
        jwk: &JWK,
        document_id: &str,
        to: Option<Vec<String>>,
    ) -> MessageFields {
        build_message_with_type(jwk, ActivityType::Create, document_id, to).await
    }

    pub async fn build_follow(follows_id: Vec<String>, jwk: &JWK) -> MessageFields {
        let did = did_from_jwk(jwk).unwrap();
        MessageBuilder::new(&jwk, ActivityType::Add, follows_id)
            .unwrap()
            .target(vec![format!("{}/actor/following", did)])
            .unwrap()
            .build()
            .await
            .unwrap()
    }

    pub async fn get_body<T, U>(response: Response<T>) -> U
    where
        T: HttpBody,
        <T as HttpBody>::Error: Debug,
        U: DeserializeOwned,
    {
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        return serde_json::from_slice(&body[..]).unwrap();
    }

    fn body_from_json(value: &impl Serialize) -> Body {
        Body::from(serde_json::to_string(value).unwrap())
    }

    pub fn request_json(
        method: &str,
        uri: &str,
        value: &(impl Serialize + ?Sized),
    ) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(http::header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(body_from_json(&value))
            .unwrap()
    }

    pub fn request_empty(method: &str, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    pub async fn build_test_api_jwk(jwk: JWK) -> Router {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let state = AppState {
            connector,
            jwk: Arc::new(jwk),
        };
        build_api(state, "api", "did:example:server")
    }

    pub async fn build_test_api() -> Router {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let jwk = Arc::new(build_jwk(&mut rand::thread_rng()).unwrap());
        let state = AppState { connector, jwk };
        build_api(state, "api", "did:example:server")
    }
}

#[cfg(test)]
mod test {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tokio;
    use tower::ServiceExt;

    use super::test_utils::*;
    use super::*;

    #[tokio::test]
    async fn api_handles_version() {
        let api = build_test_api().await;
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = api
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/version")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: String = get_body(response).await;
        assert_eq!(body, VERSION);
    }

    #[tokio::test]
    async fn use_mutable_fails_if_modified() {
        let mut connector = Connector::new("sqlite::memory:").await.unwrap();
        let mut connection = connector.connection_mut().await.unwrap();
        use_mutable("id", 1, &mut *connection).await.unwrap();
        use_mutable("id", 2, &mut *connection).await.unwrap();
        use_mutable("id", 0, &mut *connection).await.unwrap_err();
    }
}
