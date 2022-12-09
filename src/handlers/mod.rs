use axum::http::{header, Method};
use axum::routing::{get, post};
use axum::Router;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::db::Connector;

mod actor;
mod error;
mod inbox;
mod object;
mod outbox;

use actor::*;
use inbox::*;
use object::*;
use outbox::*;

#[derive(Serialize)]
pub struct ErrorMessage {
    code: u16,
    message: String,
}

pub fn build_api(connector: Arc<RwLock<Connector>>, prefix: &str, _did: &str) -> Router {
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
                        .route("/:id/actor/outbox", post(handle_actor_outbox))
                        .route("/:id/actor/inbox", get(handle_inbox))
                        // post and get a generic object
                        .route("/:id", get(handle_object_get).post(handle_object_post)),
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
        .with_state(connector)
}

#[cfg(test)]
mod test_utils {
    use std::fmt::Debug;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{self, Request, Response};
    use axum::routing::Router;
    use chatternet::didkey::did_from_jwk;
    use chatternet::model::{ActivityType, Message};
    use hyper;
    use hyper::body::HttpBody;
    use mime;
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use serde_json::json;
    use ssi::jwk::JWK;
    use tokio::sync::RwLock;

    use super::build_api;
    use crate::db::Connector;

    pub const NO_VEC: Option<&Vec<String>> = None;

    pub async fn build_message(
        object_id: &str,
        to: Option<&impl Serialize>,
        cc: Option<&impl Serialize>,
        audience: Option<&impl Serialize>,
        jwk: &JWK,
    ) -> Message {
        let did = did_from_jwk(jwk).unwrap();
        let members = json!({
            "to": to,
            "cc": cc,
            "audience": audience,
        })
        .as_object()
        .unwrap()
        .to_owned();
        Message::new(
            &did,
            &[object_id],
            ActivityType::Create,
            &jwk,
            Some(members),
        )
        .await
        .unwrap()
    }

    pub async fn build_follow(follows_id: &[&str], jwk: &JWK) -> Message {
        let did = did_from_jwk(jwk).unwrap();
        Message::new(&did, follows_id, ActivityType::Follow, &jwk, None)
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

    pub fn request_json(method: &str, uri: &str, value: &impl Serialize) -> Request<Body> {
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

    pub async fn build_test_api() -> Router {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        build_api(connector, "api", "did:example:server")
    }
}

#[cfg(test)]
mod test {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tokio;
    use tower::ServiceExt;

    use super::test_utils::*;

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
}
