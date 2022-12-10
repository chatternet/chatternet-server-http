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
mod documents;
mod error;
mod inbox;
mod outbox;

use actor::*;
use documents::*;
use inbox::*;
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
                        .route("/:id", get(handle_document_get).post(handle_body_post)),
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
    use chatternet::model::{ActivityType, MessageFields};
    use hyper;
    use hyper::body::HttpBody;
    use mime;
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use ssi::jwk::JWK;
    use ssi::vc::URI;
    use tokio::sync::RwLock;

    use super::build_api;
    use crate::db::Connector;

    pub async fn build_message(
        jwk: &JWK,
        document_id: &str,
        to: Option<Vec<String>>,
        cc: Option<Vec<String>>,
        audience: Option<Vec<String>>,
    ) -> MessageFields {
        let to = to.map(|x| x.into_iter().map(|x| URI::try_from(x)).flatten().collect());
        let cc = cc.map(|x| x.into_iter().map(|x| URI::try_from(x)).flatten().collect());
        let audience =
            audience.map(|x| x.into_iter().map(|x| URI::try_from(x)).flatten().collect());
        MessageFields::new(
            &jwk,
            ActivityType::Create,
            &[document_id],
            to,
            cc,
            audience,
            None,
        )
        .await
        .unwrap()
    }

    pub async fn build_follow(follows_id: &[&str], jwk: &JWK) -> MessageFields {
        MessageFields::new(
            &jwk,
            ActivityType::Follow,
            follows_id,
            None,
            None,
            None,
            None,
        )
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
