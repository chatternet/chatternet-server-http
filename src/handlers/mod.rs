use std::sync::Arc;
use tokio::sync::RwLock;
use warp::{filters::BoxedFilter, hyper::Method};
use warp::{Filter, Reply};

use crate::db::Connector;

mod actor;
mod inbox;
mod object;
mod outbox;

use actor::*;
use inbox::*;
use object::*;
use outbox::*;

pub fn with_resource<T: Clone + Send>(
    x: T,
) -> impl Filter<Extract = (T,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || x.clone())
}

pub fn build_api(connector: Arc<RwLock<Connector>>) -> BoxedFilter<(impl Reply,)> {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    let route_version = warp::get().and(warp::path("version")).map(|| VERSION);

    let route_did_outbox = warp::post()
        .and(warp::path!(String / "actor" / "outbox"))
        .and(warp::body::json())
        .and(with_resource(connector.clone()))
        .and_then(handle_did_outbox);

    let route_did_inbox = warp::get()
        .and(warp::path!(String / "actor" / "inbox"))
        .and(with_resource(connector.clone()))
        .and_then(|did, connector| handle_did_inbox(did, None, connector));
    let route_did_inbox_query = warp::get()
        .and(warp::path!(String / "actor" / "inbox"))
        .and(warp::query::<DidInboxQuery>())
        .and(with_resource(connector.clone()))
        .and_then(|did, query, connector| handle_did_inbox(did, Some(query), connector));

    let route_did_following = warp::get()
        .and(warp::path!(String / "actor" / "following"))
        .and(with_resource(connector.clone()))
        .and_then(handle_did_following);

    let route_did_document = warp::get()
        .and(warp::path!(String))
        .and_then(handle_did_document);
    let route_did_actor_get = warp::get()
        .and(warp::path!(String / "actor"))
        .and(with_resource(connector.clone()))
        .and_then(handle_did_actor_get);
    let route_did_actor_post = warp::post()
        .and(warp::path!(String / "actor"))
        .and(warp::body::json())
        .and(with_resource(connector.clone()))
        .and_then(handle_did_actor_post);

    let route_object_get = warp::get()
        .and(warp::path!(String))
        .and(with_resource(connector.clone()))
        .and_then(handle_object_get);
    let route_object_post = warp::post()
        .and(warp::path!(String))
        .and(warp::body::json())
        .and(with_resource(connector.clone()))
        .and_then(handle_object_post);

    let cors = warp::cors()
        .allow_any_origin()
        .allow_header("content-type")
        .allow_methods(&[Method::GET, Method::POST, Method::OPTIONS]);
    let log = warp::log("chatternet::api");
    route_version
        .or(route_did_outbox)
        .or(route_did_inbox)
        .or(route_did_inbox_query)
        .or(route_did_following)
        .or(route_did_document)
        .or(route_did_actor_get)
        .or(route_did_actor_post)
        .or(route_object_get)
        .or(route_object_post)
        .with(cors)
        .with(log)
        .boxed()
}

#[cfg(test)]
mod test {
    use serde::Serialize;
    use serde_json::json;
    use ssi::jwk::JWK;
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::chatternet::activities::{ActivityType, Message};
    use crate::chatternet::didkey;
    use crate::db::Connector;

    use super::*;

    pub async fn build_message(
        object_id: &str,
        to: Option<&impl Serialize>,
        cc: Option<&impl Serialize>,
        audience: Option<&impl Serialize>,
        jwk: &JWK,
    ) -> Message {
        let did = didkey::did_from_jwk(jwk).unwrap();
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
            None,
            Some(members),
            &jwk,
        )
        .await
        .unwrap()
    }

    pub async fn build_follow(follows_id: &[&str], jwk: &JWK) -> Message {
        let did = didkey::did_from_jwk(jwk).unwrap();
        Message::new(&did, follows_id, ActivityType::Follow, None, None, &jwk)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn api_handles_version() {
        let connector = Arc::new(RwLock::new(
            Connector::new("sqlite::memory:").await.unwrap(),
        ));
        let api = build_api(connector);
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = request().method("GET").path("/version").reply(&api).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), VERSION);
    }
}
