use serde::Serialize;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::hyper::StatusCode;
use warp::{filters::BoxedFilter, hyper::Method};
use warp::{Filter, Rejection, Reply};

use crate::db::Connector;

mod actor;
mod error;
mod inbox;
mod object;
mod outbox;

use actor::*;
use error::Error;
use inbox::*;
use object::*;
use outbox::*;

pub fn with_resource<T: Clone + Send>(
    x: T,
) -> impl Filter<Extract = (T,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || x.clone())
}

#[derive(Serialize)]
pub struct ErrorMessage {
    code: u16,
    message: String,
}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    let code;
    let message;

    if let Some(Error::DbConnectionFailed) = err.find() {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "failed to connect to database";
    } else if let Some(Error::DbQueryFailed) = err.find() {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "failed to communicate with database";
    } else if let Some(Error::DidNotValid) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = "query DID is invalid";
    } else if let Some(Error::ActorNotKnown) = err.find() {
        code = StatusCode::NOT_FOUND;
        message = "actor not known";
    } else if let Some(Error::ActorNotValid) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = "actor data is invalid";
    } else if let Some(Error::ActorIdWrong) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = "wrong actor for the resource";
    } else if let Some(Error::ObjectNotKnown) = err.find() {
        code = StatusCode::NOT_FOUND;
        message = "object not known";
    } else if let Some(Error::ObjectNotValid) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = "object data is invalid";
    } else if let Some(Error::ObjectIdWrong) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = "wrong object for the resource";
    } else if let Some(Error::MessageNotValid) = err.find() {
        code = StatusCode::BAD_REQUEST;
        message = "message data is invalid";
    } else if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "resource no found";
    } else if let Some(_) = err.find::<warp::filters::body::BodyDeserializeError>() {
        message = "post body data is invalid";
        code = StatusCode::BAD_REQUEST;
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "method not allowed for resource";
    } else {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "request caused an unexpected error";
    }

    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message: message.into(),
    });

    Ok(warp::reply::with_status(json, code))
}

pub fn build_api(connector: Arc<RwLock<Connector>>, _did: String) -> BoxedFilter<(impl Reply,)> {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    let route_version = warp::get()
        .and(warp::path!("ap" / "version"))
        .map(|| VERSION);

    let route_did_outbox = warp::post()
        .and(warp::path!("ap" / String / "actor" / "outbox"))
        .and(warp::body::json())
        .and(with_resource(connector.clone()))
        .and_then(handle_did_outbox);

    let route_did_inbox = warp::get()
        .and(warp::path!("ap" / String / "actor" / "inbox"))
        .and(warp::query::<DidInboxQuery>())
        .and(with_resource(connector.clone()))
        .and_then(|did, query, connector| handle_did_inbox(did, query, connector));

    let route_did_following = warp::get()
        .and(warp::path!("ap" / String / "actor" / "following"))
        .and(with_resource(connector.clone()))
        .and_then(handle_did_following);

    let route_did_document = warp::get()
        .and(warp::path!("ap" / String))
        .and_then(handle_did_document);
    let route_did_actor_get = warp::get()
        .and(warp::path!("ap" / String / "actor"))
        .and(with_resource(connector.clone()))
        .and_then(handle_did_actor_get);
    let route_did_actor_post = warp::post()
        .and(warp::path!("ap" / String / "actor"))
        .and(warp::body::json())
        .and(with_resource(connector.clone()))
        .and_then(handle_did_actor_post);

    let route_object_get = warp::get()
        .and(warp::path!("ap" / String))
        .and(with_resource(connector.clone()))
        .and_then(handle_object_get);
    let route_object_post = warp::post()
        .and(warp::path!("ap" / String))
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
        .or(route_did_following)
        .or(route_did_document)
        .or(route_did_actor_get)
        .or(route_did_actor_post)
        .or(route_object_get)
        .or(route_object_post)
        .recover(handle_rejection)
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
        let api = build_api(connector, "did:example:server".to_string());
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = request()
            .method("GET")
            .path("/ap/version")
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), VERSION);
    }
}
