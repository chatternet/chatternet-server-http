use anyhow::{anyhow, Result};
use serde_json::Value;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::{Filter, Rejection};

use crate::chatternet::activities::{Message, MessageType};
use crate::db::Db;
use crate::errors::Error;

fn build_audiences_id(message: &Message) -> Result<Vec<String>> {
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
        .and_then(|x| x.get("audiences"))
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

async fn handle_outbox(
    did: String,
    message: Message,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let actor_did = &message.actor.id;
    if actor_did != &did {
        Err(anyhow!("posting to the wrong outbox for the message actor")).map_err(Error)?;
    }

    let id = message.verify().await.map_err(Error)?;
    if db.has_message(&id).await.map_err(Error)? {
        return Ok(StatusCode::ACCEPTED);
    };

    let audiences_id = build_audiences_id(&message).map_err(Error)?;

    match message.message_type {
        MessageType::Follow => {
            let audience_id = message
                .members
                .as_ref()
                .and_then(|x| x.get("object"))
                .and_then(|x| match x {
                    Value::String(audience_id) => Some(audience_id.as_str()),
                    Value::Object(object) => object.get("id").and_then(|x| x.as_str()),
                    _ => None,
                });
            if let Some(audience_id) = audience_id {
                db.put_audience(&message.actor.id, audience_id)
                    .await
                    .map_err(Error)?;
            }
        }
        _ => (),
    }

    let message = serde_json::to_string(&message).map_err(|x| Error(anyhow!(x)))?;
    db.put_message(&message, &id, &actor_did, &audiences_id[..])
        .await
        .map_err(Error)?;
    Ok(StatusCode::OK)
}

async fn handle_inbox(did: String, db: Arc<Db>) -> Result<impl warp::Reply, Rejection> {
    let messages = db.get_inbox_for_did(&did, 32).await.map_err(Error)?;
    Ok(warp::reply::json(&messages))
}

fn with_resource<T: Clone + Send>(
    x: T,
) -> impl Filter<Extract = (T,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || x.clone())
}

pub fn build_api(
    db: Arc<Db>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    let route_version = warp::get().and(warp::path("version")).map(|| VERSION);
    let route_outbox = warp::post()
        .and(warp::path!("did" / String / "outbox"))
        .and(warp::body::json())
        .and(with_resource(db.clone()))
        .and_then(handle_outbox);
    let route_inbox = warp::post()
        .and(warp::path!("did" / String / "inbox"))
        .and(with_resource(db.clone()))
        .and_then(handle_inbox);
    route_version.or(route_outbox).or(route_inbox)
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use ssi::jwk::JWK;
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::chatternet::activities::{Message, MessageActor, MessageType};
    use crate::chatternet::didkey;

    use super::*;

    #[tokio::test]
    async fn api_handles_version() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let api = build_api(db);
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = request().method("GET").path("/version").reply(&api).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), VERSION);
    }

    async fn build_message(content: &str, jwk: &JWK) -> Message {
        let did = didkey::did_from_jwk(jwk).unwrap();
        let members = json!({
            "object": {
                "type": "Note",
                "content": content
            }
        })
        .as_object()
        .unwrap()
        .to_owned();
        let actor = MessageActor::new(did, None, None);
        Message::new(actor, MessageType::Create, Some(members), &jwk)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn api_outbox_handles_message() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let api = build_api(db);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("message", &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/did/{}/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        // second post returns status accepted
        let response = request()
            .method("POST")
            .path(&format!("/did/{}/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn api_outbox_rejects_wrong_did() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let api = build_api(db);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let message = build_message("message", &jwk).await;

        let response = request()
            .method("POST")
            .path("/did/did:example:a/outbox")
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_outbox_rejects_invalid_message() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let api = build_api(db);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("message", &jwk).await;

        let mut message_2 = message.clone();
        message_2.id = Some("id:a".to_string());
        let response = request()
            .method("POST")
            .path(&format!("/did/{}/outbox", did))
            .json(&message_2)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_outbox_rejects_with_bcc() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let api = build_api(db);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = build_message("message", &jwk).await;
        message.members.as_mut().and_then(|x| {
            x.insert(
                "bcc".to_string(),
                serde_json::to_value("did:example:a").unwrap(),
            )
        });

        let response = request()
            .method("POST")
            .path(&format!("/did/{}/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn api_outbox_rejects_with_bto() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let api = build_api(db);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = build_message("message", &jwk).await;
        message.members.as_mut().and_then(|x| {
            x.insert(
                "bto".to_string(),
                serde_json::to_value("did:example:a").unwrap(),
            )
        });

        let response = request()
            .method("POST")
            .path(&format!("/did/{}/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
