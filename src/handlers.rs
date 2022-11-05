use anyhow::{anyhow, Result};
use std::sync::Arc;
use warp::http::StatusCode;
use warp::{Filter, Rejection};

use crate::chatternet::activities::Message;
use crate::db::Db;
use crate::errors::Error;

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

    let mut audiences_id: Vec<String> = Vec::new();

    let tags_id = message
        .members
        .as_ref()
        .and_then(|x| x.get("to"))
        .and_then(|x| x.as_array())
        .and_then(|x| {
            x.iter()
                .map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Option<Vec<String>>>()
        });
    if let Some(mut tags_id) = tags_id {
        audiences_id.append(&mut tags_id);
    }

    let tags_id = message
        .members
        .as_ref()
        .and_then(|x| x.get("cc"))
        .and_then(|x| x.as_array())
        .and_then(|x| {
            x.iter()
                .map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Option<Vec<String>>>()
        });
    if let Some(mut tags_id) = tags_id {
        audiences_id.append(&mut tags_id);
    }

    let tags_id = message
        .members
        .as_ref()
        .and_then(|x| x.get("tags"))
        .and_then(|x| x.as_array())
        .and_then(|x| {
            x.iter()
                .map(|x| serde_json::from_value::<String>(x.to_owned()).ok())
                .collect::<Option<Vec<String>>>()
        });
    if let Some(mut tags_id) = tags_id {
        audiences_id.append(&mut tags_id);
    }

    // TODO: error for bto, tcc

    let message = serde_json::to_string(&message).map_err(|x| Error(anyhow!(x)))?;
    db.put_message(&message, &id, &actor_did, &audiences_id[..])
        .await
        .map_err(Error)?;
    Ok(StatusCode::OK)
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
    route_version.or(route_outbox)
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
    async fn api_handles_outbox() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let api = build_api(db);

        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = build_message("abc", &jwk).await;

        let response = request()
            .method("POST")
            .path(&format!("/did/{}/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = request()
            .method("POST")
            .path(&format!("/did/{}/outbox", did))
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = request()
            .method("POST")
            .path("/did/did:example:a/outbox")
            .json(&message)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let mut message_2 = message.clone();
        message_2.id = Some("a:b".to_string());
        let response = request()
            .method("POST")
            .path(&format!("/did/{}/outbox", did))
            .json(&message_2)
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
