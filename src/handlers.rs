use anyhow::{anyhow, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use ssi::vc::Credential;
use std::collections::HashSet;
use std::sync::Arc;
use warp::{Filter, Rejection};

use crate::chatternet::activities::verify_message;
use crate::db::{Db, NO_TAGS};
use crate::errors::Error;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncMessagesBody {
    have_messages_id: Vec<String>,
    start_timestamp_micros: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncMessagesResponse {
    want_messages_id: Vec<String>,
}

async fn handle_sync_messages(
    body: SyncMessagesBody,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let has_messages = db
        .filter_has_messages(&body.have_messages_id, body.start_timestamp_micros)
        .await
        .map_err(Error)?;
    let want_messages_id: Vec<String> = (&HashSet::<String>::from_iter(body.have_messages_id)
        - &HashSet::<String>::from_iter(has_messages))
        .iter()
        .cloned()
        .collect();
    Ok(warp::reply::json(&SyncMessagesResponse {
        want_messages_id,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushMessagesBody {
    messages: Vec<Credential>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushMessagesResponse {
    accepted_messages_id: Vec<String>,
}

async fn handle_push_message(mut message: Credential, db: &Arc<Db>) -> Result<String> {
    let id = verify_message(&mut message).await?;
    let message = message;
    let issuer_did = message
        .issuer
        .as_ref()
        .ok_or(anyhow!("message has no issuer"))?
        .get_id();
    let message = serde_json::to_string(&message)?;
    let timestamp_micros = Utc::now().timestamp_micros();
    db.put_message(&message, &id, timestamp_micros, &issuer_did, NO_TAGS)
        .await?;
    Ok(id)
}

async fn handle_push_messages(
    body: PushMessagesBody,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let mut accepted_messages_id = Vec::new();
    for message in body.messages {
        let id = handle_push_message(message, &db).await;
        if let Ok(id) = id {
            accepted_messages_id.push(id);
        }
    }
    Ok(warp::reply::json(&PushMessagesResponse {
        accepted_messages_id,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchIssuersMessagesBody {
    issuers_did: Vec<String>,
    start_timestamp_micros: i64,
    tags_id: Option<Vec<String>>,
}

async fn handle_fetch_issuers_messages(
    body: FetchIssuersMessagesBody,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let messages = db
        .get_issuers_messages(
            &body.issuers_did,
            body.start_timestamp_micros,
            body.tags_id.as_ref().map(Vec::as_slice),
        )
        .await
        .map_err(Error)?;
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
    let route_sync_messages = warp::post()
        .and(warp::path("syncMessages"))
        .and(warp::body::json())
        .and(with_resource(db.clone()))
        .and_then(handle_sync_messages);
    let route_push_messages = warp::post()
        .and(warp::path("pushMessages"))
        .and(warp::body::json())
        .and(with_resource(db.clone()))
        .and_then(handle_push_messages);
    let route_fetch_issuers_messages = warp::post()
        .and(warp::path("fetchIssuersMessages"))
        .and(warp::body::json())
        .and(with_resource(db.clone()))
        .and_then(handle_fetch_issuers_messages);
    route_version
        .or(route_sync_messages)
        .or(route_push_messages)
        .or(route_fetch_issuers_messages)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use activitystreams::{object::Note, prelude::ObjectExt};
    use serde_json;
    use ssi::{jwk::JWK, vc::URI};
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::chatternet::activities::{
        build_message, cid_from_json, cid_to_urn, new_context_loader,
    };
    use crate::chatternet::didkey::{build_jwk, did_from_jwk};

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

    #[tokio::test]
    async fn api_handles_sync_messages() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        db.put_message("", "a:b", 10, "", NO_TAGS).await.unwrap();
        db.put_message("", "a:c", 11, "", NO_TAGS).await.unwrap();
        db.put_message("", "a:d", 11, "", NO_TAGS).await.unwrap();
        let api = build_api(db);
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = request()
            .method("POST")
            .path("/syncMessages")
            .json(&SyncMessagesBody {
                have_messages_id: vec!["a:b".to_string(), "a:c".to_string(), "a:e".to_string()],
                start_timestamp_micros: 11,
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: SyncMessagesResponse = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(body.want_messages_id),
            HashSet::<String>::from_iter(["a:b".to_string(), "a:e".to_string()])
        );
    }

    async fn build_note(jwk: &JWK, message: &str) -> (Credential, String) {
        let mut note = Note::new();
        note.set_content(message);
        let cid = cid_to_urn(
            cid_from_json(&note, &mut new_context_loader())
                .await
                .unwrap(),
        );
        (build_message(note, &jwk).await.unwrap(), cid)
    }

    #[tokio::test]
    async fn api_handles_push_messages() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let api = build_api(db);
        let (note_1, cid_1) = build_note(&jwk, "abc").await;
        let (note_2, cid_2) = build_note(&jwk, "abcd").await;
        let mut note_invalid = note_1.clone();
        note_invalid.credential_subject.to_single_mut().unwrap().id =
            Some(URI::from_str("a:b").unwrap());
        let response = request()
            .method("POST")
            .path("/pushMessages")
            .json(&PushMessagesBody {
                messages: vec![note_invalid, note_1, note_2],
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: PushMessagesResponse = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(body.accepted_messages_id),
            HashSet::<String>::from_iter([cid_1, cid_2]),
        );
    }

    #[tokio::test]
    async fn api_handles_fetch_issuers_messages() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let jwk_1 = build_jwk(&mut rand::thread_rng()).unwrap();
        let jwk_2 = build_jwk(&mut rand::thread_rng()).unwrap();
        let api = build_api(db);
        let (note_1, _) = build_note(&jwk_1, "").await;
        let start_timestamp_micros = 1 + note_1
            .proof
            .as_ref()
            .unwrap()
            .to_single()
            .unwrap()
            .created
            .unwrap()
            .timestamp_micros();
        tokio::time::sleep(tokio::time::Duration::from_micros(1)).await;
        let (note_2, _) = build_note(&jwk_1, "").await;
        let note_2_id = note_2.id.clone();
        let (note_3, _) = build_note(&jwk_2, "").await;
        let response = request()
            .method("POST")
            .path("/pushMessages")
            .json(&PushMessagesBody {
                messages: vec![note_1, note_2, note_3],
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = request()
            .method("POST")
            .path("/fetchIssuersMessages")
            .json(&FetchIssuersMessagesBody {
                issuers_did: vec![did_from_jwk(&jwk_1).unwrap()],
                start_timestamp_micros,
                tags_id: None,
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let messages: Vec<Credential> = serde_json::from_slice::<Vec<String>>(response.body())
            .unwrap()
            .iter()
            .map(AsRef::as_ref)
            .map(serde_json::from_str::<Credential>)
            .filter_map(|x| x.ok())
            .collect();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages.first().unwrap().id, note_2_id);
    }
}
