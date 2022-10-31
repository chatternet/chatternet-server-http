use anyhow::{anyhow, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use ssi::vc::Credential;
use std::collections::HashSet;
use std::sync::Arc;
use warp::{Filter, Rejection};

use crate::activitystreams::verify_credential;
use crate::db::Db;
use crate::errors::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncActivitiesBody {
    have_activities_id: Vec<String>,
    since_timestamp_millis: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncActivitiesResponse {
    want_activities_id: Vec<String>,
}

async fn handle_sync_activities(
    body: SyncActivitiesBody,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let has_activities = db
        .filter_has_activities(&body.have_activities_id, body.since_timestamp_millis)
        .await
        .map_err(Error::Any)?;
    let want_activities_id: Vec<String> = (&HashSet::<String>::from_iter(body.have_activities_id)
        - &HashSet::<String>::from_iter(has_activities))
        .iter()
        .cloned()
        .collect();
    Ok(warp::reply::json(&SyncActivitiesResponse {
        want_activities_id,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PushActivitiesBody {
    activities: Vec<Credential>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PushActivitiesResponse {
    accepted_activities_id: Vec<String>,
}

async fn handle_push_activity(mut activity: Credential, db: &Arc<Db>) -> Result<String> {
    let id = verify_credential(&mut activity).await?;
    let activity = activity;
    let issuer_did = activity
        .issuer
        .as_ref()
        .ok_or(anyhow!("activity has no issuer"))?
        .get_id();
    let activity = serde_json::to_string(&activity)?;
    let timestamp_millis = Utc::now().timestamp_millis();
    db.put_activity(&id, timestamp_millis, &issuer_did, &activity)
        .await?;
    Ok(id)
}

async fn handle_push_activities(
    body: PushActivitiesBody,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let mut accepted_activities_id = Vec::new();
    for activity in body.activities {
        let id = handle_push_activity(activity, &db).await;
        if let Ok(id) = id {
            accepted_activities_id.push(id);
        }
    }
    Ok(warp::reply::json(&PushActivitiesResponse {
        accepted_activities_id,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchIssuersActivitiesBody {
    issuers_did: Vec<String>,
    since_timestamp_millis: i64,
}

async fn handle_fetch_issuers_activities(
    body: FetchIssuersActivitiesBody,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let activities = db
        .get_issuers_activities(&body.issuers_did, body.since_timestamp_millis)
        .await
        .map_err(Error::Any)?;
    Ok(warp::reply::json(&activities))
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
    let route_sync_activities = warp::post()
        .and(warp::path("sync_activities"))
        .and(warp::body::json())
        .and(with_resource(db.clone()))
        .and_then(handle_sync_activities);
    let route_push_activities = warp::post()
        .and(warp::path("push_activities"))
        .and(warp::body::json())
        .and(with_resource(db.clone()))
        .and_then(handle_push_activities);
    let route_fetch_issuers_activities = warp::post()
        .and(warp::path("fetch_issuers_activities"))
        .and(warp::body::json())
        .and(with_resource(db.clone()))
        .and_then(handle_fetch_issuers_activities);
    route_version
        .or(route_sync_activities)
        .or(route_push_activities)
        .or(route_fetch_issuers_activities)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use activitystreams::{object::Note, prelude::ObjectExt};
    use serde_json;
    use ssi::{jwk::JWK, vc::URI};
    use tokio;
    use warp::{http::StatusCode, test::request};

    use crate::activitystreams::{
        build_credential, build_jwk, cid_from_json, cid_to_urn, did_from_jwk, new_context_loader,
    };

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
    async fn api_handles_sync_activities() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        db.put_activity("a:b", 10, "", "").await.unwrap();
        db.put_activity("a:c", 11, "", "").await.unwrap();
        db.put_activity("a:d", 11, "", "").await.unwrap();
        let api = build_api(db);
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = request()
            .method("POST")
            .path("/sync_activities")
            .json(&SyncActivitiesBody {
                have_activities_id: vec!["a:b".to_string(), "a:c".to_string(), "a:e".to_string()],
                since_timestamp_millis: 10,
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: SyncActivitiesResponse = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(body.want_activities_id),
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
        (build_credential(note, &jwk).await.unwrap(), cid)
    }

    #[tokio::test]
    async fn api_handles_push_activities() {
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
            .path("/push_activities")
            .json(&PushActivitiesBody {
                activities: vec![note_invalid, note_1, note_2],
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: PushActivitiesResponse = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(body.accepted_activities_id),
            HashSet::<String>::from_iter([cid_1, cid_2]),
        );
    }

    #[tokio::test]
    async fn api_handles_fetch_issuers_activities() {
        let db = Arc::new(Db::new("sqlite::memory:").await.unwrap());
        let jwk_1 = build_jwk(&mut rand::thread_rng()).unwrap();
        let jwk_2 = build_jwk(&mut rand::thread_rng()).unwrap();
        let api = build_api(db);
        let (note_1, _) = build_note(&jwk_1, "").await;
        let since_timestamp_millis = note_1
            .proof
            .as_ref()
            .unwrap()
            .to_single()
            .unwrap()
            .created
            .unwrap()
            .timestamp_millis();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let (note_2, _) = build_note(&jwk_1, "").await;
        let note_2_id = note_2.id.clone();
        let (note_3, _) = build_note(&jwk_2, "").await;
        let response = request()
            .method("POST")
            .path("/push_activities")
            .json(&PushActivitiesBody {
                activities: vec![note_1, note_2, note_3],
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = request()
            .method("POST")
            .path("/fetch_issuers_activities")
            .json(&FetchIssuersActivitiesBody {
                issuers_did: vec![did_from_jwk(&jwk_1).unwrap()],
                since_timestamp_millis,
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let activities: Vec<Credential> = serde_json::from_slice::<Vec<String>>(response.body())
            .unwrap()
            .iter()
            .map(AsRef::as_ref)
            .map(serde_json::from_str::<Credential>)
            .filter_map(|x| x.ok())
            .collect();
        assert_eq!(activities.len(), 1);
        assert_eq!(activities.first().unwrap().id, note_2_id);
    }
}
