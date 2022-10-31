use anyhow::Result;
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
pub struct PostActivitiesBody {
    activities: Vec<Credential>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PostActivitiesResponse {
    accepted_activities_id: Vec<String>,
}

async fn handle_post_activity(mut activity: Credential, db: &Arc<Db>) -> Result<String> {
    let id = verify_credential(&mut activity).await?;
    let activity = serde_json::to_string(&activity.credential_subject.to_single())?;
    let timestamp_millis = Utc::now().timestamp_millis();
    db.put_activity(&id, timestamp_millis, &activity).await?;
    Ok(id)
}

async fn handle_post_activities(
    body: PostActivitiesBody,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let mut accepted_activities_id = Vec::new();
    for activity in body.activities {
        let id = handle_post_activity(activity, &db).await;
        if let Ok(id) = id {
            accepted_activities_id.push(id);
        }
    }
    Ok(warp::reply::json(&PostActivitiesResponse {
        accepted_activities_id,
    }))
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
    let route_post_activities = warp::post()
        .and(warp::path("post_activities"))
        .and(warp::body::json())
        .and(with_resource(db.clone()))
        .and_then(handle_post_activities);
    route_version
        .or(route_sync_activities)
        .or(route_post_activities)
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
        build_credential, build_jwk, cid_from_json, cid_to_urn, new_context_loader,
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
        db.put_activity("a:b", 10, "abc").await.unwrap();
        db.put_activity("a:c", 11, "abc").await.unwrap();
        db.put_activity("a:d", 11, "abc").await.unwrap();
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
    async fn api_handles_post_activities() {
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
            .path("/post_activities")
            .json(&PostActivitiesBody {
                activities: vec![note_invalid, note_1, note_2],
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: PostActivitiesResponse = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(body.accepted_activities_id),
            HashSet::<String>::from_iter([cid_1, cid_2]),
        );
    }
}
