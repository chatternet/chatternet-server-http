use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use warp::{Filter, Rejection};

use crate::db::Db;
use crate::errors::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncActivitiesBody {
    have_activities: Vec<String>,
    since_timestamp_millis: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncActivitiesResponse {
    want_ids: Vec<String>,
}

async fn handle_sync_activities(
    body: SyncActivitiesBody,
    db: Arc<Db>,
) -> Result<impl warp::Reply, Rejection> {
    let has_activities = db
        .filter_has_activities(&body.have_activities, body.since_timestamp_millis)
        .await
        .map_err(Error::Any)?;
    let want_ids: Vec<String> = (&HashSet::<String>::from_iter(body.have_activities)
        - &HashSet::<String>::from_iter(has_activities))
        .iter()
        .cloned()
        .collect();
    Ok(warp::reply::json(&SyncActivitiesResponse { want_ids }))
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
    let route_push = warp::post()
        .and(warp::path("sync_activities"))
        .and(warp::body::json())
        .and(with_resource(db))
        .and_then(handle_sync_activities);
    route_version.or(route_push)
}

#[cfg(test)]
mod test {
    use serde_json;
    use tokio;
    use warp::{http::StatusCode, test::request};

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
        db.put_activity("a:b", 10).await.unwrap();
        db.put_activity("a:c", 11).await.unwrap();
        db.put_activity("a:d", 11).await.unwrap();
        let api = build_api(db);
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let response = request()
            .method("POST")
            .path("/sync_activities")
            .json(&SyncActivitiesBody {
                have_activities: vec!["a:b".to_string(), "a:c".to_string(), "a:e".to_string()],
                since_timestamp_millis: 10,
            })
            .reply(&api)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: SyncActivitiesResponse = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(body.want_ids),
            HashSet::<String>::from_iter(["a:b".to_string(), "a:e".to_string()])
        );
    }
}
