use anyhow;
use warp;

#[derive(Debug)]
pub enum Error {
    Any(anyhow::Error),
    Json(serde_json::Error),
}

impl warp::reject::Reject for Error {}
