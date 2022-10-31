use anyhow;
use warp;

#[derive(Debug)]
pub enum Error {
    Any(anyhow::Error),
}

impl warp::reject::Reject for Error {}
