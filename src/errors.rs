use anyhow;
use warp;

#[derive(Debug)]
pub struct Error(pub anyhow::Error);

impl warp::reject::Reject for Error {}
