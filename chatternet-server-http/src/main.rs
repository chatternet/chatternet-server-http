use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Error, Result};
use axum;
use chatternet::didkey::did_from_actor_id;
use chatternet::model::{Actor, ActorFields, Document};
use clap::Parser;
use serde_json;
use tokio;
use tokio::sync::RwLock;

use chatternet_server_http::db::{self, Connector};
use chatternet_server_http::handlers::{build_api, AppState};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    port: u16,
    path_actor: PathBuf,
    path_key: PathBuf,
    path_db: PathBuf,
    #[arg(short = 'l')]
    loopback: bool,
}

struct ParsedUrl {
    did: String,
    prefix: String,
}

fn parse_actor_url(actor: &(impl Actor + Document)) -> Result<ParsedUrl> {
    let actor_id = actor.id().as_str();
    let slash_actor_id = format!("/{}", actor_id);
    let actor_url = actor
        .url()
        .as_ref()
        .ok_or(Error::msg("server actor has no URL"))?
        .as_str();
    let actor_url = if actor_url.starts_with("https://") {
        &actor_url["https://".len()..]
    } else if actor_url.starts_with("http://") {
        &actor_url["http://".len()..]
    } else {
        Err(Error::msg("actor URL is not an HTTP endpoint"))?
    };
    if !actor_url.ends_with(&slash_actor_id) {
        Err(Error::msg("actor URL is not a path to the actor ID"))?;
    }
    let actor_url = &actor_url[..actor_url.len() - slash_actor_id.len()];
    let prefix = match actor_url.split_once('/') {
        Some((_, prefix)) => prefix,
        None => "",
    }
    .to_string();
    let did = did_from_actor_id(actor_id)?;
    Ok(ParsedUrl { did, prefix })
}

async fn store_actor(actor: &ActorFields, connector: Arc<RwLock<Connector>>) -> Result<()> {
    actor.verify().await?;
    let mut connector = connector.write().await;
    let mut connection = connector.connection_mut().await?;
    db::put_document(
        &mut *connection,
        actor.id().as_str(),
        &serde_json::to_string(actor)?,
    )
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let actor: ActorFields = serde_json::from_slice(&fs::read(args.path_actor)?)?;
    tracing::info!("{}", serde_json::to_string_pretty(&actor)?);

    let connector = Arc::new(RwLock::new(
        Connector::new(&format!(
            "sqlite:{}",
            args.path_db.to_str().ok_or(Error::msg("invalid DB path"))?
        ))
        .await?,
    ));
    store_actor(&actor, connector.clone()).await?;
    let jwk = Arc::new(serde_json::from_str(&fs::read_to_string(&args.path_key)?)?);
    let state = AppState { connector, jwk };

    let parsed_url = parse_actor_url(&actor)?;

    let app = build_api(state, &parsed_url.prefix, &parsed_url.did);
    let address = if args.loopback {
        "127.0.0.1"
    } else {
        "0.0.0.0"
    };

    axum::Server::bind(&format!("{}:{}", address, args.port).parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

#[cfg(test)]
mod test {
    use chatternet::{
        didkey::{actor_id_from_did, build_jwk, did_from_jwk},
        model::ActorType,
    };

    use super::*;

    #[tokio::test]
    async fn parses_actor_url_no_prefix() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let actor_id = actor_id_from_did(&did).unwrap();
        let url = format!("https://abc.example/{}", actor_id);
        let actor = ActorFields::new(&jwk, ActorType::Service, None, Some(url))
            .await
            .unwrap();
        let parsed_url = parse_actor_url(&actor).unwrap();
        assert_eq!(parsed_url.did, did);
        assert_eq!(parsed_url.prefix, "");
    }

    #[tokio::test]
    async fn parses_actor_url_with_prefix() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let actor_id = actor_id_from_did(&did).unwrap();
        let url = format!("https://abc.example/a/b/{}", actor_id);
        let actor = ActorFields::new(&jwk, ActorType::Service, None, Some(url))
            .await
            .unwrap();
        let parsed_url = parse_actor_url(&actor).unwrap();
        assert_eq!(parsed_url.did, did);
        assert_eq!(parsed_url.prefix, "a/b");
    }
}
