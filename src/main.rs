use std::fs;
use std::sync::Arc;

use anyhow::Result;
use axum;
use chatternet_server_http::chatternet::didkey::{build_jwk, did_from_jwk};
use clap::Parser;
use serde::Serialize;
use serde_json;
use tokio;
use tokio::sync::RwLock;

use chatternet_server_http::db::Connector;
use chatternet_server_http::handlers::build_api;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    port: u16,
    path_key: String,
    path_db: String,
    #[arg(short = 'p', default_value = "api")]
    prefix: String,
    #[arg(short = 'k')]
    new_key: bool,
    #[arg(short = 'l')]
    loopback: bool,
}

#[derive(Serialize)]
struct ServerInfo {
    url: String,
    did: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    if args.new_key {
        let jwk = build_jwk(&mut rand::thread_rng())?;
        fs::write(&args.path_key, serde_json::to_string(&jwk)?)?;
    }
    let jwk = serde_json::from_str(&fs::read_to_string(&args.path_key)?)?;
    let did = did_from_jwk(&jwk)?;
    tracing::info!("DID: {}", &did);

    let connector = Arc::new(RwLock::new(
        Connector::new(&format!("sqlite:{}", args.path_db)).await?,
    ));
    let app = build_api(connector, &args.prefix, &did);
    let address = if args.loopback {
        "127.0.0.1"
    } else {
        "0.0.0.0"
    };

    let url = format!("http://{}:{}/{}", address, args.port, args.prefix);
    let server_info = ServerInfo { url, did };
    fs::write("server-info.json", serde_json::to_string(&server_info)?)?;

    // run it with hyper on localhost:3000
    axum::Server::bind(&format!("{}:{}", address, args.port).parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}
