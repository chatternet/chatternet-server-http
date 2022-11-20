use std::fs;
use std::sync::Arc;

use anyhow::Result;
use chatternet_server_http::chatternet::didkey::{build_jwk, did_from_jwk};
use clap::Parser;
use log::info;
use tokio;
use tokio::sync::RwLock;
use warp;

use chatternet_server_http::db::Connector;
use chatternet_server_http::handlers::build_api;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    port: u16,
    path_key: String,
    path_db: String,
    #[arg(short = 'k')]
    new_key: bool,
    #[arg(short = 'l')]
    loopback: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    let args = Args::parse();

    if args.new_key {
        let jwk = build_jwk(&mut rand::thread_rng())?;
        fs::write(&args.path_key, serde_json::to_string(&jwk)?)?;
    }
    let jwk = serde_json::from_str(&fs::read_to_string(&args.path_key)?)?;
    let did = did_from_jwk(&jwk)?;
    info!("DID: {}", &did);

    let connector = Arc::new(RwLock::new(
        Connector::new(&format!("sqlite:{}", args.path_db)).await?,
    ));
    let routes = build_api(connector, did);
    let address = if args.loopback {
        [127, 0, 0, 1]
    } else {
        [0, 0, 0, 0]
    };
    warp::serve(routes).run((address, args.port)).await;

    Ok(())
}
