use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio;
use tokio::sync::RwLock;
use warp;

use chatternet_server_http::db::Connector;
use chatternet_server_http::handlers::build_api;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    port: u16,
    #[arg(short, long)]
    loopback: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    let args = Args::parse();
    let connector = Arc::new(RwLock::new(Connector::new("sqlite::memory:").await?));
    let routes = build_api(connector);
    let address = if args.loopback {
        [127, 0, 0, 1]
    } else {
        [0, 0, 0, 0]
    };
    warp::serve(routes).run((address, args.port)).await;
    Ok(())
}
