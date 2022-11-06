use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio;
use warp;

use chatternet_server_http::db::new_db_pool;
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
    let db_pool = Arc::new(new_db_pool("sqlite::memory:").await?);
    let routes = build_api(db_pool);
    let address = if args.loopback {
        [127, 0, 0, 1]
    } else {
        [0, 0, 0, 0]
    };
    warp::serve(routes).run((address, args.port)).await;
    Ok(())
}
