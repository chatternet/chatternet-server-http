use std::fs;
use std::path::PathBuf;

use anyhow::{Error, Result};
use chatternet::didkey::{actor_id_from_did, did_from_jwk};
use chatternet::model::Uri;
use clap::{Parser, Subcommand};
use serde_json;
use tokio;

use chatternet_server_http::db::{self, Connector};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    path_key: PathBuf,
    path_db: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Follow { actor_id: Uri },
    ListFollows { actor_id: Uri },
    ListServerFollows,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let jwk = serde_json::from_str(&fs::read_to_string(&args.path_key)?)?;
    let server_did = did_from_jwk(&jwk)?;
    let server_actor_id = actor_id_from_did(&server_did)?;
    let mut connector = Connector::new(&format!(
        "sqlite:{}",
        args.path_db.to_str().ok_or(Error::msg("invalid DB path"))?
    ))
    .await?;

    match args.command {
        Commands::Follow { actor_id } => {
            let mut connection = connector.connection_mut().await?;
            db::put_actor_following(&mut *connection, &server_actor_id, actor_id.as_str()).await?;
            db::put_actor_audience(
                &mut *connection,
                &server_actor_id,
                &format!("{}/followers", actor_id.as_str()),
            )
            .await?;
        }
        Commands::ListFollows { actor_id } => {
            let mut connection = connector.connection_mut().await?;
            for id in db::get_actor_followings(&mut *connection, actor_id.as_str()).await? {
                println!("{}", id);
            }
        }
        Commands::ListServerFollows => {
            let mut connection = connector.connection_mut().await?;
            for id in db::get_actor_followings(&mut *connection, &server_actor_id).await? {
                println!("{}", id);
            }
        }
    };

    Ok(())
}
