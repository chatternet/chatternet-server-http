use std::fs;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Error, Result};
use chatternet::didkey::{build_jwk, did_from_jwk};
use chatternet::model::{ActorFields, ActorType};
use clap::Parser;
use serde_json;
use tokio;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    path_key: PathBuf,
    path_out: PathBuf,
    url_base: String,
    name: String,
    #[arg(short = 'p')]
    prefix: Option<String>,
    #[arg(short = 'k')]
    new_key: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.new_key {
        let jwk = build_jwk(&mut rand::thread_rng())?;
        fs::OpenOptions::new()
            // ensure not overwriting existing key
            .create_new(true)
            .write(true)
            .open(&args.path_key)
            .map_err(|_| {
                Error::msg("unable to write a new key file, ensure no file already exists")
            })?
            .write_all(serde_json::to_string(&jwk)?.as_bytes())?;
    }
    let jwk = serde_json::from_str(&fs::read_to_string(&args.path_key)?)?;
    let did = did_from_jwk(&jwk)?;
    if args.url_base.ends_with('/') {
        Err(Error::msg("url base has a trailing /"))?;
    }
    let url = match args.prefix {
        Some(prefix) => format!("{}/{}/{}/actor", args.url_base, prefix, &did),
        None => format!("{}/{}/actor", args.url_base, &did),
    };
    let actor = ActorFields::new(&jwk, ActorType::Service, Some(args.name), Some(url)).await?;
    fs::write(args.path_out, serde_json::to_string_pretty(&actor)?)?;
    Ok(())
}
