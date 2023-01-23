//! Convert between [`JWK`] key meterial, DID Key IDs, and ChatterNet actor IDs.

use anyhow::{Error, Result};
use did_method_key::DIDKey;
use ed25519_dalek::{Keypair, PublicKey, SecretKey, SECRET_KEY_LENGTH};
use lazy_static::lazy_static;
use rand::{CryptoRng, RngCore};
use regex::Regex;
use ssi::did::{DIDMethod, Source};
use ssi::jwk::{Base64urlUInt, OctetParams, Params, JWK};

/// Build a new [`JWK`] key which can be represnted by DID Key and used to
/// represent a ChatterNet user.
pub fn build_jwk(rng: &mut (impl CryptoRng + RngCore)) -> Result<JWK> {
    let mut secret_key_bytes = [0u8; SECRET_KEY_LENGTH];
    rng.fill_bytes(&mut secret_key_bytes);
    let secret = SecretKey::from_bytes(&secret_key_bytes).unwrap();
    let public: PublicKey = (&secret).into();
    let keypair = Keypair { secret, public };
    Ok(JWK::from(Params::OKP(OctetParams {
        curve: "Ed25519".to_string(),
        public_key: Base64urlUInt(keypair.public.as_ref().to_vec()),
        private_key: Some(Base64urlUInt(keypair.secret.as_ref().to_vec())),
    })))
}

/// Build the DID representation of the `jwk` key.
pub fn did_from_jwk(jwk: &JWK) -> Result<String> {
    DIDKey
        .generate(&Source::Key(&jwk))
        .ok_or(Error::msg("key pair cannot be represented as a DID"))
}

lazy_static! {
    static ref RE_DID: Regex = Regex::new(r"^did:key:z[a-km-zA-HJ-NP-Z1-9]+$").unwrap();
}

/// Check if the given string `did` is a valid DID Key.
pub fn is_valid_did(did: &str) -> bool {
    RE_DID.is_match(did)
}

/// Build the ChatterNet actor ID from the given `did`.
///
/// The `did` is already a URI, this appends the `/actor` path to it.
pub fn actor_id_from_did(did: &str) -> Result<String> {
    if !is_valid_did(did) {
        Err(Error::msg("DID has invalid characters"))?;
    }
    Ok(format!("{}/actor", did))
}

/// Extract the DID from a ChatterNet actor ID.
pub fn did_from_actor_id(actor_id: &str) -> Result<String> {
    let (did, path) = actor_id
        .split_once("/")
        .ok_or(Error::msg("actor ID is not a did and path"))?;
    if !is_valid_did(did) {
        Err(Error::msg("acotr ID doesn't contain a valid DID"))?;
    }
    if path != "actor" {
        Err(Error::msg("actor ID path is not an actor"))?;
    }
    Ok(did.to_string())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn builds_jwk() {
        build_jwk(&mut rand::thread_rng()).unwrap();
    }

    #[test]
    fn builds_did_from_jwk() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        assert!(did.starts_with("did:key:"));
    }

    #[test]
    fn transforms_did_to_and_from_actor_id() {
        assert_eq!(actor_id_from_did("did:key:za").unwrap(), "did:key:za/actor");
        actor_id_from_did("did:other:za").unwrap_err();
        actor_id_from_did("did:key:invalid").unwrap_err();
        actor_id_from_did("").unwrap_err();
        assert_eq!(did_from_actor_id("did:key:za/actor").unwrap(), "did:key:za");
        did_from_actor_id("did:key:za/other").unwrap_err();
        did_from_actor_id("did:key:za/").unwrap_err();
        did_from_actor_id("did:key:za").unwrap_err();
        did_from_actor_id("").unwrap_err();
    }
}
