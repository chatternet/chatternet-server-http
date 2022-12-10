//! Convert between [`JWK`] key meterial, DID Key IDs, and ChatterNet actor IDs.

use anyhow::{Error, Result};
use did_method_key::DIDKey;
use ed25519_dalek::{Keypair, PublicKey, SecretKey, SECRET_KEY_LENGTH};
use rand::{CryptoRng, RngCore};
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

/// Build the ChatterNet actor ID from the given `did`.
///
/// The `did` is already a URI, this appends the `/actor` path to it.
pub fn actor_id_from_did(did: &str) -> Result<String> {
    if !did.starts_with("did:") {
        Err(Error::msg("DID has invalid prefix"))?;
    }
    Ok(format!("{}/actor", did))
}

/// Extract the DID from a ChatterNet actor ID.
pub fn did_from_actor_id(actor_id: &str) -> Result<String> {
    let (did, path) = actor_id
        .split_once("/")
        .ok_or(Error::msg("actor ID is not a did and path"))?;
    if !did.starts_with("did:") {
        Err(Error::msg("actor ID is not a DID"))?;
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
        assert_eq!(
            actor_id_from_did("did:example:a").unwrap(),
            "did:example:a/actor"
        );
        actor_id_from_did("did").unwrap_err();
        actor_id_from_did("").unwrap_err();
        assert_eq!(
            did_from_actor_id("did:example:a/actor").unwrap(),
            "did:example:a"
        );
        did_from_actor_id("did:example:a/other").unwrap_err();
        did_from_actor_id("did:example:a/").unwrap_err();
        did_from_actor_id("did:example:a").unwrap_err();
        did_from_actor_id("").unwrap_err();
    }
}
