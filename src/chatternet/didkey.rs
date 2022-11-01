use anyhow::{anyhow, Result};
use did_method_key::DIDKey;
use ed25519_dalek::{Keypair, PublicKey, SecretKey, SECRET_KEY_LENGTH};
use rand::{CryptoRng, RngCore};
use ssi::did::{DIDMethod, Source};
use ssi::jwk::{Base64urlUInt, OctetParams, Params, JWK};

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

pub fn did_from_jwk(jwk: &JWK) -> Result<String> {
    DIDKey
        .generate(&Source::Key(&jwk))
        .ok_or(anyhow!("key pair cannot be represented as a DID"))
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
}
