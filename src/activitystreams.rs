use std::collections::HashMap;

use activitystreams::iri_string::types::IriString;
use activitystreams::prelude::{BaseExt, ObjectExt};
use anyhow::{anyhow, Result};
use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use did_method_key::DIDKey;
use ed25519_dalek::{Keypair, PublicKey, SecretKey, SECRET_KEY_LENGTH};
use rand::{CryptoRng, RngCore};
use serde::Serialize;
use serde_json;
use ssi::did::{DIDMethod, Source};
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::jwk::{Base64urlUInt, OctetParams, Params, JWK};
use ssi::urdna2015;
use ssi::vc::{Credential, LinkedDataProofOptions};

use crate::contexts;

const CONTEXTS: [&str; 2] = [
    "https://www.w3.org/2018/credentials/v1",
    contexts::ACTIVITY_STREAMS_URI,
];

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

pub fn new_context_loader() -> ContextLoader {
    ContextLoader::empty()
        .with_static_loader()
        .with_context_map_from(HashMap::from([(
            contexts::ACTIVITY_STREAMS_URI.to_owned(),
            contexts::ACTIVITY_STREAMS.to_owned(),
        )]))
        .unwrap()
}

pub async fn cid_from_json(
    object: &impl Serialize,
    context_loader: &mut ContextLoader,
) -> Result<Cid> {
    let json = serde_json::to_string(object)?;
    let more_contexts = serde_json::to_string(&CONTEXTS)?;
    let dataset = json_to_dataset(&json, Some(&more_contexts), false, None, context_loader).await?;
    let dataset_normalized = urdna2015::normalize(&dataset)?;
    let doc_normalized = dataset_normalized.to_nquads()?;
    Ok(Cid::new_v1(
        0x55,
        Code::Sha2_256.digest(doc_normalized.as_bytes()),
    ))
}

pub fn cid_to_urn(cid: Cid) -> String {
    format!("urn:cid:{}", cid.to_string())
}

/// Build a Chatter Net Credential
///
/// This is a Verifiable Credential whose subject is an Activity Stream Object.
pub async fn build_credential<Kind>(
    mut subject: impl ObjectExt<Kind> + BaseExt<Kind> + Serialize,
    jwk: &JWK,
) -> Result<Credential> {
    // build an ID which is isomorphic to the subject object such that new
    // messages cannot override old ones
    let id = cid_to_urn(cid_from_json(&subject, &mut new_context_loader()).await?);
    subject.set_id(id.parse::<IriString>().unwrap());

    let did = did_from_jwk(&jwk)?;

    // the credential document wrapping the subject document
    let mut credential: Credential = serde_json::from_value(serde_json::json!({
        "@context": CONTEXTS,
        "type": "VerifiableCredential",
        "issuer": did,
        "issuanceDate": ssi::ldp::now_ms(),
        "credentialSubject": subject
    }))?;

    // add the proof once the subject is known
    credential.add_proof(
        credential
            .generate_proof(
                &jwk,
                &LinkedDataProofOptions::default(),
                &DIDKey,
                &mut new_context_loader(),
            )
            .await?,
    );

    Ok(credential)
}

pub async fn verify_credential(credential: &mut Credential) -> Result<String> {
    if let Some(error) = credential
        .verify(None, &DIDKey, &mut new_context_loader())
        .await
        .errors
        .into_iter()
        .nth(0)
    {
        return Err(anyhow!(error));
    }
    let subject = credential
        .credential_subject
        .to_single_mut()
        .ok_or(anyhow!("credential does not contain a single subject"))?;
    let id = subject
        .id
        .take()
        .ok_or(anyhow!("credential subject does not have an ID"))?;
    let id_data = cid_to_urn(cid_from_json(subject, &mut new_context_loader()).await?);
    // TODO: compare CIDs (could be different encoding)
    if id_data != id.as_str() {
        return Err(anyhow!("credential subject ID does not match its contents"));
    }
    // return the credential to its original state
    subject.id = Some(id);
    Ok(id_data)
}

#[cfg(test)]
mod test {
    use activitystreams::activity::Create;
    use activitystreams::base::ExtendsExt;
    use activitystreams::object::Note;
    use tokio;

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

    #[tokio::test]
    async fn builds_cid_from_object() {
        let mut note = Note::new();
        note.set_content("abc");
        let note = note.into_any_base().unwrap();
        let activity_1 = Create::new("http://example.com/user_1", note.clone());
        let activity_2 = Create::new("http://example.com/user_2", note.clone());
        let cid_1 = cid_from_json(&activity_1, &mut new_context_loader())
            .await
            .unwrap();
        let cid_2 = cid_from_json(&activity_2, &mut new_context_loader())
            .await
            .unwrap();
        assert_ne!(cid_1.to_string(), cid_2.to_string());
    }

    #[tokio::test]
    async fn builds_credential_and_verifies() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let mut note = Note::new();
        note.set_content("abc");
        let cid = cid_to_urn(
            cid_from_json(&note, &mut new_context_loader())
                .await
                .unwrap(),
        );
        let mut credential = build_credential(note, &jwk).await.unwrap();
        let credential_before = serde_json::to_string(&credential).unwrap();
        let cid_verified = verify_credential(&mut credential).await.unwrap();
        assert_eq!(cid, cid_verified);
        let credential_after = serde_json::to_string(&credential).unwrap();
        // ensure the mutable borrow didn't change the credential
        assert_eq!(credential_before, credential_after);
    }

    #[tokio::test]
    async fn builds_credential_modified_doesnt_verify() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let mut note = Note::new();
        note.set_content("abc");

        let mut credential = build_credential(note.clone(), &jwk).await.unwrap();
        credential.id = Some(ssi::vc::URI::try_from("a:b".to_string()).unwrap());
        verify_credential(&mut credential).await.unwrap_err();
    }

    // TODO: something isn't quite right yet
    // https://github.com/timothee-haudebourg/json-ld/pull/13
    // https://github.com/timothee-haudebourg/json-ld/pull/14
    // https://github.com/timothee-haudebourg/json-ld/issues/39
    // https://releases.rs/docs/unreleased/1.65.0/
    // #[tokio::test]
    #[allow(dead_code)]
    async fn builds_credential_aribrary_data_doesnt_verify() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let mut note = Note::new();
        note.set_content("abc");

        let mut credential = build_credential(note.clone(), &jwk).await.unwrap();
        credential
            .credential_subject
            .to_single_mut()
            .unwrap()
            .property_set
            .as_mut()
            .unwrap()
            .insert(
                "new key".to_string(),
                serde_json::to_value("new value").unwrap(),
            );
        verify_credential(&mut credential).await.unwrap_err();
    }
}
