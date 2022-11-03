use std::collections::HashMap;

use activitystreams::iri_string::types::IriString;
use activitystreams::prelude::{BaseExt, ObjectExt};
use anyhow::{anyhow, Result};
use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use did_method_key::DIDKey;
use serde::Serialize;
use serde_json;
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::jwk::JWK;
use ssi::ldp::LinkedDataDocument;
use ssi::urdna2015;
use ssi::vc::{Credential, LinkedDataProofOptions};

use super::didkey;
use super::ldcontexts;

const CONTEXTS: [&str; 2] = [
    "https://www.w3.org/2018/credentials/v1",
    ldcontexts::ACTIVITY_STREAMS_URI,
];

pub fn new_context_loader() -> ContextLoader {
    ContextLoader::empty()
        .with_static_loader()
        .with_context_map_from(HashMap::from([(
            ldcontexts::ACTIVITY_STREAMS_URI.to_owned(),
            ldcontexts::ACTIVITY_STREAMS.to_owned(),
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

/// Build a Chatter Net message.
///
/// This is a Verifiable Credential whose subject is an Activity Stream Object.
pub async fn build_message<Kind>(
    mut subject: impl ObjectExt<Kind> + BaseExt<Kind> + Serialize,
    jwk: &JWK,
) -> Result<Credential> {
    // build an ID which is isomorphic to the subject object such that new
    // messages cannot override old ones
    let id = cid_to_urn(cid_from_json(&subject, &mut new_context_loader()).await?);
    subject.set_id(id.parse::<IriString>().unwrap());

    let did = didkey::did_from_jwk(&jwk)?;

    // the credential document wrapping the subject document
    let mut message: Credential = serde_json::from_value(serde_json::json!({
        "@context": CONTEXTS,
        "type": "VerifiableCredential",
        "issuer": did,
        "issuanceDate": ssi::ldp::now_ms(),
        "credentialSubject": subject
    }))?;

    // add the proof once the subject is known
    message.add_proof(
        message
            .generate_proof(
                &jwk,
                &LinkedDataProofOptions::default(),
                &DIDKey,
                &mut new_context_loader(),
            )
            .await?,
    );

    Ok(message)
}

pub async fn verify_message(message: &mut Credential) -> Result<String> {
    if let Some(error) = message
        .verify(None, &DIDKey, &mut new_context_loader())
        .await
        .errors
        .into_iter()
        .nth(0)
    {
        return Err(anyhow!(error));
    }
    let issuer_id = message
        .get_issuer()
        .ok_or(anyhow!("message does not contain an issuer ID"))?
        .to_string();
    let subject = message
        .credential_subject
        .to_single_mut()
        .ok_or(anyhow!("message does not contain a single subject"))?;
    let actor_id = subject
        .property_set
        .as_ref()
        .and_then(|x| x.get("actor"))
        .and_then(|x| x.get("id"))
        .and_then(|x| x.as_str())
        .ok_or(anyhow!(
            "message subject does not have a single actor with an ID"
        ))?;
    if issuer_id != actor_id {
        return Err(anyhow!("message issuer ID does not match subject actor ID"));
    }
    let id = subject
        .id
        .take()
        .ok_or(anyhow!("message subject does not have an ID"))?;
    let id_data = cid_to_urn(cid_from_json(subject, &mut new_context_loader()).await?);
    // TODO: compare CIDs (could be different encoding)
    if id_data != id.as_str() {
        return Err(anyhow!("message subject ID does not match its contents"));
    }
    // return the message to its original state
    subject.id = Some(id);
    Ok(id_data)
}

#[cfg(test)]
mod test {
    use activitystreams::activity::Create;
    use activitystreams::actor::Person;
    use activitystreams::base::{BaseExt, ExtendsExt};
    use activitystreams::object::Note;
    use activitystreams::prelude::ActorAndObjectRefExt;
    use tokio;

    use super::*;

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

    fn build_activity(did: &str, content: &str) -> Create {
        let mut note = Note::new();
        note.set_content(content);
        let mut actor = Person::new();
        actor.set_id(did.parse::<IriString>().unwrap());
        Create::new(
            actor.into_any_base().unwrap(),
            note.into_any_base().unwrap(),
        )
    }

    #[tokio::test]
    async fn builds_message_and_verifies() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let activity = build_activity(&did, "abc");
        let cid = cid_to_urn(
            cid_from_json(&activity, &mut new_context_loader())
                .await
                .unwrap(),
        );
        let mut message = build_message(activity, &jwk).await.unwrap();
        let message_before = serde_json::to_string(&message).unwrap();
        let cid_verified = verify_message(&mut message).await.unwrap();
        assert_eq!(cid, cid_verified);
        let message_after = serde_json::to_string(&message).unwrap();
        // ensure the mutable borrow didn't change the message
        assert_eq!(message_before, message_after);
    }

    #[tokio::test]
    async fn builds_message_wrong_actor_id_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut activity = build_activity(&did, "abc");
        let actor = Person::new();
        activity.set_actor(actor.into_any_base().unwrap());
        let mut message = build_message(activity, &jwk).await.unwrap();
        message.id = Some(ssi::vc::URI::try_from("a:b".to_string()).unwrap());
        verify_message(&mut message).await.unwrap_err();
    }

    #[tokio::test]
    async fn builds_message_modified_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let activity = build_activity(&did, "abc");
        let mut message = build_message(activity, &jwk).await.unwrap();
        message.id = Some(ssi::vc::URI::try_from("a:b".to_string()).unwrap());
        verify_message(&mut message).await.unwrap_err();
    }

    // TODO: something isn't quite right yet
    // https://github.com/timothee-haudebourg/json-ld/pull/13
    // https://github.com/timothee-haudebourg/json-ld/pull/14
    // https://github.com/timothee-haudebourg/json-ld/issues/39
    // https://releases.rs/docs/unreleased/1.65.0/
    // #[tokio::test]
    #[allow(dead_code)]
    async fn builds_message_aribrary_data_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let mut note = Note::new();
        note.set_content("abc");

        let mut message = build_message(note.clone(), &jwk).await.unwrap();
        message
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
        verify_message(&mut message).await.unwrap_err();
    }
}
