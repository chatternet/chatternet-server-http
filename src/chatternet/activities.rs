use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use did_method_key::DIDKey;
use serde::{Deserialize, Serialize};
use serde_json::{self, Map, Value};
use ssi::did::VerificationRelationship as ProofPurpose;
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::jwk::JWK;
use ssi::ldp::{Error as LdpError, Proof};
use ssi::ldp::{LinkedDataDocument, LinkedDataProofs};
use ssi::rdf::DataSet;
use ssi::vc::{LinkedDataProofOptions, URI};
use ssi::{did_resolve, urdna2015};
use std::str::FromStr;

use super::ldcontexts;

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
    more_contexts: Option<&String>,
) -> Result<Cid> {
    let json = serde_json::to_string(object)?;
    let dataset = json_to_dataset(&json, more_contexts, false, None, context_loader).await?;
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

pub fn actor_id_from_did(did: &str) -> Result<String> {
    if !did.starts_with("did:") {
        Err(anyhow!("DID has invalid prefix"))?;
    }
    Ok(format!("{}/actor", did))
}

pub fn did_from_actor_id(actor_id: &str) -> Result<String> {
    let (did, path) = actor_id
        .split_once("/")
        .ok_or(anyhow!("actor ID is not a did and path"))?;
    if !did.starts_with("did:") {
        Err(anyhow!("actor ID is not a DID"))?;
    }
    if path != "actor" {
        Err(anyhow!("actor ID path is not an actor"))?;
    }
    Ok(did.to_string())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: URI,
    pub inbox: URI,
    pub outbox: URI,
    pub following: URI,
    pub followers: URI,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
}

impl Actor {
    pub fn new(
        did: String,
        name: Option<String>,
        members: Option<Map<String, Value>>,
    ) -> Result<Self> {
        let actor_id = actor_id_from_did(&did)?;
        let id = URI::from_str(&actor_id)?;
        let inbox = URI::try_from(format!("{}/inbox", &actor_id))?;
        let outbox = URI::try_from(format!("{}/outbox", &actor_id))?;
        let following = URI::try_from(format!("{}/following", &actor_id))?;
        let followers = URI::try_from(format!("{}/followers", &actor_id))?;
        Ok(Actor {
            context: vec![ldcontexts::ACTIVITY_STREAMS_URI.to_string()],
            id,
            inbox,
            outbox,
            following,
            followers,
            name,
            members,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MessageType {
    Create,
    Follow,
    Add,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<URI>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub actor: URI,
    pub object: URI,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "https://w3id.org/security#proof")]
    pub proof: Option<Proof>,
}

async fn build_proof(
    message: &(impl LinkedDataDocument + Sync),
    did: &str,
    jwk: &JWK,
) -> Result<Proof> {
    let mut options = LinkedDataProofOptions::default();
    let proof_purpose = ProofPurpose::AssertionMethod;
    let verification_methods =
        did_resolve::get_verification_methods(did, proof_purpose, &DIDKey).await?;
    let verification_method = verification_methods
        .keys()
        .next()
        .ok_or(anyhow!("actor has no verification method"))?;
    let verification_method = URI::from_str(verification_method)?;
    options.type_ = Some("Ed25519Signature2020".to_string());
    options.verification_method = Some(verification_method);
    Ok(LinkedDataProofs::sign(
        message,
        &options,
        &DIDKey,
        &mut new_context_loader(),
        &jwk,
        None,
    )
    .await?)
}

impl Message {
    pub async fn new(
        actor_did: &str,
        object_id: &str,
        message_type: MessageType,
        members: Option<Map<String, Value>>,
        jwk: &JWK,
    ) -> Result<Self> {
        let actor_id = URI::try_from(actor_id_from_did(actor_did)?)?;
        let object_id = URI::from_str(object_id)?;
        // build an ID which is isomorphic to the subject object such that new
        // messages cannot override old ones
        let mut message = Message {
            context: vec![ldcontexts::ACTIVITY_STREAMS_URI.to_string()],
            id: None,
            message_type: message_type,
            actor: actor_id,
            object: object_id,
            members,
            proof: None,
        };
        message.id = Some(
            URI::try_from(cid_to_urn(
                cid_from_json(&message, &mut new_context_loader(), None).await?,
            ))
            .unwrap(),
        );
        message.proof = Some(build_proof(&message, &actor_did, jwk).await?);
        Ok(message)
    }

    pub async fn verify(&self) -> Result<String> {
        let mut message = self.clone();
        let proof_purpose = message
            .get_default_proof_purpose()
            .ok_or(anyhow!("message has no proof purpose"))?;
        let actor_did = did_from_actor_id(message.actor.as_str())?;
        let proof = message
            .proof
            .take()
            .ok_or(anyhow!("message does not contain a proof"))?;
        let verification_methods =
            did_resolve::get_verification_methods(&actor_did, proof_purpose, &DIDKey).await?;
        match &proof.verification_method {
            Some(verification_method) => {
                if !verification_methods.contains_key(verification_method) {
                    return Err(anyhow!("message proof cannot be verified by actor"));
                }
            }
            None => {
                return Err(anyhow!("message proof cannot be verified"));
            }
        };
        LinkedDataProofs::verify(&proof, &message, &DIDKey, &mut new_context_loader()).await?;
        let id = message
            .id
            .take()
            .ok_or(anyhow!("message does not contain an ID"))?;
        if id
            != URI::try_from(cid_to_urn(
                cid_from_json(&message, &mut new_context_loader(), None).await?,
            ))
            .unwrap()
        {
            return Err(anyhow!("message ID does not match its contents"));
        }
        message.id = Some(id.clone());
        message.proof = Some(proof);
        Ok(id.to_string())
    }

    pub fn set(&mut self, properties: &Value) -> Result<()> {
        let map: Map<String, Value> = properties
            .as_object()
            .ok_or(anyhow!("properties cannot be inserted into message"))?
            .to_owned()
            .into();
        self.members = Some(map);
        Ok(())
    }
}

#[async_trait]
impl LinkedDataDocument for Message {
    fn get_contexts(&self) -> Result<Option<String>, LdpError> {
        Ok(serde_json::to_string(&self.context).ok())
    }

    async fn to_dataset_for_signing(
        &self,
        parent: Option<&(dyn LinkedDataDocument + Sync)>,
        context_loader: &mut ContextLoader,
    ) -> Result<DataSet, LdpError> {
        let json = serde_json::to_string(&self)?;
        let more_contexts = match parent {
            Some(parent) => parent.get_contexts()?,
            None => None,
        };
        Ok(json_to_dataset(&json, more_contexts.as_ref(), false, None, context_loader).await?)
    }

    fn to_value(&self) -> Result<Value, LdpError> {
        Ok(serde_json::to_value(&self)?)
    }

    fn get_default_proof_purpose(&self) -> Option<ProofPurpose> {
        Some(ProofPurpose::AssertionMethod)
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use tokio;

    use crate::chatternet::didkey;

    use super::*;

    #[tokio::test]
    async fn builds_cid_from_object() {
        let activity_1 = json!({
            "@context": [ldcontexts::ACTIVITY_STREAMS_URI],
            "content": "abc",
        });
        let activity_2 = json!({
            "content": "abcd",
        });
        let cid_1 = cid_from_json(&activity_1, &mut new_context_loader(), None)
            .await
            .unwrap();
        let more_contexts = serde_json::to_string(&[ldcontexts::ACTIVITY_STREAMS_URI]).unwrap();
        let cid_2 = cid_from_json(&activity_2, &mut new_context_loader(), Some(&more_contexts))
            .await
            .unwrap();
        assert_ne!(cid_1.to_string(), cid_2.to_string());
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

    #[tokio::test]
    async fn builds_message_and_verifies() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = Message::new(&did, "id:a", MessageType::Create, None, &jwk)
            .await
            .unwrap();
        message.verify().await.unwrap();
    }

    #[tokio::test]
    async fn builds_message_wrong_actor_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = Message::new(&did, "id:a", MessageType::Create, None, &jwk)
            .await
            .unwrap();
        let jwk_2 = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did_2 = didkey::did_from_jwk(&jwk_2).unwrap();
        message.proof = Some(build_proof(&message, &did_2, &jwk_2).await.unwrap());
        message.verify().await.unwrap_err();
    }

    #[tokio::test]
    async fn builds_message_wrong_id_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = Message::new(&did, "id:a", MessageType::Create, None, &jwk)
            .await
            .unwrap();
        message.id = Some(URI::from_str("a:b").unwrap());
        message.proof = Some(build_proof(&message, &did, &jwk).await.unwrap());
        message.verify().await.unwrap_err();
    }

    #[tokio::test]
    async fn builds_message_modified_content_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = Message::new(&did, "id:a", MessageType::Create, None, &jwk)
            .await
            .unwrap();
        message.object = URI::from_str("id:b").unwrap();
        message.verify().await.unwrap_err();
    }

    // TODO: something isn't quite right yet
    // https://github.com/timothee-haudebourg/json-ld/pull/13
    // https://github.com/timothee-haudebourg/json-ld/pull/14
    // https://github.com/timothee-haudebourg/json-ld/issues/39
    // #[tokio::test]
    #[allow(dead_code)]
    async fn builds_message_aribtrary_data_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = Message::new(&did, "id:a", MessageType::Create, None, &jwk)
            .await
            .unwrap();
        message.members = Some(
            json!({"does not exist": "abc"})
                .as_object()
                .unwrap()
                .to_owned(),
        );
        message.verify().await.unwrap_err();
    }
}
