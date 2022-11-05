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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MessageActor {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: String,
    pub following: String,
    pub followers: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
}

impl MessageActor {
    pub fn new(did: String, name: Option<String>, members: Option<Map<String, Value>>) -> Self {
        let following = format!("{}/following", did);
        let followers = format!("{}/followers", did);
        MessageActor {
            context: vec![ldcontexts::ACTIVITY_STREAMS_URI.to_string()],
            id: did,
            following,
            followers,
            name,
            members,
        }
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
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub actor: MessageActor,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
        actor: MessageActor,
        message_type: MessageType,
        members: Option<Map<String, Value>>,
        jwk: &JWK,
    ) -> Result<Self> {
        // build an ID which is isomorphic to the subject object such that new
        // messages cannot override old ones
        let actor_did = &actor.id.to_string();
        let mut message = Message {
            context: vec![
                ldcontexts::ACTIVITY_STREAMS_URI.to_string(),
                "https://www.w3.org/2018/credentials/v1".to_string(),
            ],
            id: None,
            message_type: message_type,
            actor,
            members,
            proof: None,
        };
        message.id = Some(cid_to_urn(
            cid_from_json(&message, &mut new_context_loader(), None).await?,
        ));
        message.proof = Some(build_proof(&message, &actor_did, jwk).await?);
        Ok(message)
    }

    pub async fn verify(&self) -> Result<String> {
        let mut message = self.clone();
        let proof_purpose = message
            .get_default_proof_purpose()
            .ok_or(anyhow!("message has no proof purpose"))?;
        let did = &message.actor.id;
        let proof = message
            .proof
            .take()
            .ok_or(anyhow!("message does not contain a proof"))?;
        let verification_methods =
            did_resolve::get_verification_methods(&did, proof_purpose, &DIDKey).await?;
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
        if id != cid_to_urn(cid_from_json(&message, &mut new_context_loader(), None).await?) {
            return Err(anyhow!("message ID does not match its contents"));
        }
        message.id = Some(id.clone());
        message.proof = Some(proof);
        Ok(id)
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

    #[tokio::test]
    async fn builds_message_and_verifies() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let members = json!({
            "object": {
                "type": "Note",
                "content": "abc"
            }
        })
        .as_object()
        .unwrap()
        .to_owned();
        let actor = MessageActor::new(did, None, None);
        let message = Message::new(actor, MessageType::Create, Some(members), &jwk)
            .await
            .unwrap();
        message.verify().await.unwrap();
    }

    #[tokio::test]
    async fn builds_message_wrong_actor_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let members = json!({
            "object": {
                "type": "Note",
                "content": "abc"
            }
        })
        .as_object()
        .unwrap()
        .to_owned();
        let actor = MessageActor::new(did, None, None);
        let mut message = Message::new(actor, MessageType::Create, Some(members), &jwk)
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
        let members = json!({
            "object": {
                "type": "Note",
                "content": "abc"
            }
        })
        .as_object()
        .unwrap()
        .to_owned();
        let actor = MessageActor::new(did.clone(), None, None);
        let mut message = Message::new(actor, MessageType::Create, Some(members), &jwk)
            .await
            .unwrap();
        message.id = Some("a:b".to_string());
        message.proof = Some(build_proof(&message, &did, &jwk).await.unwrap());
        message.verify().await.unwrap_err();
    }

    #[tokio::test]
    async fn builds_message_modified_content_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let members = json!({
            "object": {
                "type": "Note",
                "content": "abc"
            }
        })
        .as_object()
        .unwrap()
        .to_owned();
        let actor = MessageActor::new(did, None, None);
        let mut message = Message::new(actor, MessageType::Create, Some(members), &jwk)
            .await
            .unwrap();
        message
            .members
            .as_mut()
            .unwrap()
            .get_mut("object")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("content".to_string(), serde_json::to_value("abcd").unwrap())
            .unwrap();
        message.verify().await.unwrap_err();
    }

    // TODO: something isn't quite right yet
    // https://github.com/timothee-haudebourg/json-ld/pull/13
    // https://github.com/timothee-haudebourg/json-ld/pull/14
    // https://github.com/timothee-haudebourg/json-ld/issues/39
    // https://releases.rs/docs/unreleased/1.65.0/
    // #[tokio::test]
    #[allow(dead_code)]
    async fn builds_message_aribtrary_data_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let members = json!({
            "object": {
                "type": "Note",
                "content": "abc"
            }
        })
        .as_object()
        .unwrap()
        .to_owned();
        let actor = MessageActor::new(did, None, None);
        let mut message = Message::new(actor, MessageType::Create, Some(members), &jwk)
            .await
            .unwrap();
        message.members.as_mut().unwrap().insert(
            "does not exit".to_string(),
            serde_json::to_value("abc").unwrap(),
        );
        message.verify().await.unwrap_err();
    }
}
