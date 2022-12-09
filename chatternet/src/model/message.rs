use anyhow::{Error, Result};
use async_trait::async_trait;
use chrono::prelude::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{self, Map, Value};
use ssi::did::VerificationRelationship as ProofPurpose;
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::jwk::JWK;
use ssi::ldp::{now_ms, LinkedDataDocument};
use ssi::ldp::{Error as LdpError, Proof};
use ssi::rdf::DataSet;
use ssi::vc::URI;
use std::str::FromStr;

use crate::cid::{cid_from_json, uri_from_cid, CidVerifier};
use crate::didkey::{actor_id_from_did, did_from_actor_id};
use crate::new_context_loader;
use crate::proof::{build_proof, ProofVerifier};

use crate::CONTEXT_ACTIVITY_STREAMS;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ActivityType {
    Accept,
    Add,
    Announce,
    Arrive,
    Block,
    Create,
    Delete,
    Dislike,
    Flag,
    Follow,
    Ignore,
    Invite,
    Join,
    Leave,
    Like,
    Listen,
    Move,
    Offer,
    Question,
    Reject,
    Read,
    Remove,
    TentativeReject,
    TentativeAccept,
    Travel,
    Undo,
    Update,
    View,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MessageNoIdProof {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    #[serde(rename = "type")]
    pub message_type: ActivityType,
    pub actor: URI,
    pub object: Vec<URI>,
    pub published: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageNoId {
    pub proof: Proof,
    #[serde(flatten)]
    pub no_proof: MessageNoIdProof,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    pub id: URI,
    #[serde(flatten)]
    pub no_id: MessageNoId,
}

impl Message {
    pub async fn new(
        actor_did: &str,
        objects_id: &[impl AsRef<str>],
        activity_type: ActivityType,
        jwk: &JWK,
        members: Option<Map<String, Value>>,
    ) -> Result<Self> {
        let actor_id = URI::try_from(actor_id_from_did(actor_did)?)?;
        let objects_id: Result<Vec<URI>> = objects_id
            .iter()
            .map(|x| URI::from_str(x.as_ref()).map_err(Error::new))
            .collect();
        let objects_id = objects_id?;
        let published = now_ms();
        let message = MessageNoIdProof {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            message_type: activity_type,
            actor: actor_id,
            object: objects_id,
            published,
            members,
        };
        let proof = build_proof(&message, &jwk).await?;
        let message_with_proof = MessageNoId {
            proof,
            no_proof: message,
        };
        let id = uri_from_cid(
            cid_from_json(&message_with_proof, &mut new_context_loader(), None).await?,
        )
        .unwrap();
        Ok(Message {
            id,
            no_id: message_with_proof,
        })
    }

    pub async fn verify(&self) -> Result<()> {
        self.verify_cid().await?;
        self.no_id.verify_proof().await?;
        Ok(())
    }
}

#[async_trait]
impl LinkedDataDocument for MessageNoIdProof {
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

impl ProofVerifier<MessageNoIdProof> for MessageNoId {
    fn get_proof_issuer_did(&self) -> Result<String> {
        Ok(did_from_actor_id(self.no_proof.actor.as_str())?)
    }
    fn extract_proof(&self) -> Result<(&Proof, &MessageNoIdProof)> {
        Ok((&self.proof, &self.no_proof))
    }
}

impl CidVerifier<MessageNoId> for Message {
    fn extract_cid(&self) -> Result<(&URI, &MessageNoId)> {
        Ok((&self.id, &self.no_id))
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;
    use crate::didkey;
    use crate::didkey::did_from_jwk;

    #[tokio::test]
    async fn builds_and_verifies_message() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let message = Message::new(&did, &["id:a"], ActivityType::Create, &jwk, None)
            .await
            .unwrap();
        message.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_data() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let message = Message::new(&did, &["id:a"], ActivityType::Create, &jwk, None)
            .await
            .unwrap();
        let message_diff = Message::new(&did, &["id:a", "id:b"], ActivityType::Create, &jwk, None)
            .await
            .unwrap();
        message.verify().await.unwrap();
        let message = Message {
            id: message.id,
            no_id: MessageNoId {
                proof: message.no_id.proof,
                no_proof: message_diff.no_id.no_proof,
            },
        };
        message.verify().await.unwrap_err();
    }
}
