use anyhow::Result;
use async_trait::async_trait;
use chrono::prelude::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use ssi::did::VerificationRelationship as ProofPurpose;
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::jwk::JWK;
use ssi::ldp::{now_ms, LinkedDataDocument};
use ssi::ldp::{Error as LdpError, Proof};
use ssi::rdf::DataSet;

use crate::cid::{cid_from_json, uri_from_cid, CidVerifier};
use crate::didkey::{actor_id_from_did, did_from_actor_id, did_from_jwk};
use crate::model::URI;
use crate::new_context_loader;
use crate::proof::{build_proof, ProofVerifier};

use super::vecmax::VecMax;
use super::CtxSigStream;

const MAX_URIS: usize = 256;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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
    context: CtxSigStream,
    #[serde(rename = "type")]
    type_: ActivityType,
    actor: URI,
    object: VecMax<URI, MAX_URIS>,
    published: DateTime<Utc>,
    to: Option<VecMax<URI, MAX_URIS>>,
    cc: Option<VecMax<URI, MAX_URIS>>,
    audience: Option<VecMax<URI, MAX_URIS>>,
    origin: Option<URI>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageNoId {
    proof: Proof,
    #[serde(flatten)]
    no_proof: MessageNoIdProof,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageFields {
    id: URI,
    #[serde(flatten)]
    no_id: MessageNoId,
}

impl MessageFields {
    pub async fn new(
        jwk: &JWK,
        type_: ActivityType,
        objects_id: Vec<URI>,
        to: Option<Vec<URI>>,
        cc: Option<Vec<URI>>,
        audience: Option<Vec<URI>>,
        origin: Option<URI>,
    ) -> Result<Self> {
        let did = did_from_jwk(jwk)?;
        let actor_id = URI::try_from(actor_id_from_did(&did)?)?;
        let published = now_ms();
        let message = MessageNoIdProof {
            context: CtxSigStream::new(),
            type_,
            actor: actor_id,
            object: VecMax::<_, MAX_URIS>::try_from(objects_id)?,
            published,
            to: to.map(VecMax::<_, MAX_URIS>::try_from).transpose()?,
            cc: cc.map(VecMax::<_, MAX_URIS>::try_from).transpose()?,
            audience: audience.map(VecMax::<_, MAX_URIS>::try_from).transpose()?,
            origin,
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
        Ok(MessageFields {
            id,
            no_id: message_with_proof,
        })
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

impl ProofVerifier<MessageNoIdProof> for MessageFields {
    fn get_proof_issuer_did(&self) -> Result<String> {
        Ok(did_from_actor_id(self.no_id.no_proof.actor.as_str())?)
    }
    fn extract_proof(&self) -> Result<(&Proof, &MessageNoIdProof)> {
        Ok((&self.no_id.proof, &self.no_id.no_proof))
    }
}

impl CidVerifier<MessageNoId> for MessageFields {
    fn extract_cid(&self) -> Result<(&URI, &MessageNoId)> {
        Ok((&self.id, &self.no_id))
    }
}

#[async_trait]
pub trait Message: CidVerifier<MessageNoId> + ProofVerifier<MessageNoIdProof> {
    fn id(&self) -> &URI;
    fn proof(&self) -> &Proof;
    fn context(&self) -> &CtxSigStream;
    fn type_(&self) -> ActivityType;
    fn actor(&self) -> &URI;
    fn object(&self) -> &VecMax<URI, MAX_URIS>;
    fn published(&self) -> &DateTime<Utc>;
    fn to(&self) -> &Option<VecMax<URI, MAX_URIS>>;
    fn cc(&self) -> &Option<VecMax<URI, MAX_URIS>>;
    fn audience(&self) -> &Option<VecMax<URI, MAX_URIS>>;

    async fn verify(&self) -> Result<()> {
        self.verify_cid().await?;
        self.verify_proof().await?;
        Ok(())
    }
}

impl Message for MessageFields {
    fn id(&self) -> &URI {
        &self.id
    }
    fn proof(&self) -> &Proof {
        &self.no_id.proof
    }
    fn context(&self) -> &CtxSigStream {
        &self.no_id.no_proof.context
    }
    fn type_(&self) -> ActivityType {
        self.no_id.no_proof.type_
    }
    fn actor(&self) -> &URI {
        &self.no_id.no_proof.actor
    }
    fn object(&self) -> &VecMax<URI, MAX_URIS> {
        &self.no_id.no_proof.object
    }
    fn published(&self) -> &DateTime<Utc> {
        &self.no_id.no_proof.published
    }
    fn to(&self) -> &Option<VecMax<URI, MAX_URIS>> {
        &self.no_id.no_proof.to
    }
    fn cc(&self) -> &Option<VecMax<URI, MAX_URIS>> {
        &self.no_id.no_proof.cc
    }
    fn audience(&self) -> &Option<VecMax<URI, MAX_URIS>> {
        &self.no_id.no_proof.audience
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use tokio;

    use super::*;
    use crate::didkey;

    #[tokio::test]
    async fn builds_and_verifies_message() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let message = MessageFields::new(
            &jwk,
            ActivityType::Create,
            vec![URI::from_str("id:a").unwrap()],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        message.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_data() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let message = MessageFields::new(
            &jwk,
            ActivityType::Create,
            vec![URI::from_str("id:a").unwrap()],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let message_diff = MessageFields::new(
            &jwk,
            ActivityType::Create,
            vec![
                URI::from_str("id:a").unwrap(),
                URI::from_str("id:b").unwrap(),
            ],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        message.verify().await.unwrap();
        let message = MessageFields {
            id: message.id,
            no_id: MessageNoId {
                proof: message.no_id.proof,
                no_proof: message_diff.no_id.no_proof,
            },
        };
        message.verify().await.unwrap_err();
    }
}
