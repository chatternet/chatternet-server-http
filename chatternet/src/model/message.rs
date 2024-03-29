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
use crate::model::Uri;
use crate::new_context_loader;
use crate::proof::{build_proof, ProofVerifier};

use super::vecmax::VecMax;
use super::CtxSigStream;

pub type VecUris = VecMax<Uri, 256>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ActivityType {
    Add,
    Create,
    Delete,
    Remove,
    View,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MessageNoIdProof {
    #[serde(rename = "@context")]
    context: CtxSigStream,
    #[serde(rename = "type")]
    type_: ActivityType,
    actor: Uri,
    object: VecUris,
    published: DateTime<Utc>,
    to: Option<VecUris>,
    origin: Option<VecUris>,
    target: Option<VecUris>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageNoId {
    proof: Proof,
    #[serde(flatten)]
    no_proof: MessageNoIdProof,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageFields {
    id: Uri,
    #[serde(flatten)]
    no_id: MessageNoId,
}

pub struct MessageBuilder<'a> {
    jwk: &'a JWK,
    type_: ActivityType,
    object: VecUris,
    to: Option<VecUris>,
    origin: Option<VecUris>,
    target: Option<VecUris>,
}

impl<'a> MessageBuilder<'a> {
    pub fn new(jwk: &'a JWK, type_: ActivityType, object: VecUris) -> Self {
        Self {
            jwk,
            type_,
            object,
            to: None,
            origin: None,
            target: None,
        }
    }

    pub fn to(mut self, to: VecUris) -> Self {
        self.to = Some(to);
        self
    }

    pub fn origin(mut self, origin: VecUris) -> Self {
        self.origin = Some(origin);
        self
    }

    pub fn target(mut self, target: VecUris) -> Self {
        self.target = Some(target);
        self
    }

    pub async fn build(self) -> Result<MessageFields> {
        MessageFields::new(
            self.jwk,
            self.type_,
            self.object,
            self.to,
            self.origin,
            self.target,
        )
        .await
    }
}

impl MessageFields {
    pub async fn new(
        jwk: &JWK,
        type_: ActivityType,
        object: VecUris,
        to: Option<VecUris>,
        origin: Option<VecUris>,
        target: Option<VecUris>,
    ) -> Result<Self> {
        let did = did_from_jwk(jwk)?;
        let actor_id = Uri::try_from(actor_id_from_did(&did)?)?;
        let published = now_ms();
        let message = MessageNoIdProof {
            context: CtxSigStream::new(),
            type_,
            actor: actor_id,
            object,
            published,
            to,
            origin,
            target,
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
    fn extract_cid(&self) -> Result<(&Uri, &MessageNoId)> {
        Ok((&self.id, &self.no_id))
    }
}

#[async_trait]
pub trait Message: CidVerifier<MessageNoId> + ProofVerifier<MessageNoIdProof> {
    fn id(&self) -> &Uri;
    fn proof(&self) -> &Proof;
    fn context(&self) -> &CtxSigStream;
    fn type_(&self) -> ActivityType;
    fn actor(&self) -> &Uri;
    fn object(&self) -> &VecUris;
    fn published(&self) -> &DateTime<Utc>;
    fn to(&self) -> &Option<VecUris>;
    fn origin(&self) -> &Option<VecUris>;
    fn target(&self) -> &Option<VecUris>;

    async fn verify(&self) -> Result<()> {
        self.verify_cid().await?;
        self.verify_proof().await?;
        Ok(())
    }
}

impl Message for MessageFields {
    fn id(&self) -> &Uri {
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
    fn actor(&self) -> &Uri {
        &self.no_id.no_proof.actor
    }
    fn object(&self) -> &VecUris {
        &self.no_id.no_proof.object
    }
    fn published(&self) -> &DateTime<Utc> {
        &self.no_id.no_proof.published
    }
    fn to(&self) -> &Option<VecUris> {
        &self.no_id.no_proof.to
    }
    fn origin(&self) -> &Option<VecUris> {
        &self.no_id.no_proof.origin
    }
    fn target(&self) -> &Option<VecUris> {
        &self.no_id.no_proof.target
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;
    use crate::didkey;

    #[tokio::test]
    async fn builds_and_verifies_message() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let message = MessageBuilder::new(
            &jwk,
            ActivityType::Create,
            vec!["id:a".try_into().unwrap()].try_into().unwrap(),
        )
        .build()
        .await
        .unwrap();
        message.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_data() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let message = MessageBuilder::new(
            &jwk,
            ActivityType::Create,
            vec!["id:a".try_into().unwrap()].try_into().unwrap(),
        )
        .build()
        .await
        .unwrap();
        let message_diff = MessageBuilder::new(
            &jwk,
            ActivityType::Create,
            vec!["id:a".try_into().unwrap(), "id:b".try_into().unwrap()]
                .try_into()
                .unwrap(),
        )
        .build()
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
