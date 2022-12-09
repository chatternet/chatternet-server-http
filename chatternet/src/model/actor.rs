use anyhow::{Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{self, Map, Value};
use ssi::did::VerificationRelationship as ProofPurpose;
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::jwk::JWK;
use ssi::ldp::LinkedDataDocument;
use ssi::ldp::{Error as LdpError, Proof};
use ssi::rdf::DataSet;
use ssi::vc::URI;
use std::str::FromStr;

use crate::didkey::{actor_id_from_did, did_from_actor_id};
use crate::proof::{build_proof, get_proof_did, ProofVerifier};
use crate::CONTEXT_ACTIVITY_STREAMS;

/// ActivityStream actor types.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ActorType {
    Application,
    Group,
    Organization,
    Person,
    Service,
}

/// The core Actor model type.
///
/// The ChatterNet Actor class requires a proof. It is stored separately so
/// that the two sets of data can be build and used indepedently.
///
/// The `members` field can contain arbitrary data.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ActorNoProof {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: URI,
    #[serde(rename = "type")]
    pub type_: ActorType,
    pub inbox: URI,
    pub outbox: URI,
    pub following: URI,
    pub followers: URI,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Actor {
    pub proof: Proof,
    #[serde(flatten)]
    pub no_proof: ActorNoProof,
}

impl Actor {
    pub async fn new(
        did: String,
        type_: ActorType,
        jwk: &JWK,
        members: Option<Map<String, Value>>,
    ) -> Result<Self> {
        let actor_id = actor_id_from_did(&did)?;
        let id = URI::from_str(&actor_id)?;
        let inbox = URI::try_from(format!("{}/inbox", &actor_id))?;
        let outbox = URI::try_from(format!("{}/outbox", &actor_id))?;
        let following = URI::try_from(format!("{}/following", &actor_id))?;
        let followers = URI::try_from(format!("{}/followers", &actor_id))?;
        let actor = ActorNoProof {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            id,
            type_,
            inbox,
            outbox,
            following,
            followers,
            members,
        };
        let proof = build_proof(&actor, &jwk).await?;
        Ok(Actor {
            proof,
            no_proof: actor,
        })
    }

    pub async fn verify(&self) -> Result<()> {
        let actor_id = self.no_proof.id.as_str();
        let did = did_from_actor_id(actor_id)?;
        let proof_did =
            get_proof_did(&self.proof).ok_or(Error::msg("actor proof doesn't match DID"))?;
        if &did != proof_did {
            Err(Error::msg("actor proof doesn't match DID"))?;
        }
        if self.no_proof.inbox.as_str() != format!("{}/inbox", &actor_id) {
            Err(Error::msg("actor inbox URI is incorrect"))?;
        }
        if self.no_proof.outbox.as_str() != format!("{}/outbox", &actor_id) {
            Err(Error::msg("actor outbox URI is incorrect"))?;
        }
        if self.no_proof.following.as_str() != format!("{}/following", &actor_id) {
            Err(Error::msg("actor following URI is incorrect"))?;
        }
        if self.no_proof.followers.as_str() != format!("{}/followers", &actor_id) {
            Err(Error::msg("actor followers URI is incorrect"))?;
        }
        self.verify_proof().await?;
        Ok(())
    }
}

#[async_trait]
impl LinkedDataDocument for ActorNoProof {
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

impl ProofVerifier<ActorNoProof> for Actor {
    fn get_proof_issuer_did(&self) -> Result<String> {
        Ok(did_from_actor_id(self.no_proof.id.as_str())?)
    }
    fn extract_proof(&self) -> Result<(&Proof, &ActorNoProof)> {
        Ok((&self.proof, &self.no_proof))
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use tokio;

    use super::*;
    use crate::didkey;
    use crate::didkey::did_from_jwk;

    #[tokio::test]
    async fn builds_and_verifies_actor() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let actor = Actor::new(did, ActorType::Person, &jwk, None)
            .await
            .unwrap();
        actor.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_invalid_uris() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let actor = Actor::new(did, ActorType::Person, &jwk, None)
            .await
            .unwrap();
        let mut invalid = actor.clone();
        invalid.no_proof.id = URI::from_str("a:b").unwrap();
        invalid.verify().await.unwrap_err();
        let mut invalid = actor.clone();
        invalid.no_proof.inbox = URI::from_str("a:b").unwrap();
        invalid.verify().await.unwrap_err();
        let mut invalid = actor.clone();
        invalid.no_proof.outbox = URI::from_str("a:b").unwrap();
        invalid.verify().await.unwrap_err();
        let mut invalid = actor.clone();
        invalid.no_proof.following = URI::from_str("a:b").unwrap();
        invalid.verify().await.unwrap_err();
        let mut invalid = actor.clone();
        invalid.no_proof.followers = URI::from_str("a:b").unwrap();
        invalid.verify().await.unwrap_err();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_data() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let actor = Actor::new(did, ActorType::Person, &jwk, Some(members))
            .await
            .unwrap();
        actor.verify().await.unwrap();
        let members = json!({"name": "abcd"}).as_object().unwrap().to_owned();
        let actor = Actor {
            proof: actor.proof,
            no_proof: ActorNoProof {
                members: Some(members),
                ..actor.no_proof
            },
        };
        actor.verify().await.unwrap_err();
    }
}
