use anyhow::{Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use ssi::did::VerificationRelationship as ProofPurpose;
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::jwk::JWK;
use ssi::ldp::LinkedDataDocument;
use ssi::ldp::{Error as LdpError, Proof};
use ssi::rdf::DataSet;
use std::str::FromStr;

use crate::didkey::{actor_id_from_did, did_from_actor_id, did_from_jwk};
use crate::model::URI;
use crate::proof::{build_proof, get_proof_did, ProofVerifier};

use super::stringmax::StringMaxChars;
use super::AstreamContext;

type ActorName = StringMaxChars<30>;

/// ActivityStream actor types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ActorNoProof {
    #[serde(rename = "@context")]
    context: AstreamContext,
    id: URI,
    #[serde(rename = "type")]
    type_: ActorType,
    inbox: URI,
    outbox: URI,
    following: URI,
    followers: URI,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<ActorName>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ActorFields {
    proof: Proof,
    #[serde(flatten)]
    no_proof: ActorNoProof,
}

impl ActorFields {
    pub async fn new(jwk: &JWK, type_: ActorType, name: Option<String>) -> Result<Self> {
        let did = did_from_jwk(jwk)?;
        let actor_id = actor_id_from_did(&did)?;
        let id = URI::from_str(&actor_id)?;
        let inbox = URI::try_from(format!("{}/inbox", &actor_id))?;
        let outbox = URI::try_from(format!("{}/outbox", &actor_id))?;
        let following = URI::try_from(format!("{}/following", &actor_id))?;
        let followers = URI::try_from(format!("{}/followers", &actor_id))?;
        let actor = ActorNoProof {
            context: AstreamContext::new(),
            id,
            type_,
            inbox,
            outbox,
            following,
            followers,
            name: name.map(ActorName::try_from).transpose()?,
        };
        let proof = build_proof(&actor, &jwk).await?;
        Ok(ActorFields {
            proof,
            no_proof: actor,
        })
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

impl ProofVerifier<ActorNoProof> for ActorFields {
    fn get_proof_issuer_did(&self) -> Result<String> {
        Ok(did_from_actor_id(self.no_proof.id.as_str())?)
    }
    fn extract_proof(&self) -> Result<(&Proof, &ActorNoProof)> {
        Ok((&self.proof, &self.no_proof))
    }
}

#[async_trait]
pub trait Actor: ProofVerifier<ActorNoProof> {
    fn proof(&self) -> &Proof;
    fn context(&self) -> &AstreamContext;
    fn id(&self) -> &URI;
    fn type_(&self) -> ActorType;
    fn inbox(&self) -> &URI;
    fn outbox(&self) -> &URI;
    fn following(&self) -> &URI;
    fn followers(&self) -> &URI;
    fn name(&self) -> &Option<ActorName>;

    async fn verify(&self) -> Result<()> {
        let actor_id = self.id().as_str();
        let did = did_from_actor_id(actor_id)?;
        let proof_did =
            get_proof_did(&self.proof()).ok_or(Error::msg("actor proof doesn't match DID"))?;
        if &did != proof_did {
            Err(Error::msg("actor proof doesn't match DID"))?;
        }
        if self.inbox().as_str() != format!("{}/inbox", &actor_id) {
            Err(Error::msg("actor inbox URI is incorrect"))?;
        }
        if self.outbox().as_str() != format!("{}/outbox", &actor_id) {
            Err(Error::msg("actor outbox URI is incorrect"))?;
        }
        if self.following().as_str() != format!("{}/following", &actor_id) {
            Err(Error::msg("actor following URI is incorrect"))?;
        }
        if self.followers().as_str() != format!("{}/followers", &actor_id) {
            Err(Error::msg("actor followers URI is incorrect"))?;
        }
        self.verify_proof().await?;
        Ok(())
    }
}

impl Actor for ActorFields {
    fn context(&self) -> &AstreamContext {
        &self.no_proof.context
    }
    fn id(&self) -> &URI {
        &self.no_proof.id
    }
    fn type_(&self) -> ActorType {
        self.no_proof.type_
    }
    fn inbox(&self) -> &URI {
        &self.no_proof.inbox
    }
    fn outbox(&self) -> &URI {
        &self.no_proof.outbox
    }
    fn following(&self) -> &URI {
        &&self.no_proof.following
    }
    fn followers(&self) -> &URI {
        &self.no_proof.followers
    }
    fn name(&self) -> &Option<ActorName> {
        &self.no_proof.name
    }
    fn proof(&self) -> &Proof {
        &self.proof
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;
    use crate::didkey;

    #[tokio::test]
    async fn builds_and_verifies_actor() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let actor = ActorFields::new(&jwk, ActorType::Person, None)
            .await
            .unwrap();
        actor.verify().await.unwrap();
        let actor = ActorFields::new(
            &jwk,
            ActorType::Person,
            Some(std::iter::repeat("a").take(30).collect::<String>()),
        )
        .await
        .unwrap();
        actor.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_name_too_long() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        ActorFields::new(
            &jwk,
            ActorType::Person,
            Some(std::iter::repeat("a").take(31).collect::<String>()),
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn doesnt_verify_invalid_uris() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let actor = ActorFields::new(&jwk, ActorType::Person, None)
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
        let name = "abc".to_string();
        let actor = ActorFields::new(&jwk, ActorType::Person, Some(name))
            .await
            .unwrap();
        actor.verify().await.unwrap();
        let name = ActorName::from_str("abcd").unwrap();
        let actor = ActorFields {
            proof: actor.proof,
            no_proof: ActorNoProof {
                name: Some(name),
                ..actor.no_proof
            },
        };
        actor.verify().await.unwrap_err();
    }
}
