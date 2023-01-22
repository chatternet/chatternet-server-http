use anyhow::{Error, Result};
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
use std::str::FromStr;

use crate::didkey::{actor_id_from_did, did_from_actor_id, did_from_jwk};
use crate::model::Uri;
use crate::proof::{build_proof, get_proof_did, ProofVerifier};

use super::document::Document;
use super::stringmax::StringMaxChars;
use super::CtxSigStream;

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
    context: CtxSigStream,
    id: Uri,
    #[serde(rename = "type")]
    type_: ActorType,
    published: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<ActorName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<Uri>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ActorFields {
    proof: Proof,
    #[serde(flatten)]
    no_proof: ActorNoProof,
}

impl ActorFields {
    pub async fn new(
        jwk: &JWK,
        type_: ActorType,
        name: Option<String>,
        url: Option<String>,
    ) -> Result<Self> {
        let did = did_from_jwk(jwk)?;
        let actor_id = actor_id_from_did(&did)?;
        let id = Uri::from_str(&actor_id)?;
        let published = now_ms();
        let actor = ActorNoProof {
            context: CtxSigStream::new(),
            id,
            type_,
            published,
            name: name.map(ActorName::try_from).transpose()?,
            url: url.map(Uri::try_from).transpose()?,
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
    fn context(&self) -> &CtxSigStream;
    fn type_(&self) -> ActorType;
    fn published(&self) -> &DateTime<Utc>;
    fn name(&self) -> &Option<ActorName>;
    fn url(&self) -> &Option<Uri>;
}

#[async_trait]
impl Document for ActorFields {
    fn id(&self) -> &Uri {
        &self.no_proof.id
    }
    async fn verify(&self) -> Result<()> {
        let actor_id = self.id().as_str();
        let did = did_from_actor_id(actor_id)?;
        let proof_did =
            get_proof_did(&self.proof()).ok_or(Error::msg("actor proof doesn't match DID"))?;
        if &did != proof_did {
            Err(Error::msg("actor proof doesn't match DID"))?;
        }
        self.verify_proof().await?;
        Ok(())
    }
}

impl Actor for ActorFields {
    fn context(&self) -> &CtxSigStream {
        &self.no_proof.context
    }
    fn type_(&self) -> ActorType {
        self.no_proof.type_
    }
    fn published(&self) -> &DateTime<Utc> {
        &self.no_proof.published
    }
    fn name(&self) -> &Option<ActorName> {
        &self.no_proof.name
    }
    fn url(&self) -> &Option<Uri> {
        &self.no_proof.url
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
        let actor = ActorFields::new(&jwk, ActorType::Person, None, None)
            .await
            .unwrap();
        actor.verify().await.unwrap();
        let actor = ActorFields::new(
            &jwk,
            ActorType::Person,
            Some(std::iter::repeat("a").take(30).collect::<String>()),
            None,
        )
        .await
        .unwrap();
        actor.verify().await.unwrap();
        let actor = ActorFields::new(
            &jwk,
            ActorType::Person,
            None,
            Some("https://abc.example".to_string()),
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
            None,
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_data() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let name = "abc".to_string();
        let actor = ActorFields::new(&jwk, ActorType::Person, Some(name), None)
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
