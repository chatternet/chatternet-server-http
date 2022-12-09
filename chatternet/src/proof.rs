use anyhow::{Error, Result};
use async_trait::async_trait;
use did_method_key::DIDKey;
use ssi::did::VerificationRelationship as ProofPurpose;
use ssi::did_resolve;
use ssi::jwk::JWK;
use ssi::ldp::Proof;
use ssi::ldp::{LinkedDataDocument, LinkedDataProofs};
use ssi::vc::{LinkedDataProofOptions, URI};
use std::str::FromStr;

use crate::didkey::did_from_jwk;
use crate::new_context_loader;

use std::fmt::Debug;

/// Extract the DID from a `Proof` object.
///
/// Returns `None` if the `Proof` is not valid is is not from a DID Key.
pub fn get_proof_did(proof: &Proof) -> Option<&str> {
    proof.verification_method.as_ref()?.split('#').next()
}

/// Build the `Proof` for a `LinkedDataDocument`.
pub async fn build_proof(
    document: &(impl LinkedDataDocument + Sync + Debug),
    jwk: &JWK,
) -> Result<Proof> {
    let did = did_from_jwk(jwk)?;
    let mut options = LinkedDataProofOptions::default();
    let proof_purpose = ProofPurpose::AssertionMethod;
    let verification_methods =
        did_resolve::get_verification_methods(&did, proof_purpose, &DIDKey).await?;
    let verification_method = verification_methods
        .keys()
        .next()
        .ok_or(Error::msg("document has no verification method"))?;
    let verification_method = URI::from_str(verification_method)?;
    options.type_ = Some("Ed25519Signature2020".to_string());
    options.verification_method = Some(verification_method);
    Ok(LinkedDataProofs::sign(
        document,
        &options,
        &DIDKey,
        &mut new_context_loader(),
        &jwk,
        None,
    )
    .await?)
}

/// An object whith some verifiable content and associated proof.
#[async_trait]
pub trait ProofVerifier<WithoutProof: LinkedDataDocument + Sync> {
    /// Get the DID of the proof issuer.
    fn get_proof_issuer_did(&self) -> Result<String>;

    /// Return a Proof and the `LinkedDataDocument` object without associated
    /// with that proof.
    fn extract_proof(&self) -> Result<(&Proof, &WithoutProof)>;

    /// Verify the object's proof.
    async fn verify_proof(&self) -> Result<()> {
        let issuer = self.get_proof_issuer_did()?.as_str().to_owned();

        let (proof, without_proof) = self.extract_proof()?;

        let proof_purpose = without_proof
            .get_default_proof_purpose()
            .ok_or(Error::msg("proof has no purpose"))?;

        let verification_methods =
            did_resolve::get_verification_methods(&issuer, proof_purpose, &DIDKey).await?;
        match &proof.verification_method {
            Some(verification_method) => {
                if !verification_methods.contains_key(verification_method) {
                    return Err(Error::msg("proof cannot be verified by issuer"));
                }
            }
            None => {
                return Err(Error::msg("proof cannot be verified"));
            }
        };

        LinkedDataProofs::verify(proof, without_proof, &DIDKey, &mut new_context_loader()).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use serde::Serialize;
    use serde_json::Value;
    use ssi::jsonld::{json_to_dataset, ContextLoader};
    use ssi::ldp::{Error as LdpError, Proof};
    use ssi::rdf::DataSet;
    use tokio;

    use super::*;
    use crate::{didkey, CONTEXT_ACTIVITY_STREAMS};

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Data {
        #[serde(rename = "@context")]
        pub context: Vec<String>,
        pub id: URI,
        pub content: String,
    }

    #[derive(Debug)]
    pub struct DataWithProof {
        pub proof: Proof,
        pub data: Data,
    }

    #[async_trait]
    impl LinkedDataDocument for Data {
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

    impl ProofVerifier<Data> for DataWithProof {
        fn get_proof_issuer_did(&self) -> Result<String> {
            Ok(self.data.id.to_string())
        }
        fn extract_proof(&self) -> Result<(&Proof, &Data)> {
            Ok((&self.proof, &self.data))
        }
    }

    #[tokio::test]
    async fn builds_and_verifies_proof() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let content = "abc".to_string();
        let data = Data {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            id: URI::try_from(did).unwrap(),
            content,
        };
        let proof = build_proof(&data, &jwk).await.unwrap();
        let data_with_proof = DataWithProof { proof, data };
        data_with_proof.verify_proof().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_content() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let content = "abc".to_string();
        let data = Data {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            id: URI::try_from(did).unwrap(),
            content,
        };
        let proof = build_proof(&data, &jwk).await.unwrap();
        let data_with_proof = DataWithProof {
            proof,
            data: Data {
                content: "abcd".to_string(),
                ..data
            },
        };
        data_with_proof.verify_proof().await.unwrap_err();
    }
}
