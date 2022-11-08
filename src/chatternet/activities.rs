use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::prelude::{DateTime, Utc};
use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use did_method_key::DIDKey;
use serde::{Deserialize, Serialize};
use serde_json::{self, Map, Value};
use ssi::did::VerificationRelationship as ProofPurpose;
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::jwk::JWK;
use ssi::ldp::{now_ms, Error as LdpError, Proof};
use ssi::ldp::{LinkedDataDocument, LinkedDataProofs};
use ssi::rdf::DataSet;
use ssi::vc::{LinkedDataProofOptions, URI};
use ssi::{did_resolve, urdna2015};
use std::str::FromStr;

use super::ldcontexts;

const CONTEXT_ACTIVITY_STREAMS: &str = ldcontexts::ACTIVITY_STREAMS_URI;
const CONTEXT_CREDENTIALS: &str = "https://www.w3.org/2018/credentials/v1";

pub fn new_context_loader() -> ContextLoader {
    ContextLoader::empty()
        .with_static_loader()
        .with_context_map_from(HashMap::from([(
            ldcontexts::ACTIVITY_STREAMS_URI.to_string(),
            ldcontexts::ACTIVITY_STREAMS.to_string(),
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

pub fn uri_from_cid(cid: Cid) -> Result<URI> {
    Ok(URI::try_from(format!("urn:cid:{}", cid.to_string()))?)
}

pub fn cid_from_uri(uri: &URI) -> Result<Cid> {
    Ok(Cid::try_from(&uri.as_str()[8..])?)
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

async fn build_proof(
    document: &(impl LinkedDataDocument + Sync),
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
        document,
        &options,
        &DIDKey,
        &mut new_context_loader(),
        &jwk,
        None,
    )
    .await?)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ActorType {
    Application,
    Group,
    Organization,
    Person,
    Service,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: URI,
    #[serde(rename = "type")]
    pub actor_type: ActorType,
    pub inbox: URI,
    pub outbox: URI,
    pub following: URI,
    pub followers: URI,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof: Option<Proof>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
}

impl Actor {
    pub async fn new(
        did: String,
        actor_type: ActorType,
        members: Option<Map<String, Value>>,
        jwk: Option<&JWK>,
    ) -> Result<Self> {
        if members.is_some() && jwk.is_none() {
            Err(anyhow!("added members without a JWK signing key"))?;
        }
        let actor_id = actor_id_from_did(&did)?;
        let id = URI::from_str(&actor_id)?;
        let inbox = URI::try_from(format!("{}/inbox", &actor_id))?;
        let outbox = URI::try_from(format!("{}/outbox", &actor_id))?;
        let following = URI::try_from(format!("{}/following", &actor_id))?;
        let followers = URI::try_from(format!("{}/followers", &actor_id))?;
        let mut actor = Actor {
            context: vec![
                CONTEXT_ACTIVITY_STREAMS.to_string(),
                CONTEXT_CREDENTIALS.to_string(),
            ],
            id,
            actor_type,
            inbox,
            outbox,
            following,
            followers,
            proof: None,
            members,
        };
        if let Some(jwk) = jwk {
            actor.proof = Some(build_proof(&actor, &did, jwk).await?);
        }
        Ok(actor)
    }

    pub async fn verify(&self) -> Result<()> {
        let actor_id = self.id.as_str();
        let did = did_from_actor_id(actor_id)?;
        if self.inbox.as_str() != format!("{}/inbox", &actor_id) {
            Err(anyhow!("actor inbox URI is incorrect"))?;
        }
        if self.outbox.as_str() != format!("{}/outbox", &actor_id) {
            Err(anyhow!("actor outbox URI is incorrect"))?;
        }
        if self.following.as_str() != format!("{}/following", &actor_id) {
            Err(anyhow!("actor following URI is incorrect"))?;
        }
        if self.followers.as_str() != format!("{}/followers", &actor_id) {
            Err(anyhow!("actor followers URI is incorrect"))?;
        }
        if self.members.is_some() && self.proof.is_none() {
            Err(anyhow!("actor has members with no proof"))?;
        }
        if self.proof.is_none() {
            return Ok(());
        }

        let mut actor = self.clone();
        let proof_purpose = actor
            .get_default_proof_purpose()
            .ok_or(anyhow!("actor has no proof purpose"))?;
        let proof = actor
            .proof
            .take()
            .ok_or(anyhow!("actor does not contain a proof"))?;
        let verification_methods =
            did_resolve::get_verification_methods(&did, proof_purpose, &DIDKey).await?;
        match &proof.verification_method {
            Some(verification_method) => {
                if !verification_methods.contains_key(verification_method) {
                    return Err(anyhow!("actor proof cannot be verified by actor"));
                }
            }
            None => {
                return Err(anyhow!("actor proof cannot be verified"));
            }
        };
        LinkedDataProofs::verify(&proof, &actor, &DIDKey, &mut new_context_loader()).await?;
        Ok(())
    }
}

#[async_trait]
impl LinkedDataDocument for Actor {
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ObjectType {
    Article,
    Audio,
    Document,
    Event,
    Image,
    Note,
    Page,
    Place,
    Profile,
    Relationship,
    Tombstone,
    Video,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Object {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<URI>,
    #[serde(rename = "type")]
    pub object_type: ObjectType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
}

impl Object {
    pub async fn new(object_type: ObjectType, members: Option<Map<String, Value>>) -> Result<Self> {
        let mut object = Object {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            id: None,
            object_type,
            members,
        };
        object.id = Some(
            uri_from_cid(cid_from_json(&object, &mut new_context_loader(), None).await?).unwrap(),
        );
        Ok(object)
    }

    pub async fn verify(&self) -> Result<String> {
        let mut object = self.clone();
        let id = object
            .id
            .take()
            .ok_or(anyhow!("object does not contain an ID"))?;
        let cid_object = cid_from_uri(&id)?;
        let cid_data = cid_from_json(&object, &mut new_context_loader(), None).await?;
        if cid_object.hash().digest() != cid_data.hash().digest() {
            return Err(anyhow!("object ID does not match its contents"));
        }
        Ok(id.to_string())
    }
}

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
pub struct Message {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<URI>,
    #[serde(rename = "type")]
    pub message_type: ActivityType,
    pub actor: URI,
    pub object: Vec<URI>,
    pub published: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof: Option<Proof>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
}

impl Message {
    pub async fn new(
        actor_did: &str,
        objects_id: &[impl AsRef<str>],
        activity_type: ActivityType,
        published: Option<DateTime<Utc>>,
        members: Option<Map<String, Value>>,
        jwk: &JWK,
    ) -> Result<Self> {
        let actor_id = URI::try_from(actor_id_from_did(actor_did)?)?;
        let objects_id: Result<Vec<URI>> = objects_id
            .iter()
            .map(|x| URI::from_str(x.as_ref()).map_err(anyhow::Error::new))
            .collect();
        let objects_id = objects_id?;
        let published = published.unwrap_or_else(now_ms);
        // build an ID which is isomorphic to the subject object such that new
        // messages cannot override old ones
        let mut message = Message {
            context: vec![
                CONTEXT_ACTIVITY_STREAMS.to_string(),
                CONTEXT_CREDENTIALS.to_string(),
            ],
            id: None,
            message_type: activity_type,
            actor: actor_id,
            object: objects_id,
            published,
            proof: None,
            members,
        };
        message.id = Some(
            uri_from_cid(cid_from_json(&message, &mut new_context_loader(), None).await?).unwrap(),
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
        let cid_message = cid_from_uri(&id)?;
        let cid_data = cid_from_json(&message, &mut new_context_loader(), None).await?;
        if cid_message.hash().digest() != cid_data.hash().digest() {
            return Err(anyhow!("message ID does not match its contents"));
        }
        message.id = Some(id.clone());
        message.proof = Some(proof);
        Ok(id.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum InboxType {
    OrderedCollection,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Inbox {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: URI,
    #[serde(rename = "type")]
    pub inbox_type: InboxType,
    pub ordered_items: Vec<Message>,
}

impl Inbox {
    pub fn new(actor_id: &str, messages: Vec<Message>, after: Option<&str>) -> Result<Self> {
        let id = match after {
            Some(after) => format!("{}/inbox?after={}", actor_id, after),
            None => format!("{}/inbox", actor_id),
        };
        let id = URI::try_from(id)?;
        Ok(Inbox {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            id,
            inbox_type: InboxType::OrderedCollection,
            ordered_items: messages,
        })
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
            "@context": [CONTEXT_ACTIVITY_STREAMS],
            "content": "abc",
        });
        let activity_2 = json!({
            "content": "abcd",
        });
        let cid_1 = cid_from_json(&activity_1, &mut new_context_loader(), None)
            .await
            .unwrap();
        let more_contexts = serde_json::to_string(&[CONTEXT_ACTIVITY_STREAMS]).unwrap();
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
    async fn builds_actor_no_members_verifies() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let actor = Actor::new(did, ActorType::Person, None, None)
            .await
            .unwrap();
        actor.verify().await.unwrap();
    }

    #[tokio::test]
    async fn builds_actor_no_members_doesnt_verify_invalid_uris() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let actor = Actor::new(did, ActorType::Person, None, None)
            .await
            .unwrap();
        let mut actor_invalid = actor.clone();
        actor_invalid.id = URI::from_str("a:b").unwrap();
        actor_invalid.verify().await.unwrap_err();
        let mut actor_invalid = actor.clone();
        actor_invalid.inbox = URI::from_str("a:b").unwrap();
        actor_invalid.verify().await.unwrap_err();
        let mut actor_invalid = actor.clone();
        actor_invalid.outbox = URI::from_str("a:b").unwrap();
        actor_invalid.verify().await.unwrap_err();
        let mut actor_invalid = actor.clone();
        actor_invalid.following = URI::from_str("a:b").unwrap();
        actor_invalid.verify().await.unwrap_err();
        let mut actor_invalid = actor.clone();
        actor_invalid.followers = URI::from_str("a:b").unwrap();
        actor_invalid.verify().await.unwrap_err();
    }

    #[tokio::test]
    async fn builds_actor_with_members_verifies() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let actor = Actor::new(did, ActorType::Person, Some(members), Some(&jwk))
            .await
            .unwrap();
        actor.verify().await.unwrap();
    }

    #[tokio::test]
    async fn builds_actor_no_members_doesnt_verify_modified() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let actor = Actor::new(did, ActorType::Person, Some(members), Some(&jwk))
            .await
            .unwrap();
        let mut actor_invalid = actor.clone();
        actor_invalid
            .members
            .as_mut()
            .and_then(|x| x.insert("name".to_string(), serde_json::to_value("abcd").unwrap()));
        actor_invalid.verify().await.unwrap_err();
    }

    #[tokio::test]
    async fn builds_object_verifies() {
        let members = json!({"content": "abc"}).as_object().unwrap().to_owned();
        let object = Object::new(ObjectType::Note, Some(members)).await.unwrap();
        object.verify().await.unwrap();
    }

    #[tokio::test]
    async fn builds_object_doesnt_verify_invalid_id() {
        let members = json!({"content": "abc"}).as_object().unwrap().to_owned();
        let object = Object::new(ObjectType::Note, Some(members)).await.unwrap();
        let mut object_invalid = object.clone();
        object_invalid
            .members
            .as_mut()
            .and_then(|x| x.insert("content".to_string(), serde_json::to_value("abcd").unwrap()));
        object_invalid.verify().await.unwrap_err();
    }

    #[tokio::test]
    async fn builds_message_and_verifies() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = Message::new(&did, &["id:a"], ActivityType::Create, None, None, &jwk)
            .await
            .unwrap();
        message.verify().await.unwrap();
    }

    #[tokio::test]
    async fn builds_message_wrong_actor_doesnt_verify() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let mut message = Message::new(&did, &["id:a"], ActivityType::Create, None, None, &jwk)
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
        let mut message = Message::new(&did, &["id:a"], ActivityType::Create, None, None, &jwk)
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
        let mut message = Message::new(&did, &["id:a"], ActivityType::Create, None, None, &jwk)
            .await
            .unwrap();
        message.object = vec![URI::from_str("id:b").unwrap()];
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
        let mut message = Message::new(&did, &["id:a"], ActivityType::Create, None, None, &jwk)
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

    #[tokio::test]
    async fn builds_inbox() {
        let jwk = didkey::build_jwk(&mut rand::thread_rng()).unwrap();
        let did = didkey::did_from_jwk(&jwk).unwrap();
        let message = Message::new(&did, &["id:a"], ActivityType::Create, None, None, &jwk)
            .await
            .unwrap();
        let message_id = message.id.clone();
        let inbox = Inbox::new("did:example:a", Vec::new(), None).unwrap();
        assert_eq!(inbox.id.as_str(), "did:example:a/inbox");
        let inbox = Inbox::new("did:example:a", Vec::new(), Some("id:1")).unwrap();
        assert_eq!(inbox.id.as_str(), "did:example:a/inbox?after=id:1");
        let inbox = Inbox::new("did:example:a", vec![message], Some("id:1")).unwrap();
        assert_eq!(inbox.id.as_str(), "did:example:a/inbox?after=id:1");
        assert_eq!(inbox.ordered_items[0].id, message_id);
    }
}
