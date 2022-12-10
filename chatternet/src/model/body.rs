use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use ssi::vc::URI;

use crate::cid::{cid_from_json, uri_from_cid, CidVerifier};
use crate::new_context_loader;
use crate::CONTEXT_ACTIVITY_STREAMS;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BodyType {
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
pub struct BodyNoId {
    #[serde(rename = "@context")]
    context: Vec<String>,
    #[serde(rename = "type")]
    type_: BodyType,
    content: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BodyFields {
    id: URI,
    #[serde(flatten)]
    no_id: BodyNoId,
}

impl BodyFields {
    pub async fn new(type_: BodyType, content: Option<String>) -> Result<Self> {
        let object = BodyNoId {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            type_,
            content,
        };
        let id =
            uri_from_cid(cid_from_json(&object, &mut new_context_loader(), None).await?).unwrap();
        Ok(BodyFields { id, no_id: object })
    }
}

impl CidVerifier<BodyNoId> for BodyFields {
    fn extract_cid(&self) -> Result<(&URI, &BodyNoId)> {
        Ok((&self.id, &self.no_id))
    }
}

#[async_trait]
pub trait Body: CidVerifier<BodyNoId> {
    fn contexts(&self) -> &Vec<String>;
    fn id(&self) -> &URI;
    fn type_(&self) -> BodyType;
    fn content(&self) -> &Option<String>;

    async fn verify(&self) -> Result<()> {
        self.verify_cid().await?;
        Ok(())
    }
}

impl Body for BodyFields {
    fn contexts(&self) -> &Vec<String> {
        &&self.no_id.context
    }
    fn id(&self) -> &URI {
        &self.id
    }
    fn type_(&self) -> BodyType {
        self.no_id.type_
    }
    fn content(&self) -> &Option<String> {
        &self.no_id.content
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn builds_and_verifies_body() {
        let body = BodyFields::new(BodyType::Note, Some("abc".to_string()))
            .await
            .unwrap();
        body.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_data() {
        let body = BodyFields::new(BodyType::Note, Some("abc".to_string()))
            .await
            .unwrap();
        body.verify().await.unwrap();
        let body = BodyFields {
            id: body.id,
            no_id: BodyNoId {
                content: Some("abcd".to_string()),
                ..body.no_id
            },
        };
        body.verify().await.unwrap_err();
    }
}
