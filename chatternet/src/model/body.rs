use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{self, Map, Value};
use ssi::vc::URI;

use crate::cid::{cid_from_json, uri_from_cid, CidVerifier};
use crate::new_context_loader;
use crate::CONTEXT_ACTIVITY_STREAMS;

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    pub context: Vec<String>,
    #[serde(rename = "type")]
    pub type_: BodyType,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub members: Option<Map<String, Value>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Body {
    pub id: URI,
    #[serde(flatten)]
    pub no_id: BodyNoId,
}

impl Body {
    pub async fn new(type_: BodyType, members: Option<Map<String, Value>>) -> Result<Self> {
        let object = BodyNoId {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            type_,
            members,
        };
        let id =
            uri_from_cid(cid_from_json(&object, &mut new_context_loader(), None).await?).unwrap();
        Ok(Body { id, no_id: object })
    }

    pub async fn verify(&self) -> Result<()> {
        let object = self.clone();
        object.verify_cid().await?;
        Ok(())
    }
}

impl CidVerifier<BodyNoId> for Body {
    fn extract_cid(&self) -> Result<(&URI, &BodyNoId)> {
        Ok((&self.id, &self.no_id))
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use tokio;

    use super::*;

    #[tokio::test]
    async fn builds_and_verifies_body() {
        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let body = Body::new(BodyType::Note, Some(members)).await.unwrap();
        body.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_data() {
        let members = json!({"name": "abc"}).as_object().unwrap().to_owned();
        let body = Body::new(BodyType::Note, Some(members)).await.unwrap();
        body.verify().await.unwrap();
        let members = json!({"name": "abcd"}).as_object().unwrap().to_owned();
        let body = Body {
            id: body.id,
            no_id: BodyNoId {
                members: Some(members),
                ..body.no_id
            },
        };
        body.verify().await.unwrap_err();
    }
}
