use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::cid::{cid_from_json, uri_from_cid, CidVerifier};
use crate::model::Uri;
use crate::new_context_loader;

use super::stringmax::{StringMaxBytes, StringMaxChars};
use super::CtxStream;

#[async_trait]
pub trait Document {
    fn id(&self) -> &Uri;
    async fn verify(&self) -> Result<()>;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NoteType {
    Note,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MarkdownMediaType {
    #[serde(rename = "text/markdown")]
    Markdown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NoteMd1kNoId {
    #[serde(rename = "@context")]
    context: CtxStream,
    #[serde(rename = "type")]
    type_: NoteType,
    content: StringMaxBytes<1024>,
    media_type: MarkdownMediaType,
    attributed_to: Uri,
    in_reply_to: Option<Uri>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NoteMd1kFields {
    id: Uri,
    #[serde(flatten)]
    no_id: NoteMd1kNoId,
}

impl NoteMd1kFields {
    pub async fn new(
        content: String,
        attributed_to: Uri,
        in_reply_to: Option<Uri>,
    ) -> Result<Self> {
        let object = NoteMd1kNoId {
            context: CtxStream::new(),
            type_: NoteType::Note,
            content: content.try_into()?,
            media_type: MarkdownMediaType::Markdown,
            attributed_to,
            in_reply_to,
        };
        let id =
            uri_from_cid(cid_from_json(&object, &mut new_context_loader(), None).await?).unwrap();
        Ok(NoteMd1kFields { id, no_id: object })
    }
}

impl CidVerifier<NoteMd1kNoId> for NoteMd1kFields {
    fn extract_cid(&self) -> Result<(&Uri, &NoteMd1kNoId)> {
        Ok((&self.id, &self.no_id))
    }
}

#[async_trait]
pub trait NoteMd1k: CidVerifier<NoteMd1kNoId> + Document {
    fn context(&self) -> &CtxStream;
    fn type_(&self) -> NoteType;
    fn content(&self) -> &StringMaxBytes<1024>;
}

#[async_trait]
impl Document for NoteMd1kFields {
    fn id(&self) -> &Uri {
        &self.id
    }
    async fn verify(&self) -> Result<()> {
        self.verify_cid().await?;
        Ok(())
    }
}

impl NoteMd1k for NoteMd1kFields {
    fn context(&self) -> &CtxStream {
        &self.no_id.context
    }
    fn type_(&self) -> NoteType {
        self.no_id.type_
    }
    fn content(&self) -> &StringMaxBytes<1024> {
        &self.no_id.content
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ObjectType {
    Object,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Tag30NoId {
    #[serde(rename = "@context")]
    context: CtxStream,
    #[serde(rename = "type")]
    type_: ObjectType,
    name: StringMaxChars<30>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Tag30Fields {
    id: Uri,
    #[serde(flatten)]
    no_id: Tag30NoId,
}

impl Tag30Fields {
    pub async fn new(name: String) -> Result<Self> {
        let object = Tag30NoId {
            context: CtxStream::new(),
            type_: ObjectType::Object,
            name: name.try_into()?,
        };
        let id =
            uri_from_cid(cid_from_json(&object, &mut new_context_loader(), None).await?).unwrap();
        Ok(Tag30Fields { id, no_id: object })
    }
}

impl CidVerifier<Tag30NoId> for Tag30Fields {
    fn extract_cid(&self) -> Result<(&Uri, &Tag30NoId)> {
        Ok((&self.id, &self.no_id))
    }
}

#[async_trait]
pub trait Tag30: CidVerifier<Tag30NoId> + Document {
    fn context(&self) -> &CtxStream;
    fn type_(&self) -> ObjectType;
    fn name(&self) -> &StringMaxChars<30>;
}

#[async_trait]
impl Document for Tag30Fields {
    fn id(&self) -> &Uri {
        &self.id
    }
    async fn verify(&self) -> Result<()> {
        self.verify_cid().await?;
        Ok(())
    }
}

impl Tag30 for Tag30Fields {
    fn context(&self) -> &CtxStream {
        &self.no_id.context
    }
    fn type_(&self) -> ObjectType {
        self.no_id.type_
    }
    fn name(&self) -> &StringMaxChars<30> {
        &self.no_id.name
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn builds_and_verifies_note1k() {
        let document = NoteMd1kFields::new(
            "abc".to_string(),
            "did:example:a".to_string().try_into().unwrap(),
            Some("urn:cid:a".to_string().try_into().unwrap()),
        )
        .await
        .unwrap();
        document.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_build_note1k_content_too_long() {
        NoteMd1kFields::new(
            std::iter::repeat("a").take(1024 + 1).collect(),
            "did:example:a".to_string().try_into().unwrap(),
            Some("urn:cid:a".to_string().try_into().unwrap()),
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn doesnt_verify_note1k_modified_data() {
        let document = NoteMd1kFields::new(
            "abc".to_string(),
            "did:example:a".to_string().try_into().unwrap(),
            None,
        )
        .await
        .unwrap();
        document.verify().await.unwrap();
        let document = NoteMd1kFields {
            id: document.id,
            no_id: NoteMd1kNoId {
                content: "abcd".to_string().try_into().unwrap(),
                ..document.no_id
            },
        };
        document.verify().await.unwrap_err();
    }

    #[tokio::test]
    async fn builds_and_verifies_tag30() {
        let document = Tag30Fields::new("abc".to_string()).await.unwrap();
        document.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_build_tag30_name_too_long() {
        Tag30Fields::new(std::iter::repeat("a").take(30 + 1).collect())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn doesnt_verify_tag30_modified_data() {
        let document = Tag30Fields::new("abc".to_string()).await.unwrap();
        document.verify().await.unwrap();
        let document = Tag30Fields {
            id: document.id,
            no_id: Tag30NoId {
                name: "abcd".to_string().try_into().unwrap(),
                ..document.no_id
            },
        };
        document.verify().await.unwrap_err();
    }
}
