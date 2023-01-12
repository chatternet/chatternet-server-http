use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::cid::{cid_from_json, uri_from_cid, CidVerifier};
use crate::model::URI;
use crate::new_context_loader;

use super::stringmax::StringMaxBytes;
use super::CtxSigStream;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NoteType {
    Note,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Note1kNoId {
    #[serde(rename = "@context")]
    context: CtxSigStream,
    #[serde(rename = "type")]
    type_: NoteType,
    content: StringMaxBytes<1024>,
    media_type: Option<String>,
    attributed_to: Option<URI>,
    in_reply_to: Option<URI>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Note1kFields {
    id: URI,
    #[serde(flatten)]
    no_id: Note1kNoId,
}

impl Note1kFields {
    pub async fn new(
        type_: NoteType,
        content: String,
        media_type: Option<String>,
        attributed_to: Option<URI>,
        in_reply_to: Option<URI>,
    ) -> Result<Self> {
        let object = Note1kNoId {
            context: CtxSigStream::new(),
            type_,
            content: content.try_into()?,
            media_type,
            attributed_to,
            in_reply_to,
        };
        let id =
            uri_from_cid(cid_from_json(&object, &mut new_context_loader(), None).await?).unwrap();
        Ok(Note1kFields { id, no_id: object })
    }
}

impl CidVerifier<Note1kNoId> for Note1kFields {
    fn extract_cid(&self) -> Result<(&URI, &Note1kNoId)> {
        Ok((&self.id, &self.no_id))
    }
}

#[async_trait]
pub trait Note1k: CidVerifier<Note1kNoId> {
    fn context(&self) -> &CtxSigStream;
    fn id(&self) -> &URI;
    fn type_(&self) -> NoteType;
    fn content(&self) -> &StringMaxBytes<1024>;

    async fn verify(&self) -> Result<()> {
        self.verify_cid().await?;
        Ok(())
    }
}

impl Note1k for Note1kFields {
    fn context(&self) -> &CtxSigStream {
        &self.no_id.context
    }
    fn id(&self) -> &URI {
        &self.id
    }
    fn type_(&self) -> NoteType {
        self.no_id.type_
    }
    fn content(&self) -> &StringMaxBytes<1024> {
        &self.no_id.content
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn builds_and_verifies_body() {
        let body = Note1kFields::new(
            NoteType::Note,
            "abc".to_string(),
            Some("text/html".to_string()),
            Some("did:example:a".to_string().try_into().unwrap()),
            Some("urn:cid:a".to_string().try_into().unwrap()),
        )
        .await
        .unwrap();
        body.verify().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_build_content_too_long() {
        Note1kFields::new(
            NoteType::Note,
            std::iter::repeat("a").take(1024 + 1).collect(),
            Some("text/html".to_string()),
            Some("did:example:a".to_string().try_into().unwrap()),
            Some("urn:cid:a".to_string().try_into().unwrap()),
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_data() {
        let body = Note1kFields::new(NoteType::Note, "abc".to_string(), None, None, None)
            .await
            .unwrap();
        body.verify().await.unwrap();
        let body = Note1kFields {
            id: body.id,
            no_id: Note1kNoId {
                content: "abcd".to_string().try_into().unwrap(),
                ..body.no_id
            },
        };
        body.verify().await.unwrap_err();
    }
}
