//! Build CIDs, verify CIDs match some document, and convert to and from the
//! URI representation.

use anyhow::{Error, Result};
use async_trait::async_trait;
use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use serde::Serialize;
use serde_json;
use ssi::jsonld::{json_to_dataset, ContextLoader};
use ssi::urdna2015;

use crate::model::URI;
use crate::new_context_loader;

/// Build a CID from a JSON-LD document.
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

/// Build a URI from a CID.
///
/// This is an ad-hoc extension of the `urn` namespace and might have
/// conflicts.
pub fn uri_from_cid(cid: Cid) -> Result<URI> {
    Ok(URI::try_from(format!("urn:cid:{}", cid.to_string()))?)
}

pub fn cid_from_uri(uri: &URI) -> Result<Cid> {
    let uri_str = uri.as_str();
    if uri_str.len() < 8 {
        Err(Error::msg("URI does not contain a CID"))?;
    }
    Ok(Cid::try_from(&uri_str[8..])?)
}

/// An object whith some verifiable content and associated proof.
#[async_trait]
pub trait CidVerifier<WithoutCid: Serialize + Sized + Sync> {
    /// Return a CID and the `Serializer` object without the CID.
    fn extract_cid(&self) -> Result<(&URI, &WithoutCid)>;

    /// Verify the object's CID matches its contents.
    async fn verify_cid(&self) -> Result<()> {
        let (cid, without_cid) = self.extract_cid()?;
        let cid = cid_from_uri(cid)?;
        let cid_data = cid_from_json(without_cid, &mut new_context_loader(), None).await?;
        if cid.hash().digest() != cid_data.hash().digest() {
            Err(Error::msg("document contents do not match CID"))?
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use tap::Pipe;
    use tokio;

    use super::*;
    use crate::{model::AstreamContext, new_context_loader};

    #[tokio::test]
    async fn builds_cid_from_object() {
        let activity_1 = json!({
            "@context": AstreamContext::new(),
            "content": "abc",
        });
        let activity_2 = json!({
            "content": "abcd",
        });
        let cid_1 = cid_from_json(&activity_1, &mut new_context_loader(), None)
            .await
            .unwrap();
        let more_contexts = serde_json::to_string(&AstreamContext::new()).unwrap();
        let cid_2 = cid_from_json(&activity_2, &mut new_context_loader(), Some(&more_contexts))
            .await
            .unwrap();
        assert_ne!(cid_1.to_string(), cid_2.to_string());
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Data {
        #[serde(rename = "@context")]
        pub context: AstreamContext,
        pub content: String,
    }

    #[derive(Debug)]
    pub struct DataWithId {
        pub id: URI,
        pub data: Data,
    }

    impl CidVerifier<Data> for DataWithId {
        fn extract_cid(&self) -> Result<(&URI, &Data)> {
            Ok((&self.id, &self.data))
        }
    }

    #[tokio::test]
    async fn builds_and_verifies_cid() {
        let content = "abc".to_string();
        let data = Data {
            context: AstreamContext::new(),
            content,
        };
        let id = cid_from_json(&data, &mut new_context_loader(), None)
            .await
            .unwrap()
            .pipe(uri_from_cid)
            .unwrap();
        let data_with_id = DataWithId { id, data };
        data_with_id.verify_cid().await.unwrap();
    }

    #[tokio::test]
    async fn doesnt_verify_modified_content() {
        let content = "abc".to_string();
        let data = Data {
            context: AstreamContext::new(),
            content,
        };
        let id = cid_from_json(&data, &mut new_context_loader(), None)
            .await
            .unwrap()
            .pipe(uri_from_cid)
            .unwrap();
        let data_with_id = DataWithId {
            id,
            data: Data {
                content: "abcd".to_string(),
                ..data
            },
        };
        data_with_id.verify_cid().await.unwrap_err();
    }
}
