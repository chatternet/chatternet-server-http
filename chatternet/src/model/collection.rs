use std::str::FromStr;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use ssi::vc::URI;

use crate::CONTEXT_ACTIVITY_STREAMS;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CollectionType {
    Collection,
    OrderedCollection,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Collection<T> {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    pub id: URI,
    #[serde(rename = "type")]
    pub collection_type: CollectionType,
    pub items: Vec<T>,
}

impl<T> Collection<T> {
    pub fn new(id: &str, collection_type: CollectionType, items: Vec<T>) -> Result<Self> {
        let id = URI::from_str(id)?;
        Ok(Collection {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            id,
            collection_type,
            items,
        })
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn builds_collection() {
        let collection =
            Collection::new("id:a", CollectionType::Collection, vec!["abc", "def"]).unwrap();
        assert_eq!(collection.items[0], "abc");
        assert_eq!(collection.items[1], "def");
    }
}
