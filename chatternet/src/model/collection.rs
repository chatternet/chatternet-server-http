use serde::{Deserialize, Serialize};
use ssi::vc::URI;

use crate::CONTEXT_ACTIVITY_STREAMS;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CollectionType {
    Collection,
    OrderedCollection,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CollectionFields<T> {
    #[serde(rename = "@context")]
    context: Vec<String>,
    id: URI,
    #[serde(rename = "type")]
    type_: CollectionType,
    items: Vec<T>,
}

impl<T> CollectionFields<T> {
    pub fn new(id: URI, type_: CollectionType, items: Vec<T>) -> Self {
        CollectionFields {
            context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
            id,
            type_,
            items,
        }
    }
}

pub trait Colleciton<T> {
    fn context(&self) -> &Vec<String>;
    fn id(&self) -> &URI;
    fn type_(&self) -> CollectionType;
    fn items(&self) -> &Vec<T>;
}

impl<T> Colleciton<T> for CollectionFields<T> {
    fn context(&self) -> &Vec<String> {
        &self.context
    }
    fn id(&self) -> &URI {
        &self.id
    }
    fn type_(&self) -> CollectionType {
        self.type_
    }
    fn items(&self) -> &Vec<T> {
        &self.items
    }
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;

    #[tokio::test]
    async fn builds_collection() {
        let collection = CollectionFields::new(
            URI::try_from("id:a".to_string()).unwrap(),
            CollectionType::Collection,
            vec!["abc", "def"],
        );
        assert_eq!(collection.items()[0], "abc");
        assert_eq!(collection.items()[1], "def");
    }
}
