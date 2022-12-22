use serde::{Deserialize, Serialize};
use ssi::vc::URI;

use super::AstreamContext;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum CollectionType {
    Collection,
    OrderedCollection,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CollectionFields<T> {
    #[serde(rename = "@context")]
    context: AstreamContext,
    id: URI,
    #[serde(rename = "type")]
    type_: CollectionType,
    items: Vec<T>,
}

impl<T> CollectionFields<T> {
    pub fn new(id: URI, type_: CollectionType, items: Vec<T>) -> Self {
        CollectionFields {
            context: AstreamContext::new(),
            id,
            type_,
            items,
        }
    }
}

pub trait Collection<T> {
    fn id(&self) -> &URI;
    fn type_(&self) -> CollectionType;
    fn items(&self) -> &Vec<T>;
}

impl<T> Collection<T> for CollectionFields<T> {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum CollectionPageType {
    CollectionPage,
    OrderedCollectionPage,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CollectionPageFields<T> {
    #[serde(rename = "@context")]
    context: AstreamContext,
    id: URI,
    #[serde(rename = "type")]
    type_: CollectionPageType,
    items: Vec<T>,
    part_of: URI,
    next: Option<URI>,
}

impl<T> CollectionPageFields<T> {
    pub fn new(
        id: URI,
        type_: CollectionPageType,
        items: Vec<T>,
        part_of: URI,
        next: Option<URI>,
    ) -> Self {
        CollectionPageFields {
            context: AstreamContext::new(),
            type_,
            id,
            items,
            part_of,
            next,
        }
    }
}

pub trait CollecitonPage<T> {
    fn id(&self) -> &URI;
    fn type_(&self) -> CollectionPageType;
    fn items(&self) -> &Vec<T>;
    fn part_of(&self) -> &URI;
    fn next(&self) -> &Option<URI>;
}

impl<T> CollecitonPage<T> for CollectionPageFields<T> {
    fn id(&self) -> &URI {
        &self.id
    }
    fn type_(&self) -> CollectionPageType {
        self.type_
    }
    fn items(&self) -> &Vec<T> {
        &self.items
    }
    fn part_of(&self) -> &URI {
        &self.part_of
    }
    fn next(&self) -> &Option<URI> {
        &self.next
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
        assert_eq!(collection.id.as_str(), "id:a");
        assert_eq!(collection.items()[0], "abc");
        assert_eq!(collection.items()[1], "def");
    }

    #[tokio::test]
    async fn builds_collection_page() {
        let collection = CollectionPageFields::new(
            URI::try_from("id:a/&start_idx=0".to_string()).unwrap(),
            CollectionPageType::OrderedCollectionPage,
            vec!["abc", "def"],
            URI::try_from("id:a".to_string()).unwrap(),
            Some(URI::try_from("id:a/&start_idx=2".to_string()).unwrap()),
        );
        assert_eq!(collection.id.as_str(), "id:a/&start_idx=0");
        assert_eq!(collection.items()[0], "abc");
        assert_eq!(collection.items()[1], "def");
        assert_eq!(collection.part_of.as_str(), "id:a");
        assert_eq!(collection.next.unwrap().as_str(), "id:a/&start_idx=2");
    }
}
