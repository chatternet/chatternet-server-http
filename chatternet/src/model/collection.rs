use serde::{Deserialize, Serialize};

use super::{CtxSigStream, Uri};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum CollectionType {
    Collection,
    OrderedCollection,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CollectionFields<T> {
    #[serde(rename = "@context")]
    context: CtxSigStream,
    id: Uri,
    #[serde(rename = "type")]
    type_: CollectionType,
    items: Vec<T>,
}

impl<T> CollectionFields<T> {
    pub fn new(id: Uri, type_: CollectionType, items: Vec<T>) -> Self {
        CollectionFields {
            context: CtxSigStream::new(),
            id,
            type_,
            items,
        }
    }
}

pub trait Collection<T> {
    fn id(&self) -> &Uri;
    fn type_(&self) -> CollectionType;
    fn items(&self) -> &Vec<T>;
}

impl<T> Collection<T> for CollectionFields<T> {
    fn id(&self) -> &Uri {
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
    context: CtxSigStream,
    id: Uri,
    #[serde(rename = "type")]
    type_: CollectionPageType,
    items: Vec<T>,
    part_of: Uri,
    next: Option<Uri>,
}

impl<T> CollectionPageFields<T> {
    pub fn new(
        id: Uri,
        type_: CollectionPageType,
        items: Vec<T>,
        part_of: Uri,
        next: Option<Uri>,
    ) -> Self {
        CollectionPageFields {
            context: CtxSigStream::new(),
            type_,
            id,
            items,
            part_of,
            next,
        }
    }
}

pub trait CollectionPage<T> {
    fn id(&self) -> &Uri;
    fn type_(&self) -> CollectionPageType;
    fn items(&self) -> &Vec<T>;
    fn part_of(&self) -> &Uri;
    fn next(&self) -> &Option<Uri>;
}

impl<T> CollectionPage<T> for CollectionPageFields<T> {
    fn id(&self) -> &Uri {
        &self.id
    }
    fn type_(&self) -> CollectionPageType {
        self.type_
    }
    fn items(&self) -> &Vec<T> {
        &self.items
    }
    fn part_of(&self) -> &Uri {
        &self.part_of
    }
    fn next(&self) -> &Option<Uri> {
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
            Uri::try_from("id:a".to_string()).unwrap(),
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
            Uri::try_from("id:a/&start_idx=0".to_string()).unwrap(),
            CollectionPageType::OrderedCollectionPage,
            vec!["abc", "def"],
            Uri::try_from("id:a".to_string()).unwrap(),
            Some(Uri::try_from("id:a/&start_idx=2".to_string()).unwrap()),
        );
        assert_eq!(collection.id.as_str(), "id:a/&start_idx=0");
        assert_eq!(collection.items()[0], "abc");
        assert_eq!(collection.items()[1], "def");
        assert_eq!(collection.part_of.as_str(), "id:a");
        assert_eq!(collection.next.unwrap().as_str(), "id:a/&start_idx=2");
    }
}
