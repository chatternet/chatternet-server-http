use anyhow::Result;
use ssi::vc::URI;
use tap::Pipe;

use super::{CollectionPageFields, CollectionPageType, MessageFields};

pub fn new_inbox(
    actor_id: &str,
    messages: Vec<MessageFields>,
    start_idx: u64,
    end_idx: Option<u64>,
) -> Result<CollectionPageFields<MessageFields>> {
    let collection_id = format!("{}/inbox", actor_id).pipe(URI::try_from)?;
    let id = format!("{}/inbox&startIdx={}", actor_id, start_idx).pipe(URI::try_from)?;
    let next = match end_idx {
        Some(end_idx) => {
            Some(format!("{}/inbox&startIdx={}", actor_id, end_idx).pipe(URI::try_from)?)
        }
        None => None,
    };
    Ok(CollectionPageFields::new(
        id,
        CollectionPageType::OrderedCollectionPage,
        messages,
        collection_id,
        next,
    ))
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;
    use crate::didkey::build_jwk;
    use crate::model::{ActivityType, CollecitonPage, Message};

    #[tokio::test]
    async fn builds_inbox() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let message = MessageFields::new(
            &jwk,
            ActivityType::Create,
            &["id:a"],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let message_id = message.id();

        let inbox = new_inbox("did:example:a", vec![message.clone()], 0, Some(3)).unwrap();
        assert_eq!(
            CollecitonPage::id(&inbox).as_str(),
            "did:example:a/inbox&startIdx=0"
        );
        assert_eq!(inbox.part_of().as_str(), "did:example:a/inbox");
        assert_eq!(
            inbox.next().as_ref().unwrap().as_str(),
            "did:example:a/inbox&startIdx=3"
        );
        assert_eq!(inbox.items()[0].id(), message_id);

        let inbox = new_inbox("did:example:a", vec![message.clone()], 0, None).unwrap();
        assert!(inbox.next().is_none());
    }
}
