use anyhow::Result;
use tap::Pipe;

use super::{CollectionPageFields, CollectionPageType, MessageFields, URI};

pub fn new_inbox(
    actor_id: &str,
    messages: Vec<MessageFields>,
    page_size: u64,
    start_idx: u64,
    end_idx: Option<u64>,
) -> Result<CollectionPageFields<MessageFields>> {
    let collection_id = format!("{}/inbox", actor_id).pipe(URI::try_from)?;
    let id = format!(
        "{}/inbox?startIdx={}&pageSize={}",
        actor_id, start_idx, page_size
    )
    .pipe(URI::try_from)?;
    let next = match end_idx {
        Some(end_idx) => Some(
            format!(
                "{}/inbox?startIdx={}&pageSize={}",
                actor_id, end_idx, page_size
            )
            .pipe(URI::try_from)?,
        ),
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
    use std::str::FromStr;

    use tokio;

    use super::*;
    use crate::didkey::build_jwk;
    use crate::model::{ActivityType, CollectionPage, Message};

    #[tokio::test]
    async fn builds_inbox() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let message = MessageFields::new(
            &jwk,
            ActivityType::Create,
            vec![URI::from_str("id:a").unwrap()],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let message_id = message.id();

        let inbox = new_inbox("did:example:a", vec![message.clone()], 4, 0, Some(3)).unwrap();
        assert_eq!(
            CollectionPage::id(&inbox).as_str(),
            "did:example:a/inbox?startIdx=0&pageSize=4"
        );
        assert_eq!(inbox.part_of().as_str(), "did:example:a/inbox");
        assert_eq!(
            inbox.next().as_ref().unwrap().as_str(),
            "did:example:a/inbox?startIdx=3&pageSize=4"
        );
        assert_eq!(inbox.items()[0].id(), message_id);

        let inbox = new_inbox("did:example:a", vec![message.clone()], 4, 0, None).unwrap();
        assert!(inbox.next().is_none());
    }
}
