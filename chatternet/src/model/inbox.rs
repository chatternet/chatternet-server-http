use anyhow::Result;
use ssi::vc::URI;

use super::{CollectionFields, CollectionType, MessageFields};

pub fn new_inbox(
    actor_id: &str,
    messages: Vec<MessageFields>,
    after: Option<&str>,
) -> Result<CollectionFields<MessageFields>> {
    let id = match after {
        Some(after) => format!("{}/inbox?after={}", actor_id, after),
        None => format!("{}/inbox", actor_id),
    };
    let id = URI::try_from(id)?;
    Ok(CollectionFields::new(
        id,
        CollectionType::OrderedCollection,
        messages,
    ))
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;
    use crate::didkey::build_jwk;
    use crate::model::{ActivityType, Colleciton, Message};

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
        let inbox = new_inbox("did:example:a", Vec::new(), None).unwrap();
        assert_eq!(inbox.id().as_str(), "did:example:a/inbox");
        let inbox = new_inbox("did:example:a", Vec::new(), Some("id:1")).unwrap();
        assert_eq!(inbox.id().as_str(), "did:example:a/inbox?after=id:1");
        let inbox = new_inbox("did:example:a", vec![message.clone()], Some("id:1")).unwrap();
        assert_eq!(inbox.id().as_str(), "did:example:a/inbox?after=id:1");
        assert_eq!(inbox.items()[0].id(), message_id);
    }
}
