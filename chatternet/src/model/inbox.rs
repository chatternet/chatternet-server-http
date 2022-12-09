use anyhow::Result;
use ssi::vc::URI;

use super::{Collection, CollectionType, Message};

use crate::CONTEXT_ACTIVITY_STREAMS;

pub fn new_inbox(
    actor_id: &str,
    messages: Vec<Message>,
    after: Option<&str>,
) -> Result<Collection<Message>> {
    let id = match after {
        Some(after) => format!("{}/inbox?after={}", actor_id, after),
        None => format!("{}/inbox", actor_id),
    };
    let id = URI::try_from(id)?;
    Ok(Collection {
        context: vec![CONTEXT_ACTIVITY_STREAMS.to_string()],
        id,
        collection_type: CollectionType::OrderedCollection,
        items: messages,
    })
}

#[cfg(test)]
mod test {
    use tokio;

    use super::*;
    use crate::didkey::build_jwk;
    use crate::didkey::did_from_jwk;
    use crate::model::ActivityType;

    #[tokio::test]
    async fn builds_inbox() {
        let jwk = build_jwk(&mut rand::thread_rng()).unwrap();
        let did = did_from_jwk(&jwk).unwrap();
        let message = Message::new(&did, &["id:a"], ActivityType::Create, &jwk, None)
            .await
            .unwrap();
        let message_id = message.id.clone();
        let inbox = new_inbox("did:example:a", Vec::new(), None).unwrap();
        assert_eq!(inbox.id.as_str(), "did:example:a/inbox");
        let inbox = new_inbox("did:example:a", Vec::new(), Some("id:1")).unwrap();
        assert_eq!(inbox.id.as_str(), "did:example:a/inbox?after=id:1");
        let inbox = new_inbox("did:example:a", vec![message], Some("id:1")).unwrap();
        assert_eq!(inbox.id.as_str(), "did:example:a/inbox?after=id:1");
        assert_eq!(inbox.items[0].id, message_id);
    }
}
