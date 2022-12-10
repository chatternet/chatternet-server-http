use std::collections::HashMap;

use ssi::jsonld::ContextLoader;

pub mod cid;
pub mod didkey;
pub mod ldcontexts;
pub mod model;
pub mod proof;

const CONTEXT_ACTIVITY_STREAMS: &str = ldcontexts::ACTIVITY_STREAMS_URI;

/// Build a [`ContextLoader`] which can lookup the contexts in the
/// [`ldcontexts`] module, and which will not attempt to dereference documents
/// via network.
fn new_context_loader() -> ContextLoader {
    ContextLoader::empty()
        .with_static_loader()
        .with_context_map_from(HashMap::from([(
            ldcontexts::ACTIVITY_STREAMS_URI.to_string(),
            ldcontexts::ACTIVITY_STREAMS.to_string(),
        )]))
        .unwrap()
}
