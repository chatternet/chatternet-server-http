//! The ChatterNet data model.
//!
//! The model is divided into three data object types:
//!
//! 1. [`Message`]: contains meta-data about a message
//! 2. [`Body`]: contains the contents a message
//! 3. [`Actor`]: describes a user
//!
//! Each is can be seen as deriving from the `ActivityStreams` `Object` class.
//! They are subsets of the `Object`, with the following constraints:
//!
//! - [`Message`]
//!   - the `id` member is a CID URI
//!   - the `type` member is an `ActivityStream` activity type
//!   - has a single `actor` member wich is a URI
//!   - has a list of `object` members wich is are URIs
//! - [`Body`]
//!   - the `id` member is a CID URI
//!   - the `type` member is an `ActivityStream` object type
//! - [`Actor`]
//!   - the `id` member is a DID Key URI with the `/actor` path appended.
//!   - the `inbox`, `outbox`, `following`, `followers` members follow the
//!     `ActivityPub` specification.
//!
//! Additionally, the [`Collection`] object provides a partial implementation
//! of the `ActivityStreams` `Collection` class.
//!
//! The [`MessageProoved`] and [`ActorProoved`] additionaly contain a
//! `proof` field which contains a data integrity proof. Objects of these
//! types are valid JSON-LD documents but are disjoint from the set of
//! `ActivityStream` objects.

mod actor;
mod body;
mod collection;
mod inbox;
mod message;

pub use actor::*;
pub use body::*;
pub use collection::*;
pub use inbox::*;
pub use message::*;
