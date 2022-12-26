//! The ChatterNet data model.
//!
//! The model is divided into three interafces:
//!
//! 1. [`Actor`]: describes a user
//! 2. [`Body`]: contains the contents a message
//! 3. [`Message`]: contains meta-data about a message
//!
//! Each is a near subset of the ActivityStreams `Object` class. They are
//! disjoint as they further include a `proof` member which comes from the
//! data intetgrity namespace.
//!
//! Additionally, the [`Collection`] object provides a partial implementation
//! of the ActivityStreams `Collection` class.

mod actor;
mod body;
mod collection;
mod context;
mod inbox;
mod message;
mod stringmax;
mod uri;
mod vecmax;

pub use actor::*;
pub use body::*;
pub use collection::*;
pub use context::*;
pub use inbox::*;
pub use message::*;
pub use uri::*;
