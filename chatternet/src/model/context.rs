use anyhow::Error;
use serde::{Deserialize, Serialize};

use crate::{CONTEXT_ACTIVITY_STREAMS, CONTEXT_SIGNATURE};

/// A context array that only contains the activity streams context.
///
/// It can serialize and deserialize.
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(try_from = "[String; 1]")]
pub struct CtxStream([&'static str; 1]);

impl CtxStream {
    /// Builds a new context with the Activity Streams context.
    pub fn new() -> CtxStream {
        CtxStream([CONTEXT_ACTIVITY_STREAMS])
    }
}

impl std::convert::TryFrom<[String; 1]> for CtxStream {
    type Error = Error;
    fn try_from(value: [String; 1]) -> Result<Self, Self::Error> {
        if value[0] != CONTEXT_ACTIVITY_STREAMS {
            Err(Error::msg("context is invalid"))?
        }
        Ok(CtxStream::new())
    }
}

/// A context that contains the activity streams and signature contexts
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(try_from = "[String; 2]")]
pub struct CtxSigStream([&'static str; 2]);

impl CtxSigStream {
    /// Builds a new context with the Activity Streams context.
    pub fn new() -> CtxSigStream {
        CtxSigStream([CONTEXT_SIGNATURE, CONTEXT_ACTIVITY_STREAMS])
    }
}

impl std::convert::TryFrom<[String; 2]> for CtxSigStream {
    type Error = Error;
    /// Attempts to build a new context from a slice of strings.
    fn try_from(value: [String; 2]) -> Result<Self, Self::Error> {
        if value[0] != CONTEXT_SIGNATURE || value[1] != CONTEXT_ACTIVITY_STREAMS {
            Err(Error::msg("context is invalid"))?
        }
        Ok(CtxSigStream::new())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use serde_json::json;

    #[test]
    fn serializes_and_deserializes_ctx_stream() {
        let value = json!([CONTEXT_ACTIVITY_STREAMS]);
        let ctx: CtxStream = serde_json::from_value(value.clone()).unwrap();
        let value_back = serde_json::to_value(&ctx).unwrap();
        assert_eq!(value, value_back);
    }

    #[test]
    fn doesnt_serialize_invalid_ctx_stream() {
        serde_json::from_value::<CtxStream>(json!(["a:b"])).unwrap_err();
    }

    #[test]
    fn doesnt_deserialize_invalid_ctx_stream() {
        serde_json::from_str::<CtxStream>("[a:b]").unwrap_err();
    }

    #[test]
    fn serializes_and_deserializes_ctx_sig_stream() {
        let value = json!([CONTEXT_SIGNATURE, CONTEXT_ACTIVITY_STREAMS]);
        let ctx: CtxSigStream = serde_json::from_value(value.clone()).unwrap();
        let value_back = serde_json::to_value(&ctx).unwrap();
        assert_eq!(value, value_back);
    }

    #[test]
    fn doesnt_serialize_invalid_ctx_sig_stream() {
        serde_json::from_value::<CtxSigStream>(json!(["a:b"])).unwrap_err();
    }

    #[test]
    fn doesnt_deserialize_invalid_ctx_sig_stream() {
        serde_json::from_str::<CtxStream>("[a:b]").unwrap_err();
    }
}
