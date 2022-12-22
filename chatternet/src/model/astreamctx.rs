use anyhow::Error;
use serde::{Deserialize, Serialize};

use crate::CONTEXT_ACTIVITY_STREAMS;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(try_from = "[String; 1]")]
pub struct AstreamContext([&'static str; 1]);

impl AstreamContext {
    pub fn new() -> AstreamContext {
        AstreamContext([CONTEXT_ACTIVITY_STREAMS])
    }
}

impl std::convert::TryFrom<[String; 1]> for AstreamContext {
    type Error = Error;
    fn try_from(value: [String; 1]) -> Result<Self, Self::Error> {
        if value[0] != CONTEXT_ACTIVITY_STREAMS {
            Err(Error::msg("actiity stream context is invalid"))?
        }
        Ok(AstreamContext::new())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn builds_new() {
        let ctx = AstreamContext::new();
        assert_eq!(ctx.0.len(), 1);
        assert_eq!(ctx.0[0], CONTEXT_ACTIVITY_STREAMS);
    }

    #[test]
    fn serializes_and_deserializes() {
        let value = serde_json::to_value(&AstreamContext::new()).unwrap();
        dbg!(&value);
        serde_json::from_value::<AstreamContext>(
            serde_json::to_value(&AstreamContext::new()).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn doesnt_deserialize_ivnalid() {
        serde_json::from_str::<AstreamContext>("[a:b]").unwrap_err();
    }
}
