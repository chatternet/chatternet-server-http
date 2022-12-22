use anyhow::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(try_from = "String")]
pub struct URI(String);

impl From<URI> for String {
    fn from(uri: URI) -> String {
        return uri.0;
    }
}

impl std::convert::TryFrom<String> for URI {
    type Error = Error;
    fn try_from(uri: String) -> Result<Self, Self::Error> {
        if !uri.contains(':') || uri.len() > 2048 {
            Err(Error::msg("invalid URI string"))?
        }
        Ok(URI(uri))
    }
}

impl core::str::FromStr for URI {
    type Err = Error;
    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        URI::try_from(String::from(uri))
    }
}

impl URI {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Display for URI {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn builds_from_string() {
        URI::try_from("a:b".to_string()).unwrap();
        URI::try_from(format!(
            "a:{}",
            std::iter::repeat("b").take(2048 - 2).collect::<String>()
        ))
        .unwrap();
    }

    #[test]
    fn builds_from_str() {
        URI::from_str("a:b").unwrap();
    }

    #[test]
    fn doesnt_build_no_colon() {
        URI::try_from("ab".to_string()).unwrap_err();
    }

    #[test]
    fn doesnt_build_too_long() {
        URI::try_from(format!(
            "a:{}",
            std::iter::repeat("b").take(2048 - 1).collect::<String>()
        ))
        .unwrap_err();
    }

    #[test]
    fn builds_string_from() {
        assert_eq!(
            String::from(URI::try_from("a:b".to_string()).unwrap()),
            "a:b"
        );
    }

    #[test]
    fn returns_as_str() {
        assert_eq!(URI::try_from("a:b".to_string()).unwrap().as_str(), "a:b");
    }

    #[test]
    fn formats_string_from() {
        assert_eq!(URI::try_from("a:b".to_string()).unwrap().to_string(), "a:b");
    }

    #[test]
    fn serializes_and_deserializes() {
        let value: URI =
            serde_json::from_value(serde_json::to_value(&URI::from_str("a:b").unwrap()).unwrap())
                .unwrap();
        assert_eq!(value, URI::from_str("a:b").unwrap());
    }

    #[test]
    fn doesnt_deserialize_ivnalid() {
        serde_json::from_str::<URI>("ab").unwrap_err();
    }
}
