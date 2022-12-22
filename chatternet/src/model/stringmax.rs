use anyhow::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(try_from = "String")]
pub struct StringMaxChars<const N: usize>(String);

impl<const N: usize> From<StringMaxChars<N>> for String {
    fn from(string: StringMaxChars<N>) -> String {
        return string.0;
    }
}

impl<const N: usize> std::convert::TryFrom<String> for StringMaxChars<N> {
    type Error = Error;
    fn try_from(string: String) -> Result<Self, Self::Error> {
        if string.chars().count() > N {
            Err(Error::msg("string has too many characters"))?
        }
        Ok(StringMaxChars(string))
    }
}

impl<const N: usize> core::str::FromStr for StringMaxChars<N> {
    type Err = Error;
    fn from_str(string: &str) -> Result<Self, Self::Err> {
        StringMaxChars::try_from(String::from(string))
    }
}

impl<const N: usize> StringMaxChars<N> {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<const N: usize> std::fmt::Display for StringMaxChars<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(try_from = "String")]
pub struct StringMaxBytes<const N: usize>(String);

impl<const N: usize> From<StringMaxBytes<N>> for String {
    fn from(string: StringMaxBytes<N>) -> String {
        return string.0;
    }
}

impl<const N: usize> std::convert::TryFrom<String> for StringMaxBytes<N> {
    type Error = Error;
    fn try_from(string: String) -> Result<Self, Self::Error> {
        if string.len() > N {
            Err(Error::msg("string has too many bytes"))?
        }
        Ok(StringMaxBytes(string))
    }
}

impl<const N: usize> core::str::FromStr for StringMaxBytes<N> {
    type Err = Error;
    fn from_str(string: &str) -> Result<Self, Self::Err> {
        StringMaxBytes::try_from(String::from(string))
    }
}

impl<const N: usize> StringMaxBytes<N> {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<const N: usize> std::fmt::Display for StringMaxBytes<N> {
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
        StringMaxChars::<3>::try_from("Ābc".to_string()).unwrap();
        StringMaxBytes::<4>::try_from("Ābc".to_string()).unwrap();
    }

    #[test]
    fn builds_from_str() {
        StringMaxChars::<3>::from_str("Ābc").unwrap();
        StringMaxBytes::<4>::from_str("Ābc").unwrap();
    }

    #[test]
    fn doesnt_build_too_long() {
        StringMaxChars::<3>::from_str("Ābcd").unwrap_err();
        StringMaxBytes::<4>::from_str("Ābcd").unwrap_err();
    }

    #[test]
    fn builds_string_from() {
        assert_eq!(
            String::from(StringMaxChars::<3>::from_str("Ābc").unwrap()),
            "Ābc"
        );
        assert_eq!(
            String::from(StringMaxBytes::<4>::from_str("Ābc").unwrap()),
            "Ābc"
        );
    }

    #[test]
    fn returns_as_str() {
        assert_eq!(
            StringMaxChars::<3>::from_str("Ābc").unwrap().as_str(),
            "Ābc"
        );
        assert_eq!(
            StringMaxBytes::<4>::from_str("Ābc").unwrap().as_str(),
            "Ābc"
        );
    }

    #[test]
    fn formats_string() {
        assert_eq!(
            StringMaxChars::<3>::from_str("Ābc").unwrap().to_string(),
            "Ābc"
        );
        assert_eq!(
            StringMaxBytes::<4>::from_str("Ābc").unwrap().to_string(),
            "Ābc"
        );
    }

    #[test]
    fn serializes_and_deserializes() {
        let value: StringMaxChars<3> = serde_json::from_value(
            serde_json::to_value(&StringMaxChars::<3>::from_str("Ābc").unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(value.as_str(), "Ābc");
        let value: StringMaxBytes<4> = serde_json::from_value(
            serde_json::to_value(&StringMaxBytes::<4>::from_str("Ābc").unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(value.as_str(), "Ābc");
    }

    #[test]
    fn doesnt_deserialize_ivnalid() {
        serde_json::from_str::<StringMaxChars<3>>("Ābc").unwrap_err();
        serde_json::from_str::<StringMaxBytes<4>>("Ābc").unwrap_err();
    }
}
