use anyhow::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(try_from = "Vec<T>")]
pub struct VecMax<T, const N: usize>(Vec<T>);

impl<T, const N: usize> std::convert::TryFrom<Vec<T>> for VecMax<T, N> {
    type Error = Error;
    fn try_from(vec: Vec<T>) -> Result<Self, Self::Error> {
        if vec.len() > N {
            Err(Error::msg("too many values"))?
        }
        Ok(VecMax(vec))
    }
}

impl<T, const N: usize> std::ops::Deref for VecMax<T, N> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, const N: usize> AsRef<Vec<T>> for VecMax<T, N> {
    fn as_ref(&self) -> &Vec<T> {
        &self.0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn builds_from_vec() {
        VecMax::<_, 3>::try_from(vec![1, 2, 3]).unwrap();
    }

    #[test]
    fn doesnt_build_too_long() {
        VecMax::<_, 3>::try_from(vec![1, 2, 3, 4]).unwrap_err();
    }

    #[test]
    fn returns_as_vec() {
        assert_eq!(
            VecMax::<_, 3>::try_from(vec![1, 2, 3]).unwrap().as_ref(),
            &vec![1, 2, 3],
        );
    }

    #[test]
    fn serializes_and_deserializes() {
        let value: VecMax<i32, 3> = serde_json::from_value(
            serde_json::to_value(&VecMax::<i32, 3>::try_from(vec![1, 2, 3]).unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(value.as_ref(), &vec![1, 2, 3],);
    }

    #[test]
    fn doesnt_deserialize_invalid() {
        serde_json::from_str::<VecMax<i32, 3>>("[1,2,3,4]").unwrap_err();
    }
}
