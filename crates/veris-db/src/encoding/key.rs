use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::Result;

use super::ByteVec;

pub mod de;
pub mod ser;

pub use self::{de::*, ser::*};

pub fn key_serialize<T: Serialize>(value: &T) -> Result<ByteVec> {
    let mut ser = KeycodeSerializer::new();
    value.serialize(&mut ser)?;
    Ok(ser.into_inner())
}

pub fn key_deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    let mut de = KeycodeDeserializer::new(bytes);
    T::deserialize(&mut de)
}

pub fn key_prefix_range(prefix: &[u8]) -> (Bound<ByteVec>, Bound<ByteVec>) {
    let start = Bound::Included(prefix.to_vec());
    let end = match prefix.iter().rposition(|&b| b != 0xff) {
        Some(i) => Bound::Excluded(
            prefix
                .iter()
                .take(i)
                .copied()
                .chain(std::iter::once(prefix[i] + 1))
                .collect(),
        ),
        None => Bound::Unbounded,
    };
    (start, end)
}

pub trait KeyEncoding<'de>: Serialize + Deserialize<'de> {
    fn decode(bytes: &'de [u8]) -> Result<Self> {
        key_deserialize(bytes)
    }

    fn encode(&self) -> Result<ByteVec> {
        key_serialize(self)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeBounds;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    struct TestKey {
        id: i64,
        name: String,
    }

    impl KeyEncoding<'_> for TestKey {}

    #[test]
    fn test_key_encoding() {
        let key = TestKey {
            id: 1,
            name: "test".to_string(),
        };
        let encoded = key.encode().unwrap();
        let decoded: TestKey = KeyEncoding::decode(&encoded).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_key_encoding_order() {
        let key1 = TestKey {
            id: 1,
            name: "test".to_string(),
        };
        let key2 = TestKey {
            id: 2,
            name: "test".to_string(),
        };
        assert!(key1 < key2);
        assert!(key2 > key1);
        assert!(key1 != key2);

        let encoded1 = key1.encode().unwrap();
        let encoded2 = key2.encode().unwrap();
        assert!(encoded1 < encoded2);
        assert!(encoded2 > encoded1);
        assert!(encoded1 != encoded2);

        let decoded1: TestKey = KeyEncoding::decode(&encoded1).unwrap();
        let decoded2: TestKey = KeyEncoding::decode(&encoded2).unwrap();
        assert_eq!(key1, decoded1);
        assert_eq!(key2, decoded2);
        assert!(decoded1 < decoded2);
        assert!(decoded2 > decoded1);
        assert!(decoded1 != decoded2);
    }

    #[test]
    fn test_key_prefix_range() {
        let prefix = b"test";
        let (start, end) = key_prefix_range(prefix);
        assert_eq!(start, Bound::Included(b"test".to_vec()));
        assert_eq!(end, Bound::Excluded(b"tesu".to_vec()));
    }

    #[test]
    fn test_key_prefix() {
        let key1 = b"test1";
        let key2 = b"test2";
        let prefix = b"test";
        let (start, end) = key_prefix_range(prefix);
        let range = (start, end);
        assert!(range.contains(&key1.to_vec()));
        assert!(range.contains(&key2.to_vec()));
    }
}
