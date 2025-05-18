use serde::{Deserialize, Serialize};

use crate::error::Error;

pub fn encode<T: Serialize>(t: &T) -> Result<Vec<u8>, Error> {
    bincode::serde::encode_to_vec(t, bincode::config::standard())
        .map_err(|e| Error::Serialization(e.to_string()))
}

pub fn decode<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, Error> {
    bincode::serde::borrow_decode_from_slice(bytes, bincode::config::standard())
        .map(|(t, _)| t)
        .map_err(|e| Error::Serialization(e.to_string()))
}
