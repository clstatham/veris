use std::{
    collections::{BTreeSet, HashSet},
    hash::Hash,
    io::{Read, Write},
};

use bincode::config::Config;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::error::Error;

use super::ByteVec;

fn bincode_config() -> impl Config {
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_big_endian()
        .with_no_limit()
}

pub fn bincode_serialize(value: &impl Serialize) -> Result<ByteVec, Error> {
    bincode::serde::encode_to_vec(value, bincode_config())
        .map_err(|e| Error::Serialization(e.to_string()))
}

pub fn bincode_deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, Error> {
    bincode::serde::borrow_decode_from_slice(bytes, bincode_config())
        .map(|(t, _)| t)
        .map_err(|e| Error::Serialization(e.to_string()))
}

pub fn bincode_serialize_into(value: &impl Serialize, w: &mut impl Write) -> Result<usize, Error> {
    bincode::serde::encode_into_std_write(value, w, bincode_config())
        .map_err(|e| Error::Serialization(e.to_string()))
}

pub fn bincode_deserialize_from<T: DeserializeOwned>(r: &mut impl Read) -> Result<T, Error> {
    bincode::serde::decode_from_std_read(r, bincode_config())
        .map_err(|e| Error::Serialization(e.to_string()))
}

pub trait ValueEncoding: Serialize + DeserializeOwned {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        bincode_deserialize(bytes)
    }

    fn encode(&self) -> Result<ByteVec, Error> {
        bincode_serialize(self)
    }

    fn encode_into(&self, w: &mut impl Write) -> Result<usize, Error> {
        bincode_serialize_into(self, w)
    }

    fn decode_from(r: &mut impl Read) -> Result<Self, Error> {
        bincode_deserialize_from(r)
    }
}

impl ValueEncoding for () {}
impl ValueEncoding for u8 {}
impl<V: ValueEncoding> ValueEncoding for Option<V> {}
impl<V: ValueEncoding> ValueEncoding for Vec<V> {}
impl<V: ValueEncoding> ValueEncoding for Box<[V]> {}
impl<V: ValueEncoding> ValueEncoding for Box<V> {}
impl<V1: ValueEncoding, V2: ValueEncoding> ValueEncoding for (V1, V2) {}
impl<V: ValueEncoding + Eq + Hash> ValueEncoding for HashSet<V> {}
impl<V: ValueEncoding + Eq + Ord + Hash> ValueEncoding for BTreeSet<V> {}
