use std::{
    collections::{BTreeSet, HashSet},
    hash::Hash,
    ops::Bound,
};

use bincode::config::*;
use itertools::Either;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{
        DeserializeOwned, DeserializeSeed, EnumAccess, IntoDeserializer, SeqAccess, VariantAccess,
    },
    ser::{Impossible, SerializeSeq, SerializeTuple, SerializeTupleVariant},
};

use crate::error::Error;

const BINCODE_CONFIG: Configuration<BigEndian, Fixint, NoLimit> =
    standard().with_big_endian().with_fixed_int_encoding();

pub fn bincode_serialize(value: &impl Serialize) -> Result<Box<[u8]>, Error> {
    bincode::serde::encode_to_vec(value, BINCODE_CONFIG)
        .map_err(|e| Error::Serialization(e.to_string()))
        .map(Into::into)
}

pub fn bincode_deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, Error> {
    bincode::serde::borrow_decode_from_slice(bytes, BINCODE_CONFIG)
        .map(|(t, _)| t)
        .map_err(|e| Error::Serialization(e.to_string()))
}

pub fn key_prefix_range(prefix: &[u8]) -> (Bound<Box<[u8]>>, Bound<Box<[u8]>>) {
    let start = Bound::Included(prefix.into());
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
    fn decode(bytes: &'de [u8]) -> Result<Self, Error> {
        let mut de = KeycodeDeserializer::new(bytes);
        Self::deserialize(&mut de)
    }

    fn encode(&self) -> Result<Box<[u8]>, Error> {
        let mut ser = KeycodeSerializer { output: Vec::new() };
        self.serialize(&mut ser)?;
        Ok(ser.output.into())
    }
}

pub trait ValueEncoding: Serialize + DeserializeOwned {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        bincode_deserialize(bytes)
    }

    fn encode(&self) -> Result<Box<[u8]>, Error> {
        bincode_serialize(self)
    }
}

impl<V: ValueEncoding> ValueEncoding for Option<V> {}
impl<V: ValueEncoding> ValueEncoding for Vec<V> {}
impl<V: ValueEncoding> ValueEncoding for Box<[V]> {}
impl<V: ValueEncoding> ValueEncoding for Box<V> {}
impl<V1: ValueEncoding, V2: ValueEncoding> ValueEncoding for (V1, V2) {}
impl<V: ValueEncoding + Eq + Hash> ValueEncoding for HashSet<V> {}
impl<V: ValueEncoding + Eq + Ord + Hash> ValueEncoding for BTreeSet<V> {}

pub struct KeycodeSerializer {
    output: Vec<u8>,
}

#[allow(unused)]
impl Serializer for &mut KeycodeSerializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleStruct = Impossible<(), Error>;

    type SerializeTupleVariant = Self;

    type SerializeMap = Impossible<(), Error>;

    type SerializeStruct = Impossible<(), Error>;

    type SerializeStructVariant = Impossible<(), Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.output.push(if v { 1 } else { 0 });
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        let mut bytes = v.to_be_bytes();
        bytes[0] ^= 1 << 7;
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        let mut bytes = v.to_be_bytes();
        if v.is_sign_negative() {
            for b in bytes.iter_mut() {
                *b = !*b;
            }
        } else {
            bytes[0] ^= 1 << 7;
        }
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let bytes = v
            .iter()
            .flat_map(|&b| match b {
                0x00 => Either::Left([0x00, 0xff].into_iter()),
                b => Either::Right([b].into_iter()),
            })
            .chain([0x00, 0x00]);
        self.output.extend(bytes);
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.output.push(variant_index as u8);
        Ok(())
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        // Err(Error::Serialization("not implemented".to_string()))
        value.serialize(self)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }
}

impl SerializeSeq for &mut KeycodeSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut KeycodeSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleVariant for &mut KeycodeSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

pub struct KeycodeDeserializer<'de> {
    bytes: &'de [u8],
}

impl<'de> KeycodeDeserializer<'de> {
    pub fn new(bytes: &'de [u8]) -> Self {
        Self { bytes }
    }

    fn take_bytes(&mut self, len: usize) -> Result<&[u8], Error> {
        if self.bytes.len() < len {
            return Err(Error::Serialization("Not enough bytes".to_string()));
        }
        let (bytes, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Ok(bytes)
    }

    fn decode_next_byte_slice(&mut self) -> Result<Box<[u8]>, Error> {
        let mut bytes = Vec::new();
        let mut iter = self.bytes.iter().enumerate();
        let taken = loop {
            match iter.next() {
                Some((_, 0x00)) => match iter.next() {
                    Some((i, 0x00)) => break i + 1,
                    Some((_, 0xff)) => bytes.push(0x00),
                    _ => return Err(Error::Serialization("invalid escape sequence".to_string())),
                },
                Some((_, &b)) => bytes.push(b),
                None => return Err(Error::Serialization("unexpected end of input".to_string())),
            }
        };
        self.bytes = &self.bytes[taken..];
        Ok(bytes.into())
    }
}

#[allow(unused)]
impl<'de> Deserializer<'de> for &mut KeycodeDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_bool(match self.take_bytes(1)?[0] {
            0x00 => false,
            0x01 => true,
            b => {
                return Err(Error::Serialization(format!(
                    "invalid boolean value: {}",
                    b
                )));
            }
        })
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7;
        #[allow(clippy::unwrap_used)]
        visitor.visit_i64(i64::from_be_bytes(bytes.as_slice().try_into().unwrap()))
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        #[allow(clippy::unwrap_used)]
        visitor.visit_u64(u64::from_be_bytes(self.take_bytes(8)?.try_into().unwrap()))
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let mut bytes = self.take_bytes(8)?.to_vec();
        if bytes[0] & (1 << 7) == 0 {
            for b in bytes.iter_mut() {
                *b = !*b;
            }
        } else {
            bytes[0] ^= 1 << 7;
        }
        #[allow(clippy::unwrap_used)]
        visitor.visit_f64(f64::from_be_bytes(bytes.as_slice().try_into().unwrap()))
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let bytes = self.decode_next_byte_slice()?;
        visitor.visit_str(&String::from_utf8(bytes.to_vec())?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let bytes = self.decode_next_byte_slice()?;
        visitor.visit_string(String::from_utf8(bytes.to_vec())?)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let bytes = self.decode_next_byte_slice()?;
        visitor.visit_bytes(&bytes)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let bytes = self.decode_next_byte_slice()?;
        visitor.visit_byte_buf(bytes.to_vec())
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }
}

impl<'de> SeqAccess<'de> for KeycodeDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.bytes.is_empty() {
            return Ok(None);
        }
        seed.deserialize(self).map(Some)
    }
}

impl<'de> EnumAccess<'de> for &mut KeycodeDeserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<T>(self, seed: T) -> Result<(T::Value, Self), Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let index = self.take_bytes(1)?[0] as u32;
        let value: Result<_, Error> = seed.deserialize(index.into_deserializer());
        Ok((value?, self))
    }
}

#[allow(unused)]
impl<'de> VariantAccess<'de> for &mut KeycodeDeserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(Error::Serialization("not implemented".to_string()))
    }
}
