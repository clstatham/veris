use serde::de::{
    DeserializeSeed, Deserializer, EnumAccess, IntoDeserializer, SeqAccess, VariantAccess,
};

use crate::error::Error;

#[derive(Default)]
pub struct KeycodeDeserializer<'de> {
    bytes: &'de [u8],
    temp: Vec<u8>,
}

impl<'de> KeycodeDeserializer<'de> {
    pub fn new(bytes: &'de [u8]) -> Self {
        Self {
            bytes,
            temp: Vec::new(),
        }
    }

    pub fn recycle<'de2>(mut self, bytes: &'de2 [u8]) -> KeycodeDeserializer<'de2> {
        self.temp.clear();
        KeycodeDeserializer {
            bytes,
            temp: self.temp,
        }
    }

    pub fn deserialize_scope<'de2, R>(
        &mut self,
        bytes: &'de2 [u8],
        f: impl FnOnce(&mut KeycodeDeserializer<'de2>) -> R,
    ) -> R {
        let this = std::mem::take(self);
        let mut de = this.recycle(bytes);
        let result = f(&mut de);
        self.temp = de.temp;
        self.temp.clear();
        result
    }

    fn take_bytes(&mut self, len: usize) -> Result<&[u8], Error> {
        if self.bytes.len() < len {
            return Err(Error::Serialization("Not enough bytes".to_string()));
        }
        let (bytes, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Ok(bytes)
    }

    fn decode_next_byte_slice(&mut self) -> Result<&[u8], Error> {
        self.temp.clear();
        let mut iter = self.bytes.iter().enumerate();
        let taken = loop {
            match iter.next() {
                Some((_, 0x00)) => match iter.next() {
                    Some((i, 0x00)) => break i + 1,
                    Some((_, 0xff)) => self.temp.push(0x00),
                    _ => return Err(Error::Serialization("invalid escape sequence".to_string())),
                },
                Some((_, &b)) => self.temp.push(b),
                None => return Err(Error::Serialization("unexpected end of input".to_string())),
            }
        };
        self.bytes = &self.bytes[taken..];
        Ok(&self.temp)
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
        visitor.visit_bytes(bytes)
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
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
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
        visitor.visit_seq(self)
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
        visitor.visit_seq(self)
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
