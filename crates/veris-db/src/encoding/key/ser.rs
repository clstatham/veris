use itertools::Either;
use serde::{
    Serialize, Serializer,
    ser::{
        Impossible, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};

use crate::error::Error;

#[derive(Default)]
pub struct KeycodeSerializer {
    output: Vec<u8>,
}

impl KeycodeSerializer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.output
    }
}

#[allow(unused)]
impl Serializer for &mut KeycodeSerializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleStruct = Self;

    type SerializeTupleVariant = Self;

    type SerializeMap = Impossible<(), Error>;

    type SerializeStruct = Self;

    type SerializeStructVariant = Self;

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
        Ok(())
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::Serialization("not implemented".to_string()))
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(self)
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

impl SerializeStruct for &mut KeycodeSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error>
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

impl SerializeStructVariant for &mut KeycodeSerializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error>
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

impl SerializeTupleStruct for &mut KeycodeSerializer {
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
