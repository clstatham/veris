use std::{
    borrow::Cow,
    io::{Read, Write},
    ops::RangeBounds,
};

pub type Bytes<'a> = Cow<'a, [u8]>;
pub type ByteVec = Vec<u8>;

pub trait ReadBytes: Read {
    fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes<'_>>;
}

impl<R: Read> ReadBytes for R {
    fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes<'_>> {
        let mut buf = vec![0; len];
        self.read_exact(&mut buf)?;
        Ok(Cow::Owned(buf))
    }
}

pub trait WriteBytes: Write {
    fn write_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()>;
}

impl<W: Write> WriteBytes for W {
    fn write_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.write_all(bytes)
    }
}

pub trait ByteBounds: RangeBounds<ByteVec> {}

impl<T: RangeBounds<ByteVec>> ByteBounds for T {}
