use std::io::Cursor;

use crate::{ByteBounds, ByteVec, KeyValue, ReadBytes, Result, WriteBytes, key_prefix_range};

pub trait StorageEngine {
    type ScanIterator<'a>: ScanIterator<'a>
    where
        Self: 'a;

    fn flush(&mut self) -> Result<()>;

    fn get_into<W>(&mut self, key: &[u8], output: W) -> Result<Option<usize>>
    where
        W: WriteBytes;

    fn get(&mut self, key: &[u8]) -> Result<Option<ByteVec>> {
        let mut buf = ByteVec::new();
        if let Some(size) = self.get_into(key, &mut buf)? {
            debug_assert_eq!(size, buf.len());
            return Ok(Some(buf));
        }
        Ok(None)
    }

    fn set_from<R>(&mut self, key: &[u8], value: R, value_size: usize) -> Result<()>
    where
        R: ReadBytes;

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.set_from(key, &mut Cursor::new(value), value.len())?;
        Ok(())
    }

    fn scan<B>(&mut self, range: B) -> Self::ScanIterator<'_>
    where
        B: ByteBounds;

    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_> {
        self.scan(key_prefix_range(prefix))
    }

    fn delete(&mut self, key: &[u8]) -> Result<()>;
}

pub trait ScanIterator<'a>: DoubleEndedIterator<Item = Result<KeyValue<'a>>> {}
impl<'a, T> ScanIterator<'a> for T where T: DoubleEndedIterator<Item = Result<KeyValue<'a>>> {}
