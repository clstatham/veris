use std::ops::RangeBounds;

use crate::{encoding::key_prefix_range, error::Error};

pub trait StorageEngine {
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: 'a;

    fn flush(&mut self) -> Result<(), Error>;
    fn get(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, Error>;
    fn set(&mut self, key: &[u8], value: Box<[u8]>) -> Result<(), Error>;
    fn scan(&mut self, range: impl RangeBounds<Box<[u8]>>) -> Self::ScanIterator<'_>;
    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_> {
        self.scan(key_prefix_range(prefix))
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), Error>;
}

pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Box<[u8]>, Box<[u8]>), Error>> {}
impl<T> ScanIterator for T where T: DoubleEndedIterator<Item = Result<(Box<[u8]>, Box<[u8]>), Error>>
{}
