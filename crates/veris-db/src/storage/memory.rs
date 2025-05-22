use std::collections::{BTreeMap, btree_map::Range};

use crate::error::Error;

use super::engine::StorageEngine;

#[derive(Default)]
pub struct Memory(BTreeMap<Box<[u8]>, Box<[u8]>>);

impl Memory {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }
}

impl StorageEngine for Memory {
    type ScanIterator<'a> = MemoryScanIterator<'a>;

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, Error> {
        Ok(self.0.get(key).cloned())
    }

    fn set(&mut self, key: &[u8], value: Box<[u8]>) -> Result<(), Error> {
        self.0.insert(key.into(), value);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Box<[u8]>>) -> Self::ScanIterator<'_> {
        MemoryScanIterator::new(self.0.range(range))
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.0.remove(key);
        Ok(())
    }
}

pub struct MemoryScanIterator<'a> {
    iter: Range<'a, Box<[u8]>, Box<[u8]>>,
}

impl<'a> MemoryScanIterator<'a> {
    pub fn new(iter: Range<'a, Box<[u8]>, Box<[u8]>>) -> Self {
        Self { iter }
    }
}

impl<'a> Iterator for MemoryScanIterator<'a> {
    type Item = Result<(Box<[u8]>, Box<[u8]>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, value)) = self.iter.next() {
            let key = key.clone();
            let value = value.clone();
            Some(Ok((key, value)))
        } else {
            None
        }
    }
}

impl<'a> DoubleEndedIterator for MemoryScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some((key, value)) = self.iter.next_back() {
            let key = key.clone();
            let value = value.clone();
            Some(Ok((key, value)))
        } else {
            None
        }
    }
}
