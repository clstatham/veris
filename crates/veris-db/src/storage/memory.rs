use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::error::Error;

use super::engine::StorageEngine;

#[derive(Default)]
pub struct Memory(Arc<Mutex<BTreeMap<Box<[u8]>, Box<[u8]>>>>);

impl Memory {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(BTreeMap::new())))
    }

    fn lock_inner(&self) -> MutexGuard<'_, BTreeMap<Box<[u8]>, Box<[u8]>>> {
        #[allow(clippy::unwrap_used)]
        self.0.lock().map_err(|_| Error::PoisonedMutex).unwrap()
    }
}

impl StorageEngine for Memory {
    type ScanIterator<'a> = MemoryScanIterator;

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, Error> {
        Ok(self.lock_inner().get(key).cloned())
    }

    fn set(&mut self, key: &[u8], value: Box<[u8]>) -> Result<(), Error> {
        self.lock_inner().insert(key.into(), value);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Box<[u8]>>) -> Self::ScanIterator<'_> {
        MemoryScanIterator::new(
            Memory(self.0.clone()),
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        )
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.lock_inner().remove(key);
        Ok(())
    }
}

pub struct MemoryScanIterator {
    storage: Memory,
    start: Bound<Box<[u8]>>,
    end: Bound<Box<[u8]>>,
}

impl MemoryScanIterator {
    pub fn new(storage: Memory, start: Bound<Box<[u8]>>, end: Bound<Box<[u8]>>) -> Self {
        Self {
            storage,
            start,
            end,
        }
    }
}

impl Clone for MemoryScanIterator {
    fn clone(&self) -> Self {
        MemoryScanIterator {
            storage: Memory(self.storage.0.clone()),
            start: self.start.clone(),
            end: self.end.clone(),
        }
    }
}

impl Iterator for MemoryScanIterator {
    type Item = Result<(Box<[u8]>, Box<[u8]>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let storage = self.storage.lock_inner();
        let mut iter = storage.range((self.start.clone(), self.end.clone()));
        if let Some((key, value)) = iter.next() {
            let key = key.clone();
            let value = value.clone();
            self.start = match self.start {
                Bound::Included(_) => Bound::Excluded(key.clone()),
                Bound::Excluded(_) => Bound::Excluded(key.clone()),
                Bound::Unbounded => Bound::Unbounded,
            };
            Some(Ok((key, value)))
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for MemoryScanIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let storage = self.storage.lock_inner();
        let mut iter = storage.range((self.start.clone(), self.end.clone()));
        if let Some((key, value)) = iter.next_back() {
            let key = key.clone();
            let value = value.clone();
            self.end = match self.end {
                Bound::Included(_) => Bound::Excluded(key.clone()),
                Bound::Excluded(_) => Bound::Excluded(key.clone()),
                Bound::Unbounded => Bound::Unbounded,
            };
            Some(Ok((key, value)))
        } else {
            None
        }
    }
}
