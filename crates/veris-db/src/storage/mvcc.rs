use std::{
    borrow::Cow,
    collections::{BTreeSet, VecDeque},
    ops::{Bound, RangeBounds},
    sync::{Arc, Mutex, MutexGuard},
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    encoding::{
        KeyEncoding, ValueEncoding, bincode_deserialize, bincode_serialize, key_prefix_range,
    },
    error::Error,
    storage::engine::StorageEngine,
};

use super::engine::ScanIterator;

pub type Version = u64;
impl ValueEncoding for Version {}

#[derive(Debug, Serialize, Deserialize)]
pub enum Key<'a> {
    NextVersion,
    ActiveTransaction(Version),
    ActiveTransactionSnapshot(Version),
    TransactionWrite(
        Version,
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
        Version,
    ),
    Unversioned(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> KeyEncoding<'a> for Key<'a> {}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyPrefix<'a> {
    NextVersion,
    ActiveTransaction,
    ActiveTransactionSnapshot,
    TransactionWrite(Version),
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    Unversioned,
}

impl<'a> KeyEncoding<'a> for KeyPrefix<'a> {}

pub struct Mvcc<E: StorageEngine>(Arc<Mutex<E>>);

impl<E: StorageEngine> Mvcc<E> {
    pub fn new(engine: E) -> Self {
        Self(Arc::new(Mutex::new(engine)))
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>, Error> {
        let mut engine = self.0.lock()?;
        let version = match engine.get(&Key::NextVersion.encode()?)? {
            Some(v) => Version::decode(&v)?,
            None => 1,
        };
        engine.set(&Key::NextVersion.encode()?, (version + 1).encode()?)?;

        let active_txns = Self::scan_active_txns(&mut engine)?;
        if !active_txns.is_empty() {
            engine.set(
                &Key::ActiveTransactionSnapshot(version).encode()?,
                active_txns.encode()?,
            )?;
        }
        engine.set(&Key::ActiveTransaction(version).encode()?, Box::new([]))?;
        drop(engine);

        Ok(MvccTransaction {
            engine: self.0.clone(),
            state: MvccTransactionState {
                version,
                read_only: false,
                active_txns,
            },
        })
    }

    fn scan_active_txns(engine: &mut MutexGuard<E>) -> Result<BTreeSet<Version>, Error> {
        let mut active_txns = BTreeSet::new();
        let mut scan = engine.scan_prefix(&KeyPrefix::ActiveTransaction.encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::ActiveTransaction(version) => active_txns.insert(version),
                key => {
                    return Err(Error::InvalidEngineState(format!(
                        "expected an ActiveTransaction key, got {key:?}"
                    )));
                }
            };
        }

        Ok(active_txns)
    }
}

pub struct MvccTransaction<E: StorageEngine> {
    engine: Arc<Mutex<E>>,
    state: MvccTransactionState,
}

#[derive(Debug, Clone)]
pub struct MvccTransactionState {
    pub version: Version,
    pub read_only: bool,
    pub active_txns: BTreeSet<Version>,
}

impl MvccTransactionState {
    pub fn is_version_visible(&self, version: Version) -> bool {
        if self.active_txns.contains(&version) {
            false
        } else if self.read_only {
            version < self.version
        } else {
            version <= self.version
        }
    }
}

impl<E: StorageEngine> MvccTransaction<E> {
    fn write_version(&self, key: &[u8], value: Option<Box<[u8]>>) -> Result<(), Error> {
        if self.state.read_only {
            return Err(Error::TransactionReadOnly);
        }

        let mut engine = self.engine.lock()?;

        let from = Key::Version(
            Cow::Borrowed(key),
            self.state
                .active_txns
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode()?;
        let to = Key::Version(Cow::Borrowed(key), Version::MAX).encode()?;
        if let Some((key, _)) = engine.scan(from..=to).last().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.state.is_version_visible(version) {
                        return Err(Error::OutOfOrder);
                    }
                }
                key => {
                    return Err(Error::InvalidEngineState(format!(
                        "expected a Version key, got {key:?}"
                    )));
                }
            }
        }

        engine.set(
            &Key::TransactionWrite(self.state.version, Cow::Borrowed(key)).encode()?,
            Box::new([]),
        )?;

        engine.set(
            &Key::Version(Cow::Borrowed(key), self.state.version).encode()?,
            bincode_serialize(&value)?,
        )?;

        Ok(())
    }

    pub fn commit(self) -> Result<(), Error> {
        if self.state.read_only {
            return Ok(());
        }

        let mut engine = self.engine.lock()?;
        let to_remove: Vec<_> = engine
            .scan_prefix(&KeyPrefix::TransactionWrite(self.state.version).encode()?)
            .map_ok(|(k, _)| k)
            .try_collect()?;
        for key in to_remove {
            engine.delete(&key)?;
        }
        engine.delete(&Key::ActiveTransaction(self.state.version).encode()?)?;

        engine.flush()?;

        Ok(())
    }

    pub fn rollback(self) -> Result<(), Error> {
        if self.state.read_only {
            return Ok(());
        }

        let mut engine = self.engine.lock()?;
        let mut rollback = Vec::new();
        let mut scan =
            engine.scan_prefix(&KeyPrefix::TransactionWrite(self.state.version).encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TransactionWrite(_, key) => {
                    rollback.push(Key::Version(key, self.state.version).encode()?);
                }
                key => {
                    return Err(Error::InvalidEngineState(format!(
                        "expected a TransactionWrites key, got {key:?}"
                    )));
                }
            }

            rollback.push(key);
        }

        drop(scan);

        for key in rollback {
            engine.delete(&key)?;
        }

        engine.delete(&Key::ActiveTransaction(self.state.version).encode()?)?;

        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.write_version(key, None)
    }

    pub fn set(&self, key: &[u8], value: Box<[u8]>) -> Result<(), Error> {
        self.write_version(key, Some(value))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Box<[u8]>>, Error> {
        let mut engine = self.engine.lock()?;

        let from = Key::Version(Cow::Borrowed(key), 0).encode()?;
        let to = Key::Version(Cow::Borrowed(key), self.state.version).encode()?;
        let mut scan = engine.scan(from..=to).rev();
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if self.state.is_version_visible(version) {
                        return bincode_deserialize(&value);
                    }
                }
                key => {
                    return Err(Error::InvalidEngineState(format!(
                        "expected a Version key, got {key:?}"
                    )));
                }
            }
        }

        Ok(None)
    }

    pub fn scan(&self, range: impl RangeBounds<Box<[u8]>>) -> Result<MvccScanIterator<E>, Error> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => {
                Bound::Excluded(Key::Version(Cow::Borrowed(k), Version::MAX).encode()?)
            }
            Bound::Included(k) => Bound::Included(Key::Version(Cow::Borrowed(k), 0).encode()?),
            Bound::Unbounded => Bound::Included(Key::Version(Cow::Borrowed(&[]), 0).encode()?),
        };

        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(Cow::Borrowed(k), 0).encode()?),
            Bound::Included(k) => {
                Bound::Included(Key::Version(Cow::Borrowed(k), Version::MAX).encode()?)
            }
            Bound::Unbounded => Bound::Excluded(KeyPrefix::Unversioned.encode()?),
        };

        Ok(MvccScanIterator::new(
            self.engine.clone(),
            self.state.clone(),
            (start, end),
        ))
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<MvccScanIterator<E>, Error> {
        let mut prefix = KeyPrefix::Version(Cow::Borrowed(prefix)).encode()?.to_vec();
        prefix.truncate(prefix.len() - 2);
        let range = key_prefix_range(&prefix);
        Ok(MvccScanIterator::new(
            self.engine.clone(),
            self.state.clone(),
            range,
        ))
    }
}

pub struct MvccScanIterator<E: StorageEngine> {
    engine: Arc<Mutex<E>>,
    state: MvccTransactionState,
    buffer: VecDeque<(Box<[u8]>, Box<[u8]>)>,
    remainder: Option<(Bound<Box<[u8]>>, Bound<Box<[u8]>>)>,
}

impl<E: StorageEngine> MvccScanIterator<E> {
    const BUFFER_SIZE: usize = 32;

    fn new(
        engine: Arc<Mutex<E>>,
        state: MvccTransactionState,
        range: (Bound<Box<[u8]>>, Bound<Box<[u8]>>),
    ) -> Self {
        Self {
            engine,
            state,
            buffer: VecDeque::with_capacity(Self::BUFFER_SIZE),
            remainder: Some(range),
        }
    }

    fn fill_buffer(&mut self) -> Result<(), Error> {
        if self.buffer.len() >= Self::BUFFER_SIZE {
            return Ok(());
        }

        let Some(range) = self.remainder.take() else {
            return Ok(());
        };
        let range_end = range.1.clone();

        let mut storage = self.engine.lock()?;

        let mut scan = VersionIterator::new(&self.state, storage.scan(range)).peekable();

        while let Some((key, _, value)) = scan.next().transpose()? {
            match scan.peek() {
                Some(Ok((next, _, _))) if next == &key => continue,
                Some(Err(e)) => return Err(e.clone()),
                Some(Ok(_)) | None => {}
            }
            let Some(value) = bincode_deserialize(&value)? else {
                continue;
            };
            self.buffer.push_back((key, value));

            if self.buffer.len() == Self::BUFFER_SIZE {
                if let Some((next, version, _)) = scan.next().transpose()? {
                    let range_start =
                        Bound::Included(Key::Version(Cow::Borrowed(&next), version).encode()?);
                    self.remainder = Some((range_start, range_end));
                }
                return Ok(());
            }
        }

        Ok(())
    }
}

impl<E: StorageEngine> Iterator for MvccScanIterator<E> {
    type Item = Result<(Box<[u8]>, Box<[u8]>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            if let Err(error) = self.fill_buffer() {
                return Some(Err(error));
            }
        }
        self.buffer.pop_front().map(Ok)
    }
}

struct VersionIterator<'a, I: ScanIterator> {
    txn: &'a MvccTransactionState,
    inner: I,
}

impl<'a, I: ScanIterator> VersionIterator<'a, I> {
    fn new(txn: &'a MvccTransactionState, inner: I) -> Self {
        Self { txn, inner }
    }

    fn try_next(&mut self) -> Result<Option<(Box<[u8]>, Version, Box<[u8]>)>, Error> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            let Key::Version(key, version) = Key::decode(&key)? else {
                return Err(Error::InvalidEngineState(format!(
                    "expected a Version key, got {key:?}"
                )));
            };
            if !self.txn.is_version_visible(version) {
                continue;
            }
            return Ok(Some((key.into_owned().into(), version, value)));
        }
        Ok(None)
    }
}

impl<'a, I: ScanIterator> Iterator for VersionIterator<'a, I> {
    type Item = Result<(Box<[u8]>, Version, Box<[u8]>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::memory::Memory;

    use super::*;

    #[test]
    fn test_mvcc() -> Result<(), Error> {
        // let engine = Mvcc::new(Bitcask::new("../../data/test_mvcc")?);
        let engine = Mvcc::new(Memory::new());
        let txn = engine.begin()?;
        assert_eq!(txn.state.version, 1);
        txn.set(b"key", (*b"value").into())?;
        assert_eq!(txn.get(b"key")?, Some((*b"value").into()));
        txn.commit()?;

        let txn = engine.begin()?;
        assert_eq!(txn.state.version, 2);
        assert_eq!(txn.get(b"key")?, Some((*b"value").into()));
        txn.rollback()?;

        Ok(())
    }
}
