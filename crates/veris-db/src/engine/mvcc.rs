use std::{
    borrow::Cow,
    collections::BTreeSet,
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
#[repr(C)]
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

        Ok(MvccScanIterator {
            engine: self.engine.clone(),
            state: self.state.clone(),
            start,
            end,
        })
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<MvccScanIterator<E>, Error> {
        let mut prefix = KeyPrefix::Version(Cow::Borrowed(prefix)).encode()?.to_vec();
        prefix.truncate(prefix.len() - 2);
        let range = key_prefix_range(&prefix);
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        Ok(MvccScanIterator {
            engine: self.engine.clone(),
            state: self.state.clone(),
            start,
            end,
        })
    }
}

pub struct MvccScanIterator<E: StorageEngine> {
    engine: Arc<Mutex<E>>,
    state: MvccTransactionState,
    start: Bound<Box<[u8]>>,
    end: Bound<Box<[u8]>>,
}

impl<E: StorageEngine> MvccScanIterator<E> {
    pub fn try_next(&mut self) -> Result<Option<(Box<[u8]>, Box<[u8]>)>, Error> {
        let mut storage = self.engine.lock()?;
        let mut range = storage.scan((self.start.clone(), self.end.clone()));
        let value = range.next().transpose()?;
        if let Some((key, value)) = value {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.state.is_version_visible(version) {
                        return Ok(None);
                    }
                }
                key => {
                    return Err(Error::InvalidEngineState(format!(
                        "expected a Versioned key, got {key:?}"
                    )));
                }
            }
            let Some(value) = bincode_deserialize(&value)? else {
                return Ok(None);
            };
            self.start = match self.start {
                Bound::Included(_) => Bound::Excluded(key.clone()),
                Bound::Excluded(_) => Bound::Excluded(key.clone()),
                Bound::Unbounded => Bound::Unbounded,
            };

            Ok(Some((key, value)))
        } else {
            Ok(None)
        }
    }
}

impl<E: StorageEngine> Iterator for MvccScanIterator<E> {
    type Item = Result<(Box<[u8]>, Box<[u8]>), Error>;

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
