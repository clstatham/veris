use std::{
    collections::{BTreeSet, VecDeque},
    ops::Bound,
    sync::{Arc, Mutex, MutexGuard},
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    ByteBounds, ByteVec, Bytes, Error, KeyEncoding, ScanIterator, StorageEngine, ValueEncoding,
    bincode_deserialize, bincode_serialize, key_prefix_range,
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
        #[serde(borrow)]
        #[serde(with = "serde_bytes")]
        Bytes<'a>,
    ),
    Version(
        #[serde(borrow)]
        #[serde(with = "serde_bytes")]
        Bytes<'a>,
        Version,
    ),
    Unversioned(
        #[serde(borrow)]
        #[serde(with = "serde_bytes")]
        Bytes<'a>,
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
        #[serde(borrow)]
        #[serde(with = "serde_bytes")]
        Bytes<'a>,
    ),
    Unversioned,
}

impl<'a> KeyEncoding<'a> for KeyPrefix<'a> {}

pub struct Mvcc<E: StorageEngine> {
    engine: Arc<Mutex<E>>,
}

impl<E: StorageEngine> Mvcc<E> {
    pub fn new(engine: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>, Error> {
        let mut engine = self.engine.lock()?;

        let version = match engine.get(&Key::NextVersion.encode()?)? {
            Some(v) => Version::decode(&v)?,
            None => 1,
        };

        engine.set(&Key::NextVersion.encode()?, &(version + 1).encode()?)?;

        let active_txns = Self::scan_active_txns(&mut engine)?;
        if !active_txns.is_empty() {
            engine.set(
                &Key::ActiveTransactionSnapshot(version).encode()?,
                &active_txns.encode()?,
            )?;
        }
        engine.set(&Key::ActiveTransaction(version).encode()?, &[])?;
        drop(engine);

        Ok(MvccTransaction {
            engine: self.engine.clone(),
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
    fn write_version(&self, key: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        if self.state.read_only {
            return Err(Error::TransactionReadOnly);
        }

        let mut engine = self.engine.lock()?;

        let from = Key::Version(
            Bytes::Borrowed(key),
            self.state
                .active_txns
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode()?;
        let to = Key::Version(Bytes::Borrowed(key), Version::MAX).encode()?;
        if let Some((key, _)) = engine.scan(from..=to).last().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.state.is_version_visible(version) {
                        return Err(Error::OutOfOrder("version".to_string()));
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
            &Key::TransactionWrite(self.state.version, Bytes::Borrowed(key)).encode()?,
            &[],
        )?;

        engine.set(
            &Key::Version(Bytes::Borrowed(key), self.state.version).encode()?,
            &bincode_serialize(&value)?,
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
            .map_ok(|(key, _)| key.into_owned())
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

            rollback.push(key.into_owned());
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

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.write_version(key, Some(value))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<ByteVec>, Error> {
        let mut engine = self.engine.lock()?;

        let from = Key::Version(Bytes::Borrowed(key), 0).encode()?;
        let to = Key::Version(Bytes::Borrowed(key), self.state.version).encode()?;
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

    pub fn scan(&self, range: impl ByteBounds) -> Result<MvccScanIterator<E>, Error> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => {
                Bound::Excluded(Key::Version(Bytes::Borrowed(k), Version::MAX).encode()?)
            }
            Bound::Included(k) => Bound::Included(Key::Version(Bytes::Borrowed(k), 0).encode()?),
            Bound::Unbounded => Bound::Included(Key::Version(Bytes::Borrowed(&[]), 0).encode()?),
        };

        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Version(Bytes::Borrowed(k), 0).encode()?),
            Bound::Included(k) => {
                Bound::Included(Key::Version(Bytes::Borrowed(k), Version::MAX).encode()?)
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
        let mut prefix = KeyPrefix::Version(Bytes::Borrowed(prefix)).encode()?;
        prefix.pop();
        prefix.pop();
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
    buffer: VecDeque<(ByteVec, ByteVec)>,
    remainder: Option<(Bound<ByteVec>, Bound<ByteVec>)>,
}

impl<E: StorageEngine> Clone for MvccScanIterator<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            state: self.state.clone(),
            buffer: self.buffer.clone(),
            remainder: self.remainder.clone(),
        }
    }
}

impl<E: StorageEngine> MvccScanIterator<E> {
    const BUFFER_SIZE: usize = 32;

    fn new(
        engine: Arc<Mutex<E>>,
        state: MvccTransactionState,
        range: (Bound<ByteVec>, Bound<ByteVec>),
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

        let Some((range_start, range_end)) = self.remainder.take() else {
            return Ok(());
        };

        let mut storage = self.engine.lock()?;

        let mut scan =
            VersionIterator::new(&self.state, storage.scan((range_start, range_end.clone())))
                .peekable();

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
                        Bound::Included(Key::Version(Bytes::Borrowed(&next), version).encode()?);
                    self.remainder = Some((range_start, range_end));
                }
                return Ok(());
            }
        }

        Ok(())
    }
}

impl<E: StorageEngine> Iterator for MvccScanIterator<E> {
    type Item = Result<(ByteVec, ByteVec), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            if let Err(error) = self.fill_buffer() {
                return Some(Err(error));
            }
        }
        self.buffer.pop_front().map(Ok)
    }
}

struct VersionIterator<'a, I: ScanIterator<'a>> {
    txn: &'a MvccTransactionState,
    inner: I,
}

impl<'a, I: ScanIterator<'a>> VersionIterator<'a, I> {
    fn new(txn: &'a MvccTransactionState, inner: I) -> Self {
        Self { txn, inner }
    }

    fn try_next(&mut self) -> Result<Option<(ByteVec, Version, Bytes<'a>)>, Error> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            let (key, version) = match Key::decode(&key)? {
                Key::Version(key, version) => (key.into_owned(), version),
                key => {
                    return Err(Error::InvalidEngineState(format!(
                        "expected a Version key, got {key:?}"
                    )));
                }
            };

            if self.txn.is_version_visible(version) {
                return Ok(Some((key, version, value)));
            }
        }
        Ok(None)
    }
}

impl<'a, I: ScanIterator<'a>> Iterator for VersionIterator<'a, I> {
    type Item = Result<(ByteVec, Version, Bytes<'a>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::*;

    use super::*;

    #[test]
    fn test_mvcc() -> Result<()> {
        let engine = Bitcask::new(Cursor::new(Vec::new())).unwrap();
        let mvcc = Mvcc::new(engine);

        let txn = mvcc.begin()?;
        txn.set(b"key", b"value")?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.get(b"key")?, Some(b"value".to_vec()));
        txn.rollback()?;

        Ok(())
    }

    #[test]
    fn test_mvcc_rollback() -> Result<()> {
        let engine = Bitcask::new(Cursor::new(Vec::new())).unwrap();
        let mvcc = Mvcc::new(engine);

        let txn = mvcc.begin()?;
        txn.set(b"key", b"value")?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        txn.set(b"key", b"new_value")?;
        txn.rollback()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.get(b"key")?, Some(b"value".to_vec()));
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_mvcc_scan() -> Result<()> {
        let engine = Bitcask::new(Cursor::new(Vec::new())).unwrap();
        let mvcc = Mvcc::new(engine);

        let txn = mvcc.begin()?;
        txn.set(b"key1", b"value1")?;
        txn.set(b"key2", b"value2")?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        let mut scan = txn.scan_prefix(b"key")?;
        assert_eq!(
            scan.next().transpose()?,
            Some((b"key1".to_vec(), b"value1".to_vec()))
        );
        assert_eq!(
            scan.next().transpose()?,
            Some((b"key2".to_vec(), b"value2".to_vec()))
        );
        assert_eq!(scan.next().transpose()?, None);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_mvcc_scan_empty() -> Result<()> {
        let engine = Bitcask::new(Cursor::new(Vec::new())).unwrap();
        let mvcc = Mvcc::new(engine);

        let txn = mvcc.begin()?;
        let mut scan = txn.scan_prefix(b"key")?;
        assert_eq!(scan.next().transpose()?, None);
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_mvcc_get() -> Result<()> {
        let engine = Bitcask::new(Cursor::new(Vec::new())).unwrap();
        let mvcc = Mvcc::new(engine);

        let txn = mvcc.begin()?;
        txn.set(b"key", b"value")?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.get(b"key")?, Some(b"value".to_vec()));
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_mvcc_delete() -> Result<()> {
        let engine = Bitcask::new(Cursor::new(Vec::new())).unwrap();
        let mvcc = Mvcc::new(engine);

        let txn = mvcc.begin()?;
        txn.set(b"key", b"value")?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        txn.delete(b"key")?;
        txn.commit()?;

        let txn = mvcc.begin()?;
        assert_eq!(txn.get(b"key")?, None);
        txn.commit()?;

        Ok(())
    }
}
