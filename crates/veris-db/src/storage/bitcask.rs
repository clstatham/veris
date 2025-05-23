use std::{
    collections::{BTreeMap, btree_map::Range},
    io::{self, BufReader, Read, Seek, Write},
};

use crate::{ByteBounds, ByteVec, Bytes, KeyValue, ReadBytes, Result, WriteBytes};

use super::engine::StorageEngine;

pub type KeyDir = BTreeMap<ByteVec, Location>;

pub struct Bitcask<T: Read + Write + Seek> {
    key_dir: KeyDir,
    log: Log<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    pub offset: u64,
    pub size: usize,
}

impl<T: Read + Write + Seek> Bitcask<T> {
    pub fn new(log: T) -> Result<Self> {
        let mut this = Self {
            key_dir: BTreeMap::new(),
            log: Log { file: log },
        };

        this.rebuild_key_dir()?;

        Ok(this)
    }

    fn rebuild_key_dir(&mut self) -> Result<()> {
        self.key_dir.clear();

        let mut reader = BufReader::new(&mut self.log.file);
        reader.seek(io::SeekFrom::Start(0))?;
        let file_length = reader.seek(io::SeekFrom::End(0))?;
        let mut offset = reader.seek(io::SeekFrom::Start(0))?;
        while offset < file_length {
            let mut size = [0u8; 4];
            reader.read_exact(&mut size)?;
            let key_len = u32::from_be_bytes(size);
            reader.read_exact(&mut size)?;

            let location = match i32::from_be_bytes(size) {
                size if size < 0 => None,
                size => Some(Location {
                    offset: offset + 8 + key_len as u64,
                    size: size as usize,
                }),
            };

            let mut key = vec![0; key_len as usize];
            reader.read_exact(&mut key)?;

            if let Some(location) = location {
                if location.offset + location.size as u64 > file_length {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Invalid location size",
                    )
                    .into());
                }
                reader.seek_relative(location.size as i64)?;
            }

            offset += 8 + key_len as u64 + location.map_or(0, |v| v.size as u64);

            if let Some(location) = location {
                self.key_dir.insert(key, location);
            } else {
                self.key_dir.remove(&key);
            }
        }

        Ok(())
    }

    pub fn get_location(&self, key: &[u8]) -> Option<Location> {
        self.key_dir.get(key).copied()
    }
}

pub struct Log<T: Read + Write + Seek> {
    pub file: T,
}

impl<T: Read + Write + Seek> Log<T> {
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    pub fn read_into<W>(&mut self, offset: u64, mut output: W) -> Result<()>
    where
        W: WriteBytes,
    {
        self.file.seek(io::SeekFrom::Start(offset))?;
        std::io::copy(&mut self.file, &mut output)?;

        Ok(())
    }

    pub fn read(&mut self, offset: u64, size: usize) -> Result<ByteVec> {
        let mut buf = vec![0; size];
        self.file.seek(io::SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;

        Ok(buf)
    }

    pub fn write_entry_from<R>(
        &mut self,
        key: &[u8],
        value: R,
        value_size: usize,
    ) -> Result<Location>
    where
        R: ReadBytes,
    {
        let offset = self.file.seek(io::SeekFrom::End(0))?;

        self.file.write_all(&(key.len() as u32).to_be_bytes())?;

        self.file.write_all(&(value_size as i32).to_be_bytes())?;

        self.file.write_all(key)?;
        if value_size > 0 {
            std::io::copy(&mut value.take(value_size as u64), &mut self.file)?;
        }
        self.file.flush()?;

        Ok(Location {
            offset: offset + 8 + key.len() as u64,
            size: value_size,
        })
    }

    pub fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<Location> {
        let offset = self.file.seek(io::SeekFrom::End(0))?;
        let value_length = value.map_or(-1, |v| v.len() as i32);

        self.file.write_all(&(key.len() as u32).to_be_bytes())?;

        self.file.write_all(&value_length.to_be_bytes())?;

        self.file.write_all(key)?;
        if let Some(value) = value {
            self.file.write_all(value)?;
        }
        self.file.flush()?;

        Ok(Location {
            offset: offset + 8 + key.len() as u64,
            size: value.map_or(0, |v| v.len()),
        })
    }
}

impl<T: Read + Write + Seek + 'static> StorageEngine for Bitcask<T> {
    type ScanIterator<'a> = BitcaskScanIterator<'a, T>;

    fn flush(&mut self) -> Result<()> {
        self.log.flush()?;
        Ok(())
    }

    fn get_into<W>(&mut self, key: &[u8], output: W) -> Result<Option<usize>>
    where
        W: WriteBytes,
    {
        if let Some(location) = self.get_location(key) {
            let size = location.size;
            self.log.read_into(location.offset, output)?;
            return Ok(Some(size));
        }
        Ok(None)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<ByteVec>> {
        if let Some(location) = self.get_location(key) {
            let data = self.log.read(location.offset, location.size)?;
            return Ok(Some(data));
        }
        Ok(None)
    }

    fn set_from<R>(&mut self, key: &[u8], value: R, value_size: usize) -> Result<()>
    where
        R: ReadBytes,
    {
        let location = self.log.write_entry_from(key, value, value_size)?;
        self.key_dir.insert(key.to_vec(), location);
        Ok(())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let location = self.log.write_entry(key, Some(value))?;
        self.key_dir.insert(key.to_vec(), location);
        Ok(())
    }

    fn scan<B>(&mut self, range: B) -> Self::ScanIterator<'_>
    where
        B: ByteBounds,
    {
        BitcaskScanIterator {
            range: self.key_dir.range(range),
            bitcask: &mut self.log,
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_entry(key, None)?;
        self.key_dir.remove(key);
        Ok(())
    }
}

impl<T: Read + Write + Seek> Drop for Bitcask<T> {
    fn drop(&mut self) {
        if let Err(e) = self.log.flush() {
            eprintln!("Error flushing log: {}", e);
        }
    }
}

pub struct BitcaskScanIterator<'a, T: Read + Write + Seek> {
    range: Range<'a, ByteVec, Location>,
    bitcask: &'a mut Log<T>,
}

impl<'a, T: Read + Write + Seek> Iterator for BitcaskScanIterator<'a, T> {
    type Item = Result<KeyValue<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, location)) = self.range.next() {
            let res = self.bitcask.read(location.offset, location.size);
            Some(res.map(|v| (Bytes::Borrowed(key.as_ref()), Bytes::Owned(v))))
        } else {
            None
        }
    }
}

impl<T: Read + Write + Seek> DoubleEndedIterator for BitcaskScanIterator<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some((key, location)) = self.range.next_back() {
            let res = self.bitcask.read(location.offset, location.size);
            Some(res.map(|v| (Bytes::Borrowed(key.as_ref()), Bytes::Owned(v))))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use itertools::Itertools;

    use crate::storage::engine::StorageEngine;

    use super::*;

    fn create_test_bitcask() -> Bitcask<Cursor<Vec<u8>>> {
        // Bitcask::new(tempfile::tempfile().unwrap()).unwrap()
        Bitcask::new(Cursor::new(Vec::new())).unwrap()
    }

    #[test]
    fn test_bitcask() {
        let data: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ];

        let mut bitcask = create_test_bitcask();

        for (key, value) in &data {
            bitcask.set(key, value).unwrap();
        }

        for (key, value) in &data {
            let result = bitcask.get(key).unwrap();
            assert_eq!(result, Some(value.to_vec()));
        }
    }

    #[test]
    fn test_bitcask_get_location() {
        let data: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ];

        let mut bitcask = create_test_bitcask();

        for (key, value) in &data {
            bitcask.set(key, value).unwrap();
        }

        for (key, value) in &data {
            let location = bitcask.get_location(key).unwrap();
            assert_eq!(location.size, value.len());
            assert!(location.offset > 0);
        }
    }

    #[test]
    fn test_bitcask_scan() {
        let data: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ];

        let mut bitcask = create_test_bitcask();

        for (key, value) in &data {
            bitcask.set(key, value).unwrap();
        }

        let scan_iter: Vec<_> = bitcask.scan(..).try_collect().unwrap();
        assert_eq!(scan_iter.len(), data.len());

        for (result, (key, value)) in scan_iter.iter().zip(data.iter()) {
            assert_eq!(result.0, Bytes::Borrowed(key));
            assert_eq!(result.1, Bytes::Owned(value.to_vec()));
        }
    }

    #[test]
    fn test_bitcask_delete() {
        let data: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ];

        let mut bitcask = create_test_bitcask();

        for (key, value) in &data {
            bitcask.set(key, value).unwrap();
        }

        bitcask.delete(b"key2").unwrap();

        assert_eq!(bitcask.get(b"key2").unwrap(), None);
    }

    #[test]
    fn test_bitcask_rebuild_key_dir() {
        let data: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ];

        let mut bitcask = create_test_bitcask();

        for (key, value) in &data {
            bitcask.set(key, value).unwrap();
        }

        bitcask.rebuild_key_dir().unwrap();

        for (key, value) in &data {
            let result = bitcask.get(key).unwrap();
            assert_eq!(result, Some(value.to_vec()));
        }
    }

    #[test]
    fn test_bitcask_location() {
        let data: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ];

        let mut bitcask = create_test_bitcask();

        for (key, value) in &data {
            bitcask.set(key, value).unwrap();
        }

        for (key, _) in &data {
            let location = bitcask.get_location(key).unwrap();
            assert_eq!(location.size, 6); // 6 bytes for "valueX"
            assert!(location.offset > 0);
        }
    }
}
