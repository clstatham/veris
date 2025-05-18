use std::{
    collections::{BTreeMap, btree_map::Range},
    fs::File,
    io::{self, BufReader, Read, Seek, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, io_error};

use super::engine::StorageEngine;

pub type KeyDir = BTreeMap<Box<[u8]>, Location>;

pub struct Bitcask {
    key_dir: KeyDir,
    log: Log,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Location {
    pub offset: u64,
    pub size: usize,
}

impl Bitcask {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, Error> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root).map_err(io_error)?;
        let filename = root.join("bitcask.db");
        let file = File::options()
            .create(true)
            .append(true)
            .read(true)
            .write(false)
            .open(&filename)
            .map_err(io_error)?;
        let mut this = Self {
            key_dir: BTreeMap::new(),
            log: Log {
                file,
                file_path: filename,
            },
        };

        this.rebuild_key_dir()?;

        Ok(this)
    }

    fn rebuild_key_dir(&mut self) -> Result<(), Error> {
        self.key_dir.clear();

        let file_length = self.log.file.metadata().map_err(io_error)?.len();
        let mut reader = BufReader::new(&mut self.log.file);
        let mut offset = reader.seek(io::SeekFrom::Start(0)).map_err(io_error)?;
        while offset < file_length {
            let mut size = [0u8; 4];
            reader.read_exact(&mut size).map_err(io_error)?;
            let key_len = u32::from_be_bytes(size);
            reader.read_exact(&mut size).map_err(io_error)?;

            let location = match i32::from_be_bytes(size) {
                size if size < 0 => None,
                size => Some(Location {
                    offset: offset + 8 + key_len as u64,
                    size: size as usize,
                }),
            };

            let mut key = vec![0; key_len as usize];
            reader.read_exact(&mut key).map_err(io_error)?;

            if let Some(location) = location {
                if location.offset + location.size as u64 > file_length {
                    return Err(io_error(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Invalid location size",
                    )));
                }
                reader
                    .seek_relative(location.size as i64)
                    .map_err(io_error)?;
            }

            offset += 8 + key_len as u64 + location.map_or(0, |v| v.size as u64);

            if let Some(location) = location {
                self.key_dir.insert(key.into_boxed_slice(), location);
            } else {
                self.key_dir.remove(&*key);
            }
        }

        Ok(())
    }

    pub fn get_location(&self, key: &[u8]) -> Option<Location> {
        self.key_dir.get(key).copied()
    }
}

pub struct Log {
    pub file: File,
    pub file_path: PathBuf,
}

impl Log {
    pub fn flush(&mut self) -> Result<(), Error> {
        self.file.flush().map_err(io_error)?;
        self.file.sync_all().map_err(io_error)?;
        Ok(())
    }

    pub fn read(&mut self, offset: u64, size: usize) -> Result<Box<[u8]>, Error> {
        let mut buf = vec![0; size];
        self.file
            .seek(io::SeekFrom::Start(offset))
            .map_err(io_error)?;
        self.file.read_exact(&mut buf).map_err(io_error)?;

        Ok(buf.into_boxed_slice())
    }

    pub fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<Location, Error> {
        let offset = self.file.seek(io::SeekFrom::End(0)).map_err(io_error)?;
        let value_length = value.map_or(-1, |v| v.len() as i32);

        self.file
            .write_all(&(key.len() as u32).to_be_bytes())
            .map_err(io_error)?;

        self.file
            .write_all(&value_length.to_be_bytes())
            .map_err(io_error)?;

        self.file.write_all(key).map_err(io_error)?;
        if let Some(value) = value {
            self.file.write_all(value).map_err(io_error)?;
        }
        self.file.flush().map_err(io_error)?;

        Ok(Location {
            offset: offset + 8 + key.len() as u64,
            size: value.map_or(0, |v| v.len()),
        })
    }
}

impl StorageEngine for Bitcask {
    type ScanIterator<'a> = BitcaskScanIterator<'a>;

    fn flush(&mut self) -> Result<(), Error> {
        self.log.flush()?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Box<[u8]>>, Error> {
        if let Some(location) = self.get_location(key) {
            let data = self.log.read(location.offset, location.size)?;
            return Ok(Some(data));
        }
        Ok(None)
    }

    fn set(&mut self, key: &[u8], value: Box<[u8]>) -> Result<(), Error> {
        let location = self.log.write_entry(key, Some(&value))?;
        self.key_dir.insert(key.into(), location);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Box<[u8]>>) -> Self::ScanIterator<'_> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(start) => std::ops::Bound::Included(start.clone()),
            std::ops::Bound::Excluded(start) => std::ops::Bound::Excluded(start.clone()),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(end) => std::ops::Bound::Included(end.clone()),
            std::ops::Bound::Excluded(end) => std::ops::Bound::Excluded(end.clone()),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };
        BitcaskScanIterator {
            range: self.key_dir.range((start, end)),
            bitcask: &mut self.log,
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.log.write_entry(key, None)?;
        self.key_dir.remove(key);
        Ok(())
    }
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        if let Err(e) = self.log.flush() {
            eprintln!("Error flushing log: {}", e);
        }
    }
}

pub struct BitcaskScanIterator<'a> {
    range: Range<'a, Box<[u8]>, Location>,
    bitcask: &'a mut Log,
}

impl Iterator for BitcaskScanIterator<'_> {
    type Item = Result<(Box<[u8]>, Box<[u8]>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, location)) = self.range.next() {
            let item = || -> Result<_, Error> {
                Ok((
                    key.clone(),
                    self.bitcask.read(location.offset, location.size)?,
                ))
            }();
            return Some(item);
        }
        None
    }
}

impl DoubleEndedIterator for BitcaskScanIterator<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some((key, location)) = self.range.next_back() {
            let item = || -> Result<_, Error> {
                Ok((
                    key.clone(),
                    self.bitcask.read(location.offset, location.size)?,
                ))
            }();
            return Some(item);
        }
        None
    }
}
