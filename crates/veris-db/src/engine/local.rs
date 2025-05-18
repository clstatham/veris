use std::{
    borrow::Cow,
    ops::Bound,
    sync::{Arc, Mutex, MutexGuard},
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    encoding::{decode, encode},
    error::Error,
    exec::expr::Expr,
    storage::engine::StorageEngine,
    types::{
        schema::{Table, TableName},
        value::{Row, Rows, Value},
    },
};

use super::{Catalog, Engine, Transaction};

#[derive(Debug, Serialize, Deserialize)]
pub enum Key<'a> {
    Table(Cow<'a, TableName>),
    Row(Cow<'a, TableName>, Cow<'a, Value>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyPrefix<'a> {
    Table,
    Row(Cow<'a, TableName>),
}

#[derive(Default)]
pub struct Local<E: StorageEngine + 'static>(Arc<Mutex<E>>);

impl<E: StorageEngine + 'static> Local<E> {
    pub fn new(engine: E) -> Self {
        Self(Arc::new(Mutex::new(engine)))
    }
}

impl<'a, E: StorageEngine + 'static> Engine<'a> for Local<E> {
    type Transaction = LocalTransaction<E>;

    fn begin(&'a self) -> Result<Self::Transaction, Error> {
        Ok(LocalTransaction {
            engine: Local(self.0.clone()),
        })
    }
}

pub struct LocalTransaction<E: StorageEngine + 'static> {
    engine: Local<E>,
}

impl<E: StorageEngine + 'static> LocalTransaction<E> {
    pub fn lock_storage(&self) -> MutexGuard<'_, E> {
        #[allow(clippy::unwrap_used)]
        self.engine
            .0
            .lock()
            .map_err(|_| Error::PoisonedMutex)
            .unwrap()
    }
}

impl<E: StorageEngine + 'static> Catalog for LocalTransaction<E> {
    fn create_table(&self, table: Table) -> Result<(), Error> {
        let mut storage = self.lock_storage();
        let table_key = encode(&Key::Table(Cow::Borrowed(&table.name)))?;
        if storage.get(&table_key)?.is_some() {
            return Err(Error::TableAlreadyExists(table.name.clone()));
        }
        let table_value = encode(&table)?;
        storage.set(&table_key, table_value.into_boxed_slice())?;
        Ok(())
    }

    fn drop_table(&self, table: &TableName) -> Result<(), Error> {
        let mut storage = self.lock_storage();
        let table_key = encode(&Key::Table(Cow::Borrowed(table)))?;
        if storage.get(&table_key)?.is_none() {
            return Err(Error::TableAlreadyExists(table.clone()));
        }

        // delete the table schema
        storage.delete(&table_key)?;

        // delete the rows
        let prefix = encode(&KeyPrefix::Row(Cow::Borrowed(table)))?;
        let elems = storage
            .scan_prefix(prefix.into_boxed_slice())
            .map_ok(|r| r.0.to_vec())
            .collect_vec();
        for key in elems {
            storage.delete(&key?)?;
        }

        Ok(())
    }

    fn get_table(&self, table: &TableName) -> Result<Option<Table>, Error> {
        let mut storage = self.lock_storage();
        let table_key = encode(&Key::Table(Cow::Borrowed(table)))?;
        if let Some(table_value) = storage.get(&table_key)? {
            let table: Table = decode(&table_value)?;
            return Ok(Some(table));
        }
        Ok(None)
    }

    fn list_tables(&self) -> Result<impl Iterator<Item = Table>, Error> {
        let mut engine = self.lock_storage();
        let prefix = encode(&KeyPrefix::Table)?;
        let elems = engine
            .scan_prefix(prefix.into_boxed_slice())
            .collect::<Vec<_>>();
        let tables = elems
            .into_iter()
            .filter_map(|elem| {
                let (_, value) = elem.ok()?;
                if let Ok(table) = decode::<Table>(&value) {
                    return Some(table);
                }
                None
            })
            .collect::<Vec<_>>();
        Ok(tables.into_iter())
    }
}

impl<E: StorageEngine + 'static> Transaction for LocalTransaction<E> {
    fn commit(self) -> Result<(), Error> {
        let mut storage = self.lock_storage();
        storage.flush()?;
        Ok(())
    }

    fn rollback(self) -> Result<(), Error> {
        Ok(())
    }

    fn delete(&self, table: &TableName, ids: &[Value]) -> Result<(), Error> {
        let mut storage = self.lock_storage();
        for id in ids {
            let key = encode(&Key::Row(Cow::Borrowed(table), Cow::Borrowed(id)))?;
            storage.delete(&key)?;
        }
        Ok(())
    }

    fn get(&self, table: &TableName, ids: &[Value]) -> Result<Box<[Row]>, Error> {
        let mut storage = self.lock_storage();
        let mut rows = Vec::new();
        for id in ids {
            let key = encode(&Key::Row(Cow::Borrowed(table), Cow::Borrowed(id)))?;
            if let Some(value) = storage.get(&key)? {
                let row: Row = decode(&value)?;
                rows.push(row);
            }
        }
        Ok(rows.into_boxed_slice())
    }

    fn insert(&self, table: &TableName, rows: Box<[Row]>) -> Result<(), Error> {
        let table = self
            .get_table(table)?
            .ok_or(Error::TableDoesNotExist(table.to_owned()))?;
        let mut storage = self.lock_storage();
        for row in rows {
            let id = &row[*table.primary_key_index];

            let key = encode(&Key::Row(Cow::Borrowed(&table.name), Cow::Borrowed(id)))?;
            let value = encode(&row)?;
            storage.set(&key, value.into_boxed_slice())?;
        }
        Ok(())
    }

    fn scan(&self, table: &TableName, filter: Option<Expr>) -> Result<Rows, Error> {
        // let mut storage = self.lock_storage();
        let key = encode(&KeyPrefix::Row(Cow::Borrowed(table)))?;
        // let rows = storage
        //     .scan_prefix(key.into_boxed_slice())
        //     .map(|res| res.and_then(|(_, value)| decode(&value)));

        let rows = LocalScanIterator::new(
            self,
            Bound::Included(key.into_boxed_slice()),
            Bound::Unbounded,
        )
        .map(|res| res.and_then(|(_, value)| decode(&value)));
        let Some(filter) = filter else {
            return Ok(Box::new(rows));
        };
        let rows = rows.filter_map(move |res| {
            res.and_then(|row| match filter.evaluate(Some(&row))? {
                Value::Boolean(true) => Ok(Some(row)),
                Value::Boolean(false) => Ok(None),
                value => Err(Error::InvalidFilterResult(value)),
            })
            .transpose()
        });
        Ok(Box::new(rows))
    }
}

struct LocalScanIterator<E: StorageEngine + 'static> {
    txn: LocalTransaction<E>,
    start: Bound<Box<[u8]>>,
    end: Bound<Box<[u8]>>,
}

impl<'a, E: StorageEngine + 'static> LocalScanIterator<E> {
    fn new(txn: &'a LocalTransaction<E>, start: Bound<Box<[u8]>>, end: Bound<Box<[u8]>>) -> Self {
        Self {
            txn: LocalTransaction {
                engine: Local(txn.engine.0.clone()),
            },
            start,
            end,
        }
    }
}

impl<E: StorageEngine + 'static> Iterator for LocalScanIterator<E> {
    type Item = Result<(Box<[u8]>, Box<[u8]>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut storage = self.txn.lock_storage();
        let mut range = storage.scan((self.start.clone(), self.end.clone()));
        let value = range.next()?;
        Some(value.map(|(k, v)| {
            self.start = match self.start {
                Bound::Included(_) => Bound::Excluded(k.clone()),
                Bound::Excluded(_) => Bound::Excluded(k.clone()),
                Bound::Unbounded => Bound::Unbounded,
            };

            (k, v)
        }))
    }
}
