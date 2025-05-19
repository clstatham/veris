use std::borrow::Cow;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    encoding::{KeyEncoding, ValueEncoding},
    error::Error,
    exec::expr::Expr,
    storage::engine::StorageEngine,
    types::{
        schema::{Table, TableName},
        value::{Row, Rows, Value},
    },
};

use super::{
    Catalog, Engine, Transaction,
    mvcc::{Mvcc, MvccTransaction},
};

#[derive(Debug, Serialize, Deserialize)]
pub enum Key<'a> {
    Table(Cow<'a, TableName>),
    Row(Cow<'a, TableName>, Cow<'a, Value>),
}

impl<'a> KeyEncoding<'a> for Key<'a> {}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyPrefix<'a> {
    Table,
    Row(Cow<'a, TableName>),
}

impl<'a> KeyEncoding<'a> for KeyPrefix<'a> {}

pub struct Local<E: StorageEngine>(Mvcc<E>);

impl<E: StorageEngine> Local<E> {
    pub fn new(engine: E) -> Self {
        Self(Mvcc::new(engine))
    }
}

impl<'a, E: StorageEngine + 'static> Engine<'a> for Local<E> {
    type Transaction = LocalTransaction<E>;

    fn begin(&'a self) -> Result<Self::Transaction, Error> {
        Ok(LocalTransaction(self.0.begin()?))
    }
}

pub struct LocalTransaction<E: StorageEngine>(MvccTransaction<E>);

impl<E: StorageEngine> Catalog for LocalTransaction<E> {
    fn create_table(&self, table: Table) -> Result<(), Error> {
        let table_key = Key::Table(Cow::Borrowed(&table.name)).encode()?;
        if self.0.get(&table_key)?.is_some() {
            return Err(Error::TableAlreadyExists(table.name.clone()));
        }
        let table_value = table.encode()?;
        self.0.set(&table_key, table_value)?;
        Ok(())
    }

    fn drop_table(&self, table: &TableName) -> Result<(), Error> {
        let table_key = Key::Table(Cow::Borrowed(table)).encode()?;
        if self.0.get(&table_key)?.is_none() {
            return Err(Error::TableAlreadyExists(table.clone()));
        }

        // delete the table schema
        self.0.delete(&table_key)?;

        // delete the rows
        let prefix = KeyPrefix::Row(Cow::Borrowed(table)).encode()?;
        let elems = self
            .0
            .scan_prefix(&prefix)?
            .map_ok(|r| r.0.to_vec())
            .collect_vec();
        for key in elems {
            self.0.delete(&key?)?;
        }

        Ok(())
    }

    fn get_table(&self, table: &TableName) -> Result<Option<Table>, Error> {
        let table_key = Key::Table(Cow::Borrowed(table)).encode()?;
        if let Some(table_value) = self.0.get(&table_key)? {
            let table = Table::decode(&table_value)?;
            return Ok(Some(table));
        }
        Ok(None)
    }

    fn list_tables(&self) -> Result<Vec<Table>, Error> {
        let prefix = KeyPrefix::Table.encode()?;
        self.0
            .scan_prefix(&prefix)?
            .map(|r| r.and_then(|(_, v)| Table::decode(&v)))
            .try_collect()
    }
}

impl<E: StorageEngine + 'static> Transaction for LocalTransaction<E> {
    fn commit(self) -> Result<(), Error> {
        self.0.commit()?;
        Ok(())
    }

    fn rollback(self) -> Result<(), Error> {
        self.0.rollback()?;
        Ok(())
    }

    fn delete(&self, table: &TableName, ids: &[Value]) -> Result<(), Error> {
        for id in ids {
            let key = Key::Row(Cow::Borrowed(table), Cow::Borrowed(id)).encode()?;
            self.0.delete(&key)?;
        }
        Ok(())
    }

    fn get(&self, table: &TableName, ids: &[Value]) -> Result<Box<[Row]>, Error> {
        let mut rows = Vec::new();
        for id in ids {
            let key = Key::Row(Cow::Borrowed(table), Cow::Borrowed(id)).encode()?;
            if let Some(value) = self.0.get(&key)? {
                let row = Row::decode(&value)?;
                rows.push(row);
            }
        }
        Ok(rows.into_boxed_slice())
    }

    fn insert(&self, table: &TableName, rows: Box<[Row]>) -> Result<(), Error> {
        let table = self
            .get_table(table)?
            .ok_or(Error::TableDoesNotExist(table.to_owned()))?;
        for row in rows {
            let id = &row[*table.primary_key_index];

            let key = Key::Row(Cow::Borrowed(&table.name), Cow::Borrowed(id)).encode()?;
            let value = row.encode()?;
            self.0.set(&key, value)?;
        }
        Ok(())
    }

    fn scan(&self, table: &TableName, filter: Option<Expr>) -> Result<Rows, Error> {
        let key = KeyPrefix::Row(Cow::Borrowed(table)).encode()?;
        let rows = self
            .0
            .scan_prefix(&key)?
            .map(|res| res.and_then(|(_, value)| Row::decode(&value)));

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
