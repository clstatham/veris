use std::{borrow::Cow, collections::BTreeSet};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    encoding::{KeyEncoding, ValueEncoding},
    error::Error,
    exec::expr::Expr,
    storage::{
        engine::StorageEngine,
        mvcc::{Mvcc, MvccTransaction},
    },
    types::{
        schema::{ColumnIndex, ColumnName, Table, TableName},
        value::{Row, Rows, Value},
    },
};

use super::{Catalog, Engine, Transaction};

#[derive(Debug, Serialize, Deserialize)]
pub enum Key<'a> {
    Table(Cow<'a, TableName>),
    Index(Cow<'a, TableName>, Cow<'a, ColumnName>, Cow<'a, Value>),
    Row(Cow<'a, TableName>, Cow<'a, Value>),
}

impl<'a> KeyEncoding<'a> for Key<'a> {}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyPrefix<'a> {
    Table,
    Index(Cow<'a, TableName>, Cow<'a, ColumnName>),
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

impl<E: StorageEngine> LocalTransaction<E> {
    fn get_row(&self, table: &TableName, id: &Value) -> Result<Option<Row>, Error> {
        let key = Key::Row(Cow::Borrowed(table), Cow::Borrowed(id)).encode()?;
        if let Some(value) = self.0.get(&key)? {
            let row = Row::decode(&value)?;
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn has_index(&self, table: &TableName, column: &ColumnName) -> Result<bool, Error> {
        let table = self
            .get_table(table)?
            .ok_or(Error::TableDoesNotExist(table.to_owned()))?;
        Ok(table
            .columns
            .iter()
            .find(|c| &c.name == column)
            .map(|c| c.has_secondary_index)
            .unwrap_or(false))
    }

    fn get_index(
        &self,
        table: &TableName,
        column: &ColumnName,
        value: &Value,
    ) -> Result<BTreeSet<Value>, Error> {
        debug_assert!(self.has_index(table, column)?);
        Ok(self
            .0
            .get(
                &Key::Index(
                    Cow::Borrowed(table),
                    Cow::Borrowed(column),
                    Cow::Borrowed(value),
                )
                .encode()?,
            )?
            .map(|v| BTreeSet::decode(&v))
            .transpose()?
            .unwrap_or_default())
    }

    fn set_index(
        &self,
        table: &TableName,
        column: &ColumnName,
        value: &Value,
        ids: BTreeSet<Value>,
    ) -> Result<(), Error> {
        debug_assert!(self.has_index(table, column)?);
        let key = Key::Index(
            Cow::Borrowed(table),
            Cow::Borrowed(column),
            Cow::Borrowed(value),
        )
        .encode()?;
        if ids.is_empty() {
            self.0.delete(&key)?;
        } else {
            self.0.set(&key, ids.encode()?)?;
        }

        Ok(())
    }

    fn table_refs(
        &self,
        referenced_table: &TableName,
    ) -> Result<Vec<(Table, Vec<ColumnIndex>)>, Error> {
        let tables = self.list_tables()?;
        let mut refs = Vec::new();
        for table in tables {
            let r = table
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| {
                    c.references
                        .as_ref()
                        .is_some_and(|key| &key.table == referenced_table)
                })
                .map(|(i, _)| ColumnIndex::new(i))
                .collect_vec();
            if !r.is_empty() {
                refs.push((table, r));
            }
        }
        Ok(refs)
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
        let table = self
            .get_table(table)?
            .ok_or(Error::TableDoesNotExist(table.to_owned()))?;

        let indices = table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.has_secondary_index)
            .collect_vec();

        for (source, refs) in self.table_refs(&table.name)? {
            let self_reference = source.name == table.name;
            for i in refs {
                let column = &source.columns[i.clone().into_inner()];
                let mut source_ids: BTreeSet<Value> = if i == source.primary_key_index {
                    self.get(&source.name, ids)?
                        .into_iter()
                        .map(|row| {
                            row.into_iter()
                                .nth(i.clone().into_inner())
                                .ok_or(Error::InvalidRowState)
                        })
                        .try_collect()?
                } else {
                    self.lookup_index(&source.name, &column.name, ids)?
                };

                if self_reference {
                    for id in ids {
                        source_ids.remove(id);
                    }
                }

                if let Some(source_id) = source_ids.first() {
                    let table = source.name.clone();
                    let column = source.columns[source.primary_key_index.into_inner()]
                        .name
                        .clone();
                    return Err(Error::ReferentialIntegrity(
                        table,
                        column,
                        source_id.clone(),
                    ));
                }
            }
        }

        for id in ids {
            if !indices.is_empty() {
                if let Some(row) = self.get_row(&table.name, id)? {
                    for (i, column) in indices.iter().copied() {
                        let mut ids = self.get_index(&table.name, &column.name, &row[i])?;
                        ids.remove(id);
                        self.set_index(&table.name, &column.name, &row[i], ids)?;
                    }
                }
            }

            self.0
                .delete(&Key::Row(Cow::Borrowed(&table.name), Cow::Borrowed(id)).encode()?)?;
        }
        Ok(())
    }

    fn get(&self, table: &TableName, ids: &[Value]) -> Result<Box<[Row]>, Error> {
        ids.iter()
            .filter_map(|id| self.get_row(table, id).transpose())
            .collect()
    }

    fn insert(&self, table: &TableName, rows: Box<[Row]>) -> Result<(), Error> {
        let table = self
            .get_table(table)?
            .ok_or(Error::TableDoesNotExist(table.to_owned()))?;
        for row in rows {
            if !table.validate_row(&row) {
                return Err(Error::InvalidRow(table.name.clone()));
            }
            let id = &row[*table.primary_key_index];

            let key = Key::Row(Cow::Borrowed(&table.name), Cow::Borrowed(id)).encode()?;
            let value = row.encode()?;
            self.0.set(&key, value)?;

            for (i, column) in table
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.has_secondary_index)
            {
                let mut ids = self.get_index(&table.name, &column.name, &row[i])?;
                ids.insert(id.clone());
                self.set_index(&table.name, &column.name, &row[i], ids)?;
            }
        }
        Ok(())
    }

    fn lookup_index(
        &self,
        table: &TableName,
        column: &ColumnName,
        values: &[Value],
    ) -> Result<BTreeSet<Value>, Error> {
        values
            .iter()
            .map(|v| self.get_index(table, column, v))
            .flatten_ok()
            .collect()
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
        let Some(table) = self.get_table(table)? else {
            return Err(Error::TableDoesNotExist(table.to_owned()));
        };

        // delete the table schema
        self.0
            .delete(&Key::Table(Cow::Borrowed(&table.name)).encode()?)?;

        // delete the rows
        let prefix = KeyPrefix::Row(Cow::Borrowed(&table.name)).encode()?;
        let elems: Vec<_> = self
            .0
            .scan_prefix(&prefix)?
            .map_ok(|r| r.0.to_vec())
            .try_collect()?;
        for key in elems {
            self.0.delete(&key)?;
        }

        // delete any secondary indices
        for column in &table.columns {
            if column.has_secondary_index {
                let prefix =
                    KeyPrefix::Index(Cow::Borrowed(&table.name), Cow::Borrowed(&column.name))
                        .encode()?;
                let mut keys = self.0.scan_prefix(&prefix)?.map_ok(|(key, _)| key);
                while let Some(key) = keys.next().transpose()? {
                    self.0.delete(&key)?;
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::memory::Memory, types::schema::ColumnIndex};

    #[test]
    fn test_local_engine() {
        let engine = Local::new(Memory::default());
        let txn = engine.begin().unwrap();
        let table = Table {
            name: TableName::new("test".to_string()),
            primary_key_index: ColumnIndex::new(0),
            columns: vec![],
        };
        txn.create_table(table).unwrap();
        txn.commit().unwrap();

        let txn = engine.begin().unwrap();
        let tables = txn.list_tables().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, TableName::new("test".to_string()));
        txn.commit().unwrap();

        let txn = engine.begin().unwrap();
        let table = txn.get_table(&TableName::new("test".to_string())).unwrap();
        assert!(table.is_some());
        let table = table.unwrap();
        assert_eq!(table.name, TableName::new("test".to_string()));
        assert_eq!(table.primary_key_index, ColumnIndex::new(0));
        assert_eq!(table.columns.len(), 0);
        txn.drop_table(&TableName::new("test".to_string())).unwrap();
        txn.commit().unwrap();

        let txn = engine.begin().unwrap();
        let table = txn.get_table(&TableName::new("test".to_string())).unwrap();
        assert!(table.is_none());
        txn.commit().unwrap();

        let txn = engine.begin().unwrap();
        let tables = txn.list_tables().unwrap();
        assert_eq!(tables.len(), 0);
        txn.commit().unwrap();
    }
}
