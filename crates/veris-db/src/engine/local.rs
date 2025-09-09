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
        schema::Table,
        value::{Row, RowIter, Value},
    },
};

use super::{Catalog, Engine, Transaction};

#[derive(Debug, Serialize, Deserialize)]
pub enum Key<'a> {
    Table(Cow<'a, str>),
    Index(Cow<'a, str>, Cow<'a, str>, Cow<'a, Value>),
    Row(Cow<'a, str>, Cow<'a, Value>),
}

impl<'a> KeyEncoding<'a> for Key<'a> {}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyPrefix<'a> {
    Table,
    Index(Cow<'a, str>, Cow<'a, str>),
    Row(Cow<'a, str>),
}

impl<'a> KeyEncoding<'a> for KeyPrefix<'a> {}

pub struct Local<E: StorageEngine>(Mvcc<E>);

impl<E: StorageEngine> Local<E> {
    pub fn new(engine: E) -> Self {
        Self(Mvcc::new(engine))
    }
}

impl<E: StorageEngine + 'static> Engine for Local<E> {
    type Transaction = LocalTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction, Error> {
        Ok(LocalTransaction(self.0.begin()?))
    }
}

pub struct LocalTransaction<E: StorageEngine>(MvccTransaction<E>);

impl<E: StorageEngine> LocalTransaction<E> {
    fn get_row(&self, table: &str, id: &Value) -> Result<Option<Row>, Error> {
        let key = Key::Row(Cow::Borrowed(table), Cow::Borrowed(id)).encode()?;
        if let Some(row) = self.0.get(&key)? {
            return Ok(Some(Row::decode(&row)?));
        }
        Ok(None)
    }

    fn get_index(
        &self,
        table: &str,
        column: &str,
        value: &Value,
    ) -> Result<BTreeSet<Value>, Error> {
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
        table: &str,
        column: &str,
        value: &Value,
        ids: &BTreeSet<Value>,
    ) -> Result<(), Error> {
        let key = Key::Index(
            Cow::Borrowed(table),
            Cow::Borrowed(column),
            Cow::Borrowed(value),
        )
        .encode()?;
        if ids.is_empty() {
            self.0.delete(&key)?;
        } else {
            self.0.set(&key, &ids.encode()?)?;
        }

        Ok(())
    }

    fn table_refs(&self, referenced_table: &String) -> Result<Vec<(Table, Vec<usize>)>, Error> {
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
                .map(|(i, _)| i)
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

    fn delete(&self, table: &str, ids: impl AsRef<[Value]>) -> Result<(), Error> {
        let ids = ids.as_ref();
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
                let column = &source.columns[i];
                let mut source_ids: BTreeSet<Value> = if i == source.primary_key_index {
                    self.get(&source.name, ids)?
                        .into_iter()
                        .map(|row| row.into_iter().nth(i).ok_or(Error::InvalidRowState))
                        .try_collect()?
                } else {
                    self.lookup_index(&source.name, &column.name, ids)?
                };

                if self_reference {
                    for id in ids.iter() {
                        source_ids.remove(id);
                    }
                }

                if let Some(source_id) = source_ids.first() {
                    let table = source.name.clone();
                    let column = source.columns[source.primary_key_index].name.clone();
                    return Err(Error::ReferentialIntegrity {
                        table,
                        column,
                        source_id: source_id.clone(),
                    });
                }
            }
        }

        for id in ids {
            if !indices.is_empty()
                && let Some(row) = self.get_row(&table.name, id)?
            {
                for (i, column) in indices.iter().copied() {
                    let mut ids = self.get_index(&table.name, &column.name, &row[i])?;
                    ids.remove(id);
                    self.set_index(&table.name, &column.name, &row[i], &ids)?;
                }
            }

            self.0
                .delete(&Key::Row(Cow::Borrowed(&table.name), Cow::Borrowed(id)).encode()?)?;
        }
        Ok(())
    }

    fn get(&self, table: &str, ids: impl AsRef<[Value]>) -> Result<Box<[Row]>, Error> {
        let ids = ids.as_ref();
        ids.iter()
            .filter_map(|id| self.get_row(table, id).transpose())
            .collect()
    }

    fn insert(&self, table: &str, rows: impl AsRef<[Row]>) -> Result<(), Error> {
        let rows = rows.as_ref();
        let table = self
            .get_table(table)?
            .ok_or(Error::TableDoesNotExist(table.to_owned()))?;
        for row in rows.iter() {
            if !table.validate_row(row) {
                return Err(Error::InvalidRow(table.name));
            }
            let id = &row[table.primary_key_index];

            let key = Key::Row(Cow::Borrowed(&table.name), Cow::Borrowed(id)).encode()?;
            self.0.set(&key, &row.encode()?)?;

            for (i, column) in table
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.has_secondary_index)
            {
                let mut ids = self.get_index(&table.name, &column.name, &row[i])?;
                ids.insert(id.clone());
                self.set_index(&table.name, &column.name, &row[i], &ids)?;
            }
        }
        Ok(())
    }

    fn lookup_index(
        &self,
        table: &str,
        column: &str,
        values: &[Value],
    ) -> Result<BTreeSet<Value>, Error> {
        values
            .iter()
            .map(|v| self.get_index(table, column, v))
            .flatten_ok()
            .collect()
    }

    fn scan(&self, table: &str, filter: Option<Expr>) -> Result<RowIter, Error> {
        let key = KeyPrefix::Row(Cow::Borrowed(table)).encode()?;
        let rows = self
            .0
            .scan_prefix(&key)?
            .map(|res| res.and_then(|(_, value)| Row::decode(&value)));

        let Some(filter) = filter else {
            return Ok(RowIter::new(rows));
        };
        let rows = rows.filter_map(move |res| {
            res.and_then(|row| match filter.eval(Some(&row))? {
                Value::Boolean(true) => Ok(Some(row)),
                Value::Boolean(false) => Ok(None),
                value => Err(Error::InvalidFilterResult(value)),
            })
            .transpose()
        });
        Ok(RowIter::new(rows))
    }
}

impl<E: StorageEngine> Catalog for LocalTransaction<E> {
    fn create_table(&self, table: Table) -> Result<(), Error> {
        let table_key = Key::Table(Cow::Borrowed(&table.name)).encode()?;
        if self.0.get(&table_key)?.is_some() {
            return Err(Error::TableAlreadyExists(table.name));
        }
        self.0.set(&table_key, &table.encode()?)?;
        Ok(())
    }

    fn drop_table(&self, table: &str) -> Result<(), Error> {
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

    fn get_table(&self, table: &str) -> Result<Option<Table>, Error> {
        let table_key = Key::Table(Cow::Borrowed(table)).encode()?;
        if let Some(table) = self.0.get(&table_key)? {
            return Ok(Some(Table::decode(&table)?));
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
    use std::io::Cursor;

    use super::*;
    use crate::*;

    fn create_test_engine() -> Local<Bitcask<Cursor<Vec<u8>>>> {
        let engine = Bitcask::new(Cursor::new(Vec::new())).unwrap();
        Local::new(engine)
    }

    fn create_test_table() -> Table {
        Table {
            name: "test".to_owned(),
            primary_key_index: 0,
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    data_type: DataType::Integer,
                    references: None,
                    has_secondary_index: false,
                    nullable: false,
                },
                Column {
                    name: "name".to_owned(),
                    data_type: DataType::String { length: None },
                    references: None,
                    has_secondary_index: true,
                    nullable: false,
                },
            ],
        }
    }

    #[test]
    fn test_create_table() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();
        let tables = tx.list_tables().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, table.name);
        assert_eq!(tables[0].columns.len(), table.columns.len());
    }

    #[test]
    fn test_drop_table() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();
        tx.drop_table(&table.name).unwrap();
        let tables = tx.list_tables().unwrap();
        assert_eq!(tables.len(), 0);
    }

    #[test]
    fn test_insert() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let rows = vec![
            Row::from(vec![Value::Integer(1), Value::String("Alice".to_owned())]),
            Row::from(vec![Value::Integer(2), Value::String("Bob".to_owned())]),
        ];
        tx.insert(&table.name, rows).unwrap();

        let result = tx.get(&table.name, vec![Value::Integer(1)]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Integer(1));
        assert_eq!(result[0][1], Value::String("Alice".to_owned()));
    }

    #[test]
    fn test_delete() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let rows = vec![
            Row::from(vec![Value::Integer(1), Value::String("Alice".to_owned())]),
            Row::from(vec![Value::Integer(2), Value::String("Bob".to_owned())]),
        ];
        tx.insert(&table.name, rows).unwrap();

        tx.delete(&table.name, vec![Value::Integer(1)]).unwrap();
        let result = tx.get(&table.name, vec![Value::Integer(1)]).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_scan() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let rows = vec![
            Row::from(vec![Value::Integer(1), Value::String("Alice".to_owned())]),
            Row::from(vec![Value::Integer(2), Value::String("Bob".to_owned())]),
        ];
        tx.insert(&table.name, rows).unwrap();

        let result = tx.scan(&table.name, None).unwrap();
        assert_eq!(result.count(), 2);
    }

    #[test]
    fn test_get() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let rows = vec![
            Row::from(vec![Value::Integer(1), Value::String("Alice".to_owned())]),
            Row::from(vec![Value::Integer(2), Value::String("Bob".to_owned())]),
        ];
        tx.insert(&table.name, rows).unwrap();

        let result = tx.get(&table.name, vec![Value::Integer(1)]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Integer(1));
        assert_eq!(result[0][1], Value::String("Alice".to_owned()));
    }

    #[test]
    fn test_get_index() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let rows = vec![
            Row::from(vec![Value::Integer(1), Value::String("Alice".to_owned())]),
            Row::from(vec![Value::Integer(2), Value::String("Bob".to_owned())]),
        ];
        tx.insert(&table.name, rows).unwrap();

        let result = tx
            .get_index(&table.name, "name", &Value::String("Alice".to_owned()))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.iter().next().unwrap(), &Value::Integer(1));
    }

    #[test]
    fn test_set_index() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let rows = vec![
            Row::from(vec![Value::Integer(1), Value::String("Alice".to_owned())]),
            Row::from(vec![Value::Integer(2), Value::String("Bob".to_owned())]),
        ];
        tx.insert(&table.name, rows).unwrap();

        let mut ids = BTreeSet::new();
        ids.insert(Value::Integer(1));
        tx.set_index(
            &table.name,
            "name",
            &Value::String("Alice".to_owned()),
            &ids,
        )
        .unwrap();

        let result = tx
            .get_index(&table.name, "name", &Value::String("Alice".to_owned()))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.iter().next().unwrap(), &Value::Integer(1));
    }

    #[test]
    fn test_table_refs() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let refs = tx.table_refs(&table.name).unwrap();
        assert_eq!(refs.len(), 0);
    }

    #[test]
    fn test_list_tables() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let tables = tx.list_tables().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, table.name);
    }

    #[test]
    fn test_get_table() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();

        let result = tx.get_table(&table.name).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, table.name);
    }

    #[test]
    fn test_get_table_not_found() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let result = tx.get_table("non_existent_table");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_create_table_already_exists() {
        let engine = create_test_engine();
        let tx = engine.begin().unwrap();
        let table = create_test_table();
        tx.create_table(table.clone()).unwrap();
        let result = tx.create_table(table);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::TableAlreadyExists("test".to_owned())
        );
    }
}
