use crate::{
    error::Error,
    exec::expr::Expr,
    types::{
        schema::{Table, TableName},
        value::{Row, Rows, Value},
    },
};

use super::{Catalog, Engine, Transaction};

pub struct DebugEngine;

impl<'a> Engine<'a> for DebugEngine {
    type Transaction = DebugTransaction;

    fn begin(&self) -> Result<Self::Transaction, Error> {
        log::debug!("begin()");
        Ok(DebugTransaction)
    }
}

pub struct DebugTransaction;

impl Catalog for DebugTransaction {
    fn create_table(&self, table: Table) -> Result<(), Error> {
        log::debug!("create_table(): {table:?}");
        Ok(())
    }

    fn drop_table(&self, table: &TableName) -> Result<(), Error> {
        log::debug!("drop_table(): {table:?}");
        Ok(())
    }

    fn get_table(&self, table: &TableName) -> Result<Option<Table>, Error> {
        log::debug!("get_table(): {table:?}");
        Ok(None)
    }

    fn list_tables(&self) -> Result<impl Iterator<Item = Table>, Error> {
        log::debug!("list_tables()");
        Ok(std::iter::empty())
    }
}

impl Transaction for DebugTransaction {
    fn commit(self) -> Result<(), Error> {
        log::debug!("commit()");
        Ok(())
    }

    fn rollback(self) -> Result<(), Error> {
        log::debug!("rollback()");
        Ok(())
    }

    fn delete(&self, table: &TableName, ids: &[Value]) -> Result<(), Error> {
        log::debug!("delete(): table={table:?}, ids={ids:?}");
        Ok(())
    }

    fn get(&self, table: &TableName, ids: &[Value]) -> Result<Box<[Row]>, Error> {
        log::debug!("get(): table={table:?}, ids={ids:?}");
        Ok(Box::new([]))
    }

    fn insert(&self, table: &TableName, rows: Box<[Row]>) -> Result<(), Error> {
        log::debug!("insert(): table={table:?}, rows={rows:?}");
        Ok(())
    }

    fn scan(&self, table: &TableName, filter: Option<Expr>) -> Result<Rows, Error> {
        log::debug!("scan(): table={table:?}, filter={filter:?}");
        Ok(Box::new(std::iter::empty()))
    }
}
