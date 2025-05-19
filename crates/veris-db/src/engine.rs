use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::{
    error::Error,
    exec::expr::Expr,
    types::{
        schema::{Table, TableName},
        value::{Row, Rows, Value},
    },
    wrap,
};

pub mod debug;
pub mod local;
pub mod mvcc;

wrap! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Display)]
    pub struct TransactionId(u64);
}

pub trait Catalog {
    fn create_table(&self, table: Table) -> Result<(), Error>;
    fn drop_table(&self, table: &TableName) -> Result<(), Error>;
    fn get_table(&self, table: &TableName) -> Result<Option<Table>, Error>;
    fn list_tables(&self) -> Result<Vec<Table>, Error>;
}

pub trait Transaction: Catalog {
    fn commit(self) -> Result<(), Error>;
    fn rollback(self) -> Result<(), Error>;

    fn delete(&self, table: &TableName, ids: &[Value]) -> Result<(), Error>;
    fn get(&self, table: &TableName, ids: &[Value]) -> Result<Box<[Row]>, Error>;
    fn insert(&self, table: &TableName, rows: Box<[Row]>) -> Result<(), Error>;
    fn scan(&self, table: &TableName, filter: Option<Expr>) -> Result<Rows, Error>;
}

pub trait Engine<'a> {
    type Transaction: Transaction + 'a;

    fn begin(&'a self) -> Result<Self::Transaction, Error>;
}
