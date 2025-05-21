use std::collections::BTreeSet;

use crate::{
    error::Error,
    exec::expr::Expr,
    types::{
        schema::Table,
        value::{Row, RowIter, Rows, Value},
    },
};

pub mod local;

pub trait Catalog {
    fn create_table(&self, table: Table) -> Result<(), Error>;
    fn drop_table(&self, table: &str) -> Result<(), Error>;
    fn get_table(&self, table: &str) -> Result<Option<Table>, Error>;
    fn list_tables(&self) -> Result<Vec<Table>, Error>;
}

pub trait Transaction: Catalog {
    fn commit(self) -> Result<(), Error>;
    fn rollback(self) -> Result<(), Error>;

    fn delete(&self, table: &str, ids: impl Into<Row>) -> Result<(), Error>;
    fn get(&self, table: &str, ids: impl Into<Row>) -> Result<Box<[Row]>, Error>;
    fn insert(&self, table: &str, rows: impl Into<Rows>) -> Result<(), Error>;
    fn scan(&self, table: &str, filter: Option<Expr>) -> Result<RowIter, Error>;
    fn lookup_index(
        &self,
        table: &str,
        column: &str,
        values: &[Value],
    ) -> Result<BTreeSet<Value>, Error>;
}

pub trait Engine {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction, Error>;
}
