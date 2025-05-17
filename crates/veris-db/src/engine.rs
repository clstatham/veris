use derive_more::Display;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    types::schema::{Table, TableName},
    wrap,
};

pub mod debug;

wrap! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Display)]
    pub struct TransactionId(u64);
}

#[derive(Debug, Error, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EngineError {
    #[error("Not in a transaction")]
    NotInTransaction,
    #[error("Transaction already in progress")]
    AlreadyInTransaction,
    #[error("Error converting from AST: {0}")]
    FromAstError(String),
    #[error("Statement {0} not yet supported")]
    NotYetSupported(String),
}

pub trait Catalog {
    fn create_table(&self, table: Table) -> Result<(), EngineError>;
    fn drop_table(&self, table: &TableName) -> Result<(), EngineError>;
    fn get_table(&self, table: &TableName) -> Result<Option<Table>, EngineError>;
    fn list_tables(&self) -> Result<impl Iterator<Item = Table>, EngineError>;
}

pub trait Transaction: Catalog {
    fn commit(self) -> Result<(), EngineError>;
    fn rollback(self) -> Result<(), EngineError>;
}

pub trait Engine<'a> {
    type Transaction: Transaction + 'a;

    fn begin(&'a self) -> Result<Self::Transaction, EngineError>;
}
