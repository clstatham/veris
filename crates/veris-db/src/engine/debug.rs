use crate::types::schema::{Table, TableName};

use super::{Catalog, Engine, EngineError, Transaction};

pub struct DebugEngine;

impl<'a> Engine<'a> for DebugEngine {
    type Transaction = DebugTransaction;

    fn begin(&self) -> Result<Self::Transaction, EngineError> {
        Ok(DebugTransaction)
    }
}

pub struct DebugTransaction;

impl Catalog for DebugTransaction {
    fn create_table(&self, table: Table) -> Result<(), EngineError> {
        log::debug!("create_table(): {table:?}");
        Ok(())
    }

    fn drop_table(&self, table: &TableName) -> Result<(), EngineError> {
        log::debug!("drop_table(): {table:?}");
        Ok(())
    }

    fn get_table(&self, table: &TableName) -> Result<Option<Table>, EngineError> {
        log::debug!("get_table(): {table:?}");
        Ok(None)
    }

    fn list_tables(&self) -> Result<impl Iterator<Item = Table>, EngineError> {
        log::debug!("list_tables()");
        Ok(std::iter::empty())
    }
}

impl Transaction for DebugTransaction {
    fn commit(self) -> Result<(), EngineError> {
        log::debug!("commit()");
        Ok(())
    }

    fn rollback(self) -> Result<(), EngineError> {
        log::debug!("rollback()");
        Ok(())
    }
}
