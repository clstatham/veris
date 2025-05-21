use std::fmt;

use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{
    engine::{Catalog, Engine, Transaction},
    error::Error,
    types::{
        schema::{Table, TableName},
        value::{ColumnLabel, Row},
    },
};

use super::planner::Planner;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum StatementResult {
    Null,
    Error(String),
    Begin,
    Commit,
    Rollback,
    CreateTable(TableName),
    DropTable(TableName),
    ShowTables {
        tables: Vec<Table>,
    },
    Delete(usize),
    Insert(usize),
    Query {
        rows: Vec<Row>,
        columns: Vec<ColumnLabel>,
    },
}

impl fmt::Display for StatementResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementResult::Null => write!(f, "NULL"),
            StatementResult::Error(err) => write!(f, "Error: {}", err),
            StatementResult::Begin => write!(f, "Transaction started"),
            StatementResult::Commit => write!(f, "Transaction committed"),
            StatementResult::Rollback => write!(f, "Transaction rolled back"),
            StatementResult::CreateTable(name) => write!(f, "Created table {}", name),
            StatementResult::DropTable(name) => write!(f, "Dropped table {}", name),
            StatementResult::ShowTables { .. } => write!(f, "Showed tables"),
            StatementResult::Delete(count) => write!(f, "Deleted {} rows", count),
            StatementResult::Insert(count) => write!(f, "Inserted {} rows", count),
            StatementResult::Query { .. } => write!(f, "Query ran"),
        }
    }
}

pub struct Session<'a, E: Engine<'a>> {
    engine: &'a E,
    current_transaction: Option<E::Transaction>,
}

impl<'a, E: Engine<'a>> Session<'a, E> {
    pub fn new(engine: &'a E) -> Self {
        Self {
            engine,
            current_transaction: None,
        }
    }

    pub fn exec(&mut self, statement: &ast::Statement) -> Result<StatementResult, Error> {
        match statement {
            ast::Statement::StartTransaction { .. } => {
                self.begin()?;
                Ok(StatementResult::Begin)
            }
            ast::Statement::Commit { .. } => {
                self.commit()?;
                Ok(StatementResult::Commit)
            }
            ast::Statement::Rollback { .. } => {
                self.rollback()?;
                Ok(StatementResult::Rollback)
            }
            ast::Statement::ShowTables { .. } => {
                let tables = self.with_transaction(|t| t.list_tables())?;
                Ok(StatementResult::ShowTables { tables })
            }
            statement => self.with_transaction(|t| Planner::new(t).plan(statement)?.execute(t)),
        }
    }

    pub fn with_transaction<F, R>(&mut self, f: F) -> Result<R, Error>
    where
        F: FnOnce(&mut E::Transaction) -> Result<R, Error>,
    {
        if let Some(txn) = self.current_transaction.as_mut() {
            return f(txn);
        }

        // implicitly start a transaction
        let mut txn = self.engine.begin()?;
        let res = f(&mut txn);
        match res {
            Ok(_) => txn.commit()?,
            Err(_) => txn.rollback()?,
        }
        res
    }

    pub fn begin(&mut self) -> Result<(), Error> {
        if self.current_transaction.is_some() {
            return Err(Error::AlreadyInTransaction);
        }
        self.current_transaction = Some(self.engine.begin()?);
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        if let Some(transaction) = self.current_transaction.take() {
            transaction.commit()?;
        } else {
            return Err(Error::NotInTransaction);
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), Error> {
        if let Some(transaction) = self.current_transaction.take() {
            transaction.rollback()?;
        } else {
            return Err(Error::NotInTransaction);
        }
        Ok(())
    }
}
