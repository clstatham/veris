use derive_more::Display;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{
    engine::{Catalog, Engine, Transaction},
    error::Error,
    types::{
        schema::{Table, TableName},
        value::Row,
    },
};

use super::{ExecResult, plan::Planner};

#[derive(Debug, PartialEq, Serialize, Deserialize, Display)]
pub enum StatementResult {
    Null,
    Begin,
    Commit,
    Rollback,
    CreateTable(TableName),
    DropTable(TableName),
    #[display("{:?}", 0)]
    ShowTables(Vec<Table>),
    Delete(usize),
    Insert(usize),
    #[display("{:?}", rows)]
    Select {
        rows: Vec<Row>,
    },
}

impl TryFrom<ExecResult> for StatementResult {
    type Error = Error;

    fn try_from(result: ExecResult) -> Result<Self, Self::Error> {
        match result {
            ExecResult::Null => Ok(StatementResult::Begin),
            ExecResult::Table(name) => Ok(StatementResult::CreateTable(name)),
            ExecResult::Rows(rows) => Ok(StatementResult::Select {
                rows: rows.into_iter().try_collect()?,
            }),
            ExecResult::Count(count) => Ok(StatementResult::Insert(count)),
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
                Ok(StatementResult::ShowTables(tables))
            }
            statement => self
                .with_transaction(|t| Planner::new(t).plan(statement)?.execute(t))?
                .try_into(),
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
