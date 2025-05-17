use sqlparser::ast;

use crate::{
    engine::{Engine, EngineError, Transaction},
    types::value::Value,
};

use super::plan::Planner;

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

    pub fn exec(&mut self, statement: &ast::Statement) -> Result<Value, EngineError> {
        match statement {
            ast::Statement::StartTransaction { .. } => {
                self.begin()?;
            }
            ast::Statement::Commit { .. } => {
                self.commit()?;
            }
            ast::Statement::Rollback { .. } => {
                self.rollback()?;
            }
            statement => {
                return self.with_transaction(|t| Planner::new(t).plan(statement)?.execute(t));
            }
        }

        Ok(Value::Null)
    }

    pub fn with_transaction<F, R>(&mut self, f: F) -> Result<R, EngineError>
    where
        F: FnOnce(&mut E::Transaction) -> Result<R, EngineError>,
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

    pub fn begin(&mut self) -> Result<(), EngineError> {
        if self.current_transaction.is_some() {
            return Err(EngineError::AlreadyInTransaction);
        }
        self.current_transaction = Some(self.engine.begin()?);
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), EngineError> {
        if let Some(transaction) = self.current_transaction.take() {
            transaction.commit()?;
        } else {
            return Err(EngineError::NotInTransaction);
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), EngineError> {
        if let Some(transaction) = self.current_transaction.take() {
            transaction.rollback()?;
        } else {
            return Err(EngineError::NotInTransaction);
        }
        Ok(())
    }
}
