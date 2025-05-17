use plan::Plan;

use crate::engine::{EngineError, Transaction};

pub mod plan;
pub mod session;

pub struct Executor<'a, T: Transaction> {
    txn: &'a T,
}

impl<'a, T: Transaction> Executor<'a, T> {
    pub fn new(txn: &'a T) -> Self {
        Self { txn }
    }

    pub fn execute(&mut self, plan: Plan) -> Result<(), EngineError> {
        match plan {
            Plan::CreateTable(table) => {
                self.txn.create_table(table)?;
                Ok(())
            }
        }
    }
}
