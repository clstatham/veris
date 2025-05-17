use sqlparser::ast;

use crate::{
    engine::{Catalog, EngineError, Transaction},
    types::schema::Table,
};

use super::Executor;

pub struct Planner<'a, C: Catalog> {
    catalog: &'a C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    pub fn new(catalog: &'a C) -> Self {
        Self { catalog }
    }

    pub fn plan(&self, statement: &ast::Statement) -> Result<Plan, EngineError> {
        match statement {
            ast::Statement::CreateTable(stmt) => self.plan_create_table(stmt),
            stmt => Err(EngineError::NotYetSupported(stmt.to_string())),
        }
    }

    fn plan_create_table(&self, table: &ast::CreateTable) -> Result<Plan, EngineError> {
        let table = Table::try_from(table).map_err(|e| EngineError::FromAstError(e.to_string()))?;
        Ok(Plan::CreateTable(table))
    }
}

pub enum Plan {
    CreateTable(Table),
}

impl Plan {
    pub fn execute(self, txn: &impl Transaction) -> Result<(), EngineError> {
        Executor::new(txn).execute(self)
    }
}
