use itertools::Itertools;
use plan::{Node, Plan};
use session::StatementResult;

use crate::{
    engine::Transaction,
    error::Error,
    types::{
        schema::{ColumnIndex, Table, TableName},
        value::{Rows, Value},
    },
};

pub mod expr;
pub mod plan;
pub mod scope;
pub mod session;

pub struct Executor<'a, T: Transaction> {
    txn: &'a T,
}

impl<'a, T: Transaction> Executor<'a, T> {
    pub fn new(txn: &'a T) -> Self {
        Self { txn }
    }

    pub fn execute(&mut self, plan: Plan) -> Result<StatementResult, Error> {
        match plan {
            Plan::CreateTable(table) => {
                let name = table.name.clone();
                self.txn.create_table(table)?;
                Ok(StatementResult::CreateTable(name))
            }
            Plan::DropTable(table) => {
                self.txn.drop_table(&table)?;
                Ok(StatementResult::DropTable(table))
            }
            Plan::Insert { table, source } => {
                let source = self.execute_node(source)?;
                let count = self.insert(table, source)?;
                Ok(StatementResult::Insert(count))
            }
            Plan::Delete {
                table,
                primary_key,
                source,
            } => {
                let source = self.execute_node(source)?;
                let count = self.delete(table, primary_key, source)?;
                Ok(StatementResult::Delete(count))
            }
            Plan::Select(node) => {
                let mut columns = Vec::new();
                for col in 0..node.num_columns() {
                    columns.push(node.column_label(&ColumnIndex::new(col)));
                }
                let rows = self.execute_node(node)?;

                Ok(StatementResult::Select {
                    rows: rows.try_collect()?,
                    columns,
                })
            }
        }
    }

    fn execute_node(&mut self, node: Node) -> Result<Rows, Error> {
        match node {
            Node::Values { rows } => {
                Ok(Box::new(rows.into_iter().map(|row| {
                    row.into_iter().map(|expr| expr.evaluate(None)).collect()
                })))
            }
            Node::Scan { table, filter } => Ok(self.txn.scan(&table.name, filter)?),
            Node::Filter { source, predicate } => {
                let source = self.execute_node(*source)?;
                let mut rows = Vec::new();
                for source_row in source {
                    match source_row {
                        Ok(source_row) => {
                            let result = predicate.evaluate(Some(&source_row))?;
                            match result {
                                Value::Boolean(true) => rows.push(Ok(source_row)),
                                Value::Boolean(false) => continue,
                                result => {
                                    return Err(Error::InvalidFilterResult(result));
                                }
                            }
                        }
                        Err(e) => rows.push(Err(e)),
                    }
                }

                Ok(Box::new(rows.into_iter()))
            }
            Node::Project {
                source,
                expressions,
            } => {
                let source = self.execute_node(*source)?;
                Ok(Box::new(source.into_iter().map(move |res| {
                    let row = res?;
                    expressions
                        .iter()
                        .map(|expr| expr.evaluate(Some(&row)))
                        .collect()
                })))
            }
        }
    }

    fn insert(&mut self, table: Table, mut source: Rows) -> Result<usize, Error> {
        let mut rows = Vec::new();
        while let Some(values) = source.next().transpose()? {
            if values.len() == table.columns.len() {
                rows.push(values);
                continue;
            }
            if values.len() > table.columns.len() {
                return Err(Error::TooManyValues(table.name.clone()));
            }
        }

        let count = rows.len();
        self.txn.insert(&table.name, rows.into_boxed_slice())?;
        Ok(count)
    }

    fn delete(
        &mut self,
        table: TableName,
        primary_key: ColumnIndex,
        source: Rows,
    ) -> Result<usize, Error> {
        let ids: Vec<Value> = source
            .into_iter()
            .map_ok(|row| {
                row.into_iter()
                    .nth(*primary_key.inner())
                    .ok_or(Error::InvalidRowState)
            })
            .flatten_ok()
            .try_collect()?;

        let count = ids.len();
        self.txn.delete(&table, &ids)?;
        Ok(count)
    }
}
