use itertools::Itertools;

use crate::{Error, Result, Row, RowIter, Table, Transaction};

use super::{Aggregate, Aggregator, Expr, JoinType, NestedLoopJoiner, Plan, StatementResult};

pub struct Executor<'a, T: Transaction> {
    txn: &'a T,
}

impl<'a, T: Transaction> Executor<'a, T> {
    pub fn new(txn: &'a T) -> Self {
        Self { txn }
    }

    pub fn execute(&mut self, plan: Plan) -> Result<StatementResult> {
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
                let source = self.execute_inner(*source)?;
                let count = self.insert(table, source)?;
                Ok(StatementResult::Insert(count))
            }
            Plan::Query(node) => {
                let mut columns = Vec::new();
                for i in 0..node.num_columns() {
                    let label = node.column_label(i);
                    columns.push(label.clone());
                }

                let rows = self.execute_inner(*node)?;

                Ok(StatementResult::Query {
                    rows: rows.try_collect()?,
                    columns,
                })
            }
            _ => Err(Error::InvalidPlan),
        }
    }

    fn insert(&mut self, table: Table, mut source: RowIter) -> Result<usize> {
        let mut rows = Vec::new();
        while let Some(values) = source.next().transpose()? {
            if !table.validate_row(&values) {
                return Err(Error::InvalidRow(table.name));
            }
            let mut casted_row = Vec::new();
            for (i, value) in values.iter().enumerate() {
                casted_row.push(value.try_cast(&table.columns[i].data_type)?);
            }
            rows.push(Row::from(casted_row));
        }

        let count = rows.len();
        self.txn.insert(&table.name, rows)?;
        Ok(count)
    }

    fn execute_inner(&mut self, plan: Plan) -> Result<RowIter> {
        match plan {
            Plan::Query(node) => self.execute_inner(*node),
            Plan::Values { rows } => self.execute_values(rows),
            Plan::Scan { table, filter, .. } => self.execute_scan(table, filter),
            Plan::Join {
                left,
                right,
                on,
                join_type,
            } => self.execute_join(*left, *right, join_type, on),
            Plan::Aggregate {
                source,
                group_by,
                aggregates,
            } => self.execute_aggregate(*source, group_by, aggregates),
            Plan::Filter { source, predicate } => self.execute_filter(*source, predicate),
            Plan::Project {
                source, columns, ..
            } => self.execute_project(*source, columns),
            Plan::Nothing { .. } => Ok(RowIter::new(std::iter::empty())),
            _ => Err(Error::InvalidPlan),
        }
    }

    fn execute_values(&mut self, rows: Vec<Vec<Expr>>) -> Result<RowIter> {
        let mut result = Vec::new();
        for row in rows {
            let mut values = Vec::new();
            for expr in row {
                values.push(expr.eval(None)?);
            }
            result.push(Row::from(values));
        }
        Ok(RowIter::new(result.into_iter().map(Ok)))
    }

    fn execute_scan(&mut self, table: Table, filter: Option<Expr>) -> Result<RowIter> {
        let rows = self.txn.scan(&table.name, filter)?;
        Ok(rows)
    }

    fn execute_join(
        &mut self,
        left: Plan,
        right: Plan,
        join_type: JoinType,
        on: Option<Expr>,
    ) -> Result<RowIter> {
        let left_cols = left.num_columns();
        let right_cols = right.num_columns();
        let left = self.execute_inner(left)?;
        let right = self.execute_inner(right)?;

        let joiner = NestedLoopJoiner::new(left, right, left_cols, right_cols, on, join_type);

        Ok(RowIter::new(joiner))
    }

    fn execute_aggregate(
        &mut self,
        source: Plan,
        group_by: Vec<Expr>,
        aggregates: Vec<Aggregate>,
    ) -> Result<RowIter> {
        let source = self.execute_inner(source)?;
        let mut aggregator = Aggregator::new(group_by, aggregates);
        for row in source {
            let row = row?;
            aggregator.add_row(&row)?;
        }
        let result = aggregator.finish()?;
        Ok(result)
    }

    fn execute_filter(&mut self, source: Plan, predicate: Expr) -> Result<RowIter> {
        let source = self.execute_inner(source)?;
        let mut result = Vec::new();
        for row in source {
            let row = row?;
            if predicate
                .eval(Some(&row))
                .map(|v| v.is_truthy())
                .unwrap_or(false)
            {
                result.push(row);
            }
        }
        Ok(RowIter::new(result.into_iter().map(Ok)))
    }

    fn execute_project(&mut self, source: Plan, columns: Vec<Expr>) -> Result<RowIter> {
        let source = self.execute_inner(source)?;
        let mut result = Vec::new();
        for row in source {
            let row = row?;
            let mut projected_row = Vec::new();
            for expr in &columns {
                projected_row.push(expr.eval(Some(&row))?);
            }
            result.push(Row::from(projected_row));
        }
        Ok(RowIter::new(result.into_iter().map(Ok)))
    }
}
