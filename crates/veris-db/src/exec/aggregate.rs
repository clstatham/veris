use std::collections::BTreeMap;

use derive_more::Display;
use itertools::Itertools;
use sqlparser::ast;

use crate::{
    error::Error,
    types::value::{Row, Rows, Value},
};

use super::expr::Expr;

pub struct Aggregator {
    group_by: Vec<Expr>,
    aggregates: Vec<Aggregate>,
    groups: BTreeMap<Vec<Value>, Vec<Accumulator>>,
}

impl Aggregator {
    pub fn new(group_by: Vec<Expr>, aggregates: Vec<Aggregate>) -> Self {
        Self {
            group_by,
            aggregates,
            groups: BTreeMap::new(),
        }
    }

    pub fn add_row(&mut self, row: &Row) -> Result<(), Error> {
        let key = self
            .group_by
            .iter()
            .map(|expr| expr.eval(Some(row)))
            .collect::<Result<Vec<_>, _>>()?;

        let accumulators = self
            .groups
            .entry(key)
            .or_insert_with(|| self.aggregates.iter().map(Accumulator::new).collect());

        for (accumulator, aggregate) in accumulators.iter_mut().zip(&self.aggregates) {
            accumulator.add_value(aggregate.expr().eval(Some(row))?)?;
        }

        Ok(())
    }

    pub fn finish(self) -> Result<Rows, Error> {
        let groups = self.groups.into_iter().collect::<Vec<_>>();
        Ok(Box::new(groups.into_iter().map(|(keys, accums)| {
            keys.into_iter()
                .map(Ok)
                .chain(accums.into_iter().map(|accum| accum.value()))
                .try_collect()
        })))
    }
}

#[derive(Clone)]
pub enum Accumulator {
    Average { count: i64, sum: Value },
    Count(i64),
    Max(Option<Value>),
    Min(Option<Value>),
    Sum(Option<Value>),
}

impl Accumulator {
    pub fn new(aggregate: &Aggregate) -> Self {
        match aggregate {
            Aggregate::Average(_) => Self::Average {
                count: 0,
                sum: Value::Integer(0),
            },
            Aggregate::Count(_) => Self::Count(0),
            Aggregate::Max(_) => Self::Max(None),
            Aggregate::Min(_) => Self::Min(None),
            Aggregate::Sum(_) => Self::Sum(None),
        }
    }

    pub fn add_value(&mut self, value: Value) -> Result<(), Error> {
        if value == Value::Null {
            return Ok(());
        }

        match self {
            Self::Average { count, sum } => {
                *sum = sum.checked_add(&value)?;
                *count += 1;
            }
            Self::Count(count) => *count += 1,
            Self::Max(max @ None) => *max = Some(value),
            Self::Max(Some(max)) => {
                if value > *max {
                    *max = value;
                }
            }
            Self::Min(min @ None) => *min = Some(value),
            Self::Min(Some(min)) => {
                if value < *min {
                    *min = value;
                }
            }
            Self::Sum(sum @ None) => *sum = Some(Value::Integer(0).checked_add(&value)?),
            Self::Sum(Some(sum)) => *sum = sum.checked_add(&value)?,
        }

        Ok(())
    }

    pub fn value(self) -> Result<Value, Error> {
        match self {
            Self::Average { count: 0, .. } => Ok(Value::Null),
            Self::Average { count, sum } => Ok(sum.checked_div(&Value::Integer(count))?),
            Self::Count(count) => Ok(Value::Integer(count)),
            Self::Max(Some(value)) | Self::Min(Some(value)) | Self::Sum(Some(value)) => Ok(value),
            Self::Max(None) | Self::Min(None) | Self::Sum(None) => Ok(Value::Null),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Display)]
pub enum Aggregate {
    Average(Expr),
    Count(Expr),
    Max(Expr),
    Min(Expr),
    Sum(Expr),
}

impl Aggregate {
    pub fn expr(&self) -> &Expr {
        match self {
            Self::Average(expr)
            | Self::Count(expr)
            | Self::Max(expr)
            | Self::Min(expr)
            | Self::Sum(expr) => expr,
        }
    }
}

pub fn is_aggregate(func: &ast::Function) -> bool {
    matches!(
        func.name.to_string().to_lowercase().as_str(),
        "avg" | "count" | "max" | "min" | "sum"
    )
}

pub fn aggregate_function_args(func: &ast::Function) -> Result<Vec<ast::Expr>, Error> {
    match &func.args {
        ast::FunctionArguments::None => Ok(vec![]),
        ast::FunctionArguments::List(func_args) => {
            let mut args = Vec::new();
            for arg in func_args.args.iter() {
                if let ast::FunctionArg::Unnamed(arg) = arg {
                    if let ast::FunctionArgExpr::Expr(expr) = arg {
                        args.push(expr.clone());
                    } else {
                        return Err(Error::NotYetSupported(
                            "Aggregate function with named arguments".to_string(),
                        ));
                    }
                }
            }
            Ok(args)
        }
        ast::FunctionArguments::Subquery(_) => Err(Error::NotYetSupported(
            "Aggregate function with subquery".to_string(),
        )),
    }
}
