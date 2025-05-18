use sqlparser::ast;

use crate::{
    error::Error,
    types::{
        schema::{ColumnIndex, ColumnName},
        value::{Row, Value},
    },
};

use super::scope::Scope;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Expr {
    Constant(Value),
    Column(ColumnIndex),
    Ident(String),
    Equal(Box<Expr>, Box<Expr>),
}

impl Expr {
    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value, Error> {
        match self {
            Expr::Constant(value) => Ok(value.clone()),
            Expr::Column(index) => {
                if let Some(row) = row {
                    row.get(*index.inner())
                        .cloned()
                        .ok_or(Error::InvalidColumnIndex(index.clone()))
                } else {
                    Err(Error::RowNotFound)
                }
            }
            Expr::Equal(lhs, rhs) => {
                let lhs_value = lhs.evaluate(row)?;
                let rhs_value = rhs.evaluate(row)?;
                if lhs_value == rhs_value {
                    Ok(Value::Boolean(true))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
            Expr::Ident(ident) => Ok(Value::String(ident.clone())),
        }
    }

    pub fn build(expr: &ast::Expr, scope: &Scope) -> Result<Self, Error> {
        match expr {
            ast::Expr::Value(v) => Ok(Expr::Constant(Value::try_from(&v.value)?)),
            ast::Expr::BinaryOp { left, op, right } => match op {
                ast::BinaryOperator::Eq => Ok(Expr::Equal(
                    Box::new(Expr::build(left, scope)?),
                    Box::new(Expr::build(right, scope)?),
                )),
                _ => Err(Error::NotYetSupported(expr.to_string())),
            },
            ast::Expr::Identifier(ident) => {
                let column = scope
                    .get_columm_index(None, &ColumnName::new(ident.value.clone()))
                    .ok_or(Error::ColumnNotFound(ident.value.clone()))?;
                Ok(Expr::Column(column))
            }
            _ => Err(Error::NotYetSupported(expr.to_string())),
        }
    }
}
