use sqlparser::ast;

use crate::{
    error::Error,
    types::{
        schema::{ColumnIndex, ColumnName, TableName},
        value::{Row, Value},
    },
};

use super::scope::Scope;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Expr {
    Constant(Value),
    Column(ColumnIndex),
    Equal(Box<Expr>, Box<Expr>),
    NotEqual(Box<Expr>, Box<Expr>),
    Greater(Box<Expr>, Box<Expr>),
    GreaterEqual(Box<Expr>, Box<Expr>),
    Less(Box<Expr>, Box<Expr>),
    LessEqual(Box<Expr>, Box<Expr>),
}

impl Expr {
    pub fn build(expr: &ast::Expr, scope: &Scope) -> Result<Self, Error> {
        match expr {
            ast::Expr::Value(v) => Ok(Expr::Constant(Value::try_from_ast(&v.value, None)?)),
            ast::Expr::BinaryOp { left, op, right } => {
                let lhs = Box::new(Expr::build(left, scope)?);
                let rhs = Box::new(Expr::build(right, scope)?);
                match op {
                    ast::BinaryOperator::Eq => Ok(Expr::Equal(lhs, rhs)),
                    ast::BinaryOperator::NotEq => Ok(Expr::NotEqual(lhs, rhs)),
                    ast::BinaryOperator::Gt => Ok(Expr::Greater(lhs, rhs)),
                    ast::BinaryOperator::GtEq => Ok(Expr::GreaterEqual(lhs, rhs)),
                    ast::BinaryOperator::Lt => Ok(Expr::Less(lhs, rhs)),
                    ast::BinaryOperator::LtEq => Ok(Expr::LessEqual(lhs, rhs)),
                    _ => Err(Error::NotYetSupported(expr.to_string())),
                }
            }
            ast::Expr::Identifier(ident) => {
                let column = scope
                    .get_column_index(None, &ColumnName::new(ident.value.clone()))
                    .ok_or(Error::ColumnNotFound(ident.value.clone()))?;
                Ok(Expr::Column(column))
            }
            ast::Expr::CompoundIdentifier(idents) => {
                assert_eq!(idents.len(), 2);
                let column = scope
                    .get_column_index(
                        Some(&TableName::new(idents[0].value.clone())),
                        &ColumnName::new(idents[1].value.clone()),
                    )
                    .ok_or(Error::ColumnNotFound(format!(
                        "{}.{}",
                        &idents[0].value, &idents[1].value
                    )))?;

                Ok(Expr::Column(column))
            }
            _ => Err(Error::NotYetSupported(expr.to_string())),
        }
    }

    pub fn evaluate(&self, row: Option<&Row>) -> Result<Value, Error> {
        match self {
            Expr::Constant(value) => Ok(value.clone()),
            Expr::Column(index) => {
                if let Some(row) = row {
                    row.get(*index.inner())
                        .cloned()
                        .ok_or(Error::InvalidColumnIndex(index.clone(), row.clone()))
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
            Expr::NotEqual(lhs, rhs) => {
                let lhs_value = lhs.evaluate(row)?;
                let rhs_value = rhs.evaluate(row)?;
                if lhs_value != rhs_value {
                    Ok(Value::Boolean(true))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
            Expr::Greater(lhs, rhs) => {
                let lhs_value = lhs.evaluate(row)?;
                let rhs_value = rhs.evaluate(row)?;
                if lhs_value > rhs_value {
                    Ok(Value::Boolean(true))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
            Expr::GreaterEqual(lhs, rhs) => {
                let lhs_value = lhs.evaluate(row)?;
                let rhs_value = rhs.evaluate(row)?;
                if lhs_value >= rhs_value {
                    Ok(Value::Boolean(true))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
            Expr::Less(lhs, rhs) => {
                let lhs_value = lhs.evaluate(row)?;
                let rhs_value = rhs.evaluate(row)?;
                if lhs_value < rhs_value {
                    Ok(Value::Boolean(true))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
            Expr::LessEqual(lhs, rhs) => {
                let lhs_value = lhs.evaluate(row)?;
                let rhs_value = rhs.evaluate(row)?;
                if lhs_value <= rhs_value {
                    Ok(Value::Boolean(true))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
        }
    }
}
