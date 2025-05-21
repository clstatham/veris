use std::fmt;

use sqlparser::ast;

use crate::{
    error::Error,
    types::{
        schema::ColumnIndex,
        value::{Row, Value},
    },
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Expr {
    Constant(Value),
    Column(ColumnIndex),
    BinaryOp(Box<Expr>, BinaryOp, Box<Expr>),
}

impl Expr {
    pub fn eval(&self, row: Option<&Row>) -> Result<Value, Error> {
        match self {
            Expr::Constant(value) => Ok(value.clone()),
            Expr::Column(index) => {
                if let Some(row) = row {
                    Ok(row
                        .get(**index)
                        .cloned()
                        .ok_or(Error::InvalidColumnIndex(index.clone()))?)
                } else {
                    Err(Error::RowNotFound)
                }
            }
            Expr::BinaryOp(a, op, b) => {
                let a = a.eval(row)?;
                let b = b.eval(row)?;
                let result = match op {
                    BinaryOp::Add => a.checked_add(&b)?,
                    BinaryOp::Subtract => a.checked_sub(&b)?,
                    BinaryOp::Multiply => a.checked_mul(&b)?,
                    BinaryOp::Divide => a.checked_div(&b)?,
                    BinaryOp::Equal => Value::Boolean(a == b),
                    BinaryOp::NotEqual => Value::Boolean(a != b),
                    BinaryOp::GreaterThan => Value::Boolean(a > b),
                    BinaryOp::LessThan => Value::Boolean(a < b),
                    BinaryOp::GreaterThanOrEqual => Value::Boolean(a >= b),
                    BinaryOp::LessThanOrEqual => Value::Boolean(a <= b),
                    BinaryOp::And => Value::Boolean(a.is_truthy() && b.is_truthy()),
                    BinaryOp::Or => Value::Boolean(a.is_truthy() || b.is_truthy()),

                    _ => {
                        return Err(Error::NotYetSupported(format!(
                            "Binary operator {:?} not yet supported",
                            op
                        )));
                    }
                };
                Ok(result)
            }
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Constant(value) => write!(f, "{}", value),
            Expr::Column(index) => write!(f, "col{}", index),
            Expr::BinaryOp(left, op, right) => {
                write!(f, "({} {} {})", left, op, right)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulus,
    And,
    Or,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

impl TryFrom<&ast::BinaryOperator> for BinaryOp {
    type Error = Error;

    fn try_from(value: &ast::BinaryOperator) -> Result<Self, Self::Error> {
        match value {
            ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOp::Subtract),
            ast::BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
            ast::BinaryOperator::Divide => Ok(BinaryOp::Divide),
            ast::BinaryOperator::Modulo => Ok(BinaryOp::Modulus),
            ast::BinaryOperator::And => Ok(BinaryOp::And),
            ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            ast::BinaryOperator::Eq => Ok(BinaryOp::Equal),
            ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEqual),
            ast::BinaryOperator::Gt => Ok(BinaryOp::GreaterThan),
            ast::BinaryOperator::Lt => Ok(BinaryOp::LessThan),
            ast::BinaryOperator::GtEq => Ok(BinaryOp::GreaterThanOrEqual),
            ast::BinaryOperator::LtEq => Ok(BinaryOp::LessThanOrEqual),
            _ => Err(Error::NotYetSupported(format!(
                "Binary operator {:?} not supported",
                value
            ))),
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Subtract => write!(f, "-"),
            BinaryOp::Multiply => write!(f, "*"),
            BinaryOp::Divide => write!(f, "/"),
            BinaryOp::Modulus => write!(f, "%"),
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
            BinaryOp::Equal => write!(f, "="),
            BinaryOp::NotEqual => write!(f, "<>"),
            BinaryOp::GreaterThan => write!(f, ">"),
            BinaryOp::LessThan => write!(f, "<"),
            BinaryOp::GreaterThanOrEqual => write!(f, ">="),
            BinaryOp::LessThanOrEqual => write!(f, "<="),
        }
    }
}
