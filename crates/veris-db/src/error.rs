use std::{num::TryFromIntError, string::FromUtf8Error, sync::PoisonError};

use derive_more::Display;
use sqlparser::ast;
use thiserror::Error;

use crate::types::value::{ColumnLabel, DataType, Value};

#[derive(Debug, Error, PartialEq, Clone, Display)]
pub enum Error {
    AggregateNotFound(String),
    AlreadyInTransaction,
    ColumnNotFound(String),
    DuplicateAggregate(String),
    DuplicateColumn(ColumnLabel),
    DuplicateTable(String),
    FromAstError(String),
    IntegerOverflow,
    #[display("Invalid cast from {} to {}", value, to)]
    InvalidCast {
        value: Value,
        to: DataType,
    },
    InvalidColumnIndex(usize),
    InvalidColumnLabel(String),
    InvalidDataType(ast::DataType),
    InvalidDate(String),
    InvalidEngineState(String),
    InvalidFilterResult(Value),
    InvalidPlan,
    InvalidPrimaryKey(Box<ast::Expr>),
    InvalidRow(String),
    InvalidRowState,
    InvalidSql(String),
    InvalidUtf8,
    InvalidValue(Box<ast::Value>),
    Io(String),
    NoCurrentTable,
    NotInTransaction,
    NotYetSupported(String),
    OutOfOrder,
    PoisonedMutex,
    #[display(
        "Referential integrity violation: {}.{} = {}",
        table,
        column,
        source_id
    )]
    ReferentialIntegrity {
        table: String,
        column: String,
        source_id: Value,
    },
    RowNotFound,
    Serialization(String),
    TableAlreadyExists(String),
    TableDoesNotExist(String),
    TooManyValues(String),
    TransactionReadOnly,
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Error::PoisonedMutex
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error.to_string())
    }
}

impl From<TryFromIntError> for Error {
    fn from(_: TryFromIntError) -> Self {
        Error::IntegerOverflow
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_: FromUtf8Error) -> Self {
        Error::InvalidUtf8
    }
}

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Serialization(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Serialization(msg.to_string())
    }
}
