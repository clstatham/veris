use std::{num::TryFromIntError, string::FromUtf8Error, sync::PoisonError};

use sqlparser::ast;
use thiserror::Error;

use crate::types::value::{ColumnLabel, DataType, Value};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error, PartialEq, Clone)]
pub enum Error {
    #[error("Aggregate function not found: {}", _0)]
    AggregateNotFound(String),
    #[error("Already in transaction")]
    AlreadyInTransaction,
    #[error("Column not found: {}", _0)]
    ColumnNotFound(String),
    #[error("Duplicate aggregate function: {}", _0)]
    DuplicateAggregate(String),
    #[error("Duplicate column: {}", _0)]
    DuplicateColumn(ColumnLabel),
    #[error("Duplicate table: {}", _0)]
    DuplicateTable(String),
    #[error("Error parsing AST: {}", _0)]
    FromAst(String),
    #[error("Integer overflow")]
    IntegerOverflow,
    #[error("Invalid cast from {} to {}", value, to)]
    InvalidCast { value: Value, to: DataType },
    #[error("Invalid column index: {}", _0)]
    InvalidColumnIndex(usize),
    #[error("Invalid column label: {}", _0)]
    InvalidColumnLabel(String),
    #[error("Invalid datatype: {}", _0)]
    InvalidDataType(ast::DataType),
    #[error("Invalid date: {}", _0)]
    InvalidDate(String),
    #[error("Engine in invalid state: {}", _0)]
    InvalidEngineState(String),
    #[error("Invalid filter result: {}", _0)]
    InvalidFilterResult(Value),
    #[error("Invalid plan")]
    InvalidPlan,
    #[error("Invalid primary key: {}", _0)]
    InvalidPrimaryKey(Box<ast::Expr>),
    #[error("Invalid row: {}", _0)]
    InvalidRow(String),
    #[error("Invalid row state")]
    InvalidRowState,
    #[error("Invalid SQL: {}", _0)]
    InvalidSql(String),
    #[error("Invalid UTF-8")]
    InvalidUtf8,
    #[error("Invalid value: {}", _0)]
    InvalidValue(Box<ast::Value>),
    #[error("I/O error: {}", _0)]
    Io(String),
    #[error("Not in transaction")]
    NotInTransaction,
    #[error("Not yet supported: {}", _0)]
    NotYetSupported(String),
    #[error("Error in order of operations: {}", _0)]
    OutOfOrder(String),
    #[error("Poisoned mutex")]
    PoisonedMutex,
    #[error(
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
    #[error("Row not found")]
    RowNotFound,
    #[error("Error de/serializing: {}", _0)]
    Serialization(String),
    #[error("Table already exists: {}", _0)]
    TableAlreadyExists(String),
    #[error("Table does not exist: {}", _0)]
    TableDoesNotExist(String),
    #[error("Transaction is read-only")]
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
