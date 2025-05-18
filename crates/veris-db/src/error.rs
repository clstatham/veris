use std::path::PathBuf;

use sqlparser::ast;
use thiserror::Error;

use crate::types::{
    schema::{ColumnIndex, TableName},
    value::Value,
};

#[derive(Debug, Error, PartialEq, Clone)]
pub enum Error {
    #[error("Not in a transaction")]
    NotInTransaction,
    #[error("Transaction already in progress")]
    AlreadyInTransaction,
    #[error("Table {0} already exists")]
    TableAlreadyExists(TableName),
    #[error("Table {0} does not exist")]
    TableDoesNotExist(TableName),
    #[error("Row not found")]
    RowNotFound,
    #[error("Column {0} not found")]
    ColumnNotFound(String),
    #[error("Invalid column index: {0}")]
    InvalidColumnIndex(ColumnIndex),
    #[error("Invalid data type: {0}")]
    InvalidDataType(ast::DataType),
    #[error("Invalid value: {0}")]
    InvalidValue(Box<ast::Value>),
    #[error("Invalid primary key: {0}")]
    InvalidPrimaryKey(Box<ast::Expr>),
    #[error("Row is in invalid state")]
    InvalidRowState,
    #[error("Invalid filter: {0}")]
    InvalidFilterResult(Value),
    #[error("Row has too many values for table {0}")]
    TooManyValues(TableName),
    #[error("Error converting from AST: {0}")]
    FromAstError(String),
    #[error("`{0}` not yet supported")]
    NotYetSupported(String),
    #[error("Poisoned mutex")]
    PoisonedMutex,
    #[error("De/serialization error: {0}")]
    Serialization(String),
    #[error("I/O error: {0}")]
    Io(String),
    #[error("Directory already exists: {0}")]
    DirectoryAlreadyExists(PathBuf),
    #[error("Integer overflow")]
    IntegerOverflow,
}

pub fn io_error(error: std::io::Error) -> Error {
    Error::Io(error.to_string())
}
