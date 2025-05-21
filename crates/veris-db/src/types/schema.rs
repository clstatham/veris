use std::ops::{Add, Sub};

use derive_more::Display;
use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{encoding::ValueEncoding, error::Error, wrap};

use super::value::{DataType, Value};

wrap! {
    #[derive(Clone, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct TableName(String);

    #[derive(Clone, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct ColumnName(String);

    #[derive(Clone, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct ColumnIndex(usize);
}

impl Add<usize> for ColumnIndex {
    type Output = ColumnIndex;

    fn add(self, rhs: usize) -> Self::Output {
        ColumnIndex(self.0 + rhs)
    }
}

impl Sub<usize> for ColumnIndex {
    type Output = ColumnIndex;

    fn sub(self, rhs: usize) -> Self::Output {
        ColumnIndex(self.0 - rhs)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct Table {
    pub name: TableName,
    pub primary_key_index: ColumnIndex,
    pub columns: Vec<Column>,
}

impl ValueEncoding for Table {}

impl Table {
    pub fn validate_row(&self, row: &[Value]) -> bool {
        if row.len() != self.columns.len() {
            return false;
        }
        for (i, column) in self.columns.iter().enumerate() {
            if !row[i].is_compatible(&column.data_type) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct ForeignKey {
    pub table: TableName,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct Column {
    pub name: ColumnName,
    pub data_type: DataType,
    pub nullable: bool,
    pub references: Option<ForeignKey>,
    pub has_secondary_index: bool,
}

impl ValueEncoding for Column {}

impl TryFrom<&ast::ColumnDef> for Column {
    type Error = Error;

    fn try_from(value: &ast::ColumnDef) -> Result<Self, Self::Error> {
        let mut nullable = true;
        let mut references = None;
        for option in value.options.iter() {
            match &option.option {
                ast::ColumnOption::Null => nullable = true,
                ast::ColumnOption::NotNull => nullable = false,
                ast::ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    let foreign_key = ForeignKey {
                        table: TableName(foreign_table.to_string()),
                        columns: referred_columns
                            .iter()
                            .map(|col| ColumnName(col.to_string()))
                            .collect(),
                    };
                    references = Some(foreign_key);
                }
                _ => {}
            }
        }
        Ok(Column {
            name: ColumnName(value.name.to_string()),
            data_type: DataType::try_from(&value.data_type)?,
            nullable,
            has_secondary_index: references.is_some(),
            references,
        })
    }
}
