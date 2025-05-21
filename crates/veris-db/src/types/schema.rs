use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{encoding::ValueEncoding, error::Error};

use super::value::{DataType, Value};

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct Table {
    pub name: String,
    pub primary_key_index: usize,
    pub columns: Vec<Column>,
}

impl ValueEncoding for Table {}

impl Table {
    pub fn new(name: &str, primary_key_index: usize) -> Self {
        Self {
            name: name.to_string(),
            primary_key_index,
            columns: Vec::new(),
        }
    }

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

    pub fn with_column(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }

    pub fn with_columns(mut self, columns: impl IntoIterator<Item = Column>) -> Self {
        self.columns.extend(columns);
        self
    }

    pub fn with_primary_key(mut self, primary_key_index: usize) -> Self {
        self.primary_key_index = primary_key_index;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct ForeignKey {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub references: Option<ForeignKey>,
    pub has_secondary_index: bool,
}

impl ValueEncoding for Column {}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable: true,
            references: None,
            has_secondary_index: false,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn with_references(mut self, table: String, columns: Vec<String>) -> Self {
        self.references = Some(ForeignKey { table, columns });
        self.has_secondary_index = true;
        self
    }
}

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
                        table: foreign_table.to_string(),
                        columns: referred_columns.iter().map(|col| col.to_string()).collect(),
                    };
                    references = Some(foreign_key);
                }
                _ => {}
            }
        }
        Ok(Column {
            name: value.name.to_string(),
            data_type: DataType::try_from(&value.data_type)?,
            nullable,
            has_secondary_index: references.is_some(),
            references,
        })
    }
}
