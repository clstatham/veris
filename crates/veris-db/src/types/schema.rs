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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: TableName,
    pub primary_key_index: ColumnIndex,
    pub columns: Vec<Column>,
}

impl ValueEncoding for Table {}

impl TryFrom<&ast::CreateTable> for Table {
    type Error = Error;

    fn try_from(value: &ast::CreateTable) -> Result<Self, Self::Error> {
        let mut primary_key_index = ColumnIndex::new(0);
        if let Some(primary_key) = value.primary_key.as_ref() {
            if let ast::Expr::Value(v) = &**primary_key {
                if let ast::Value::Number(a, _) = &v.value {
                    primary_key_index = ColumnIndex::new(
                        a.parse()
                            .map_err(|_| Error::InvalidPrimaryKey(primary_key.clone()))?,
                    );
                } else {
                    return Err(Error::InvalidPrimaryKey(primary_key.clone()));
                }
            } else {
                return Err(Error::InvalidPrimaryKey(primary_key.clone()));
            }
        }
        let mut columns = Vec::new();
        for column in value.columns.iter() {
            let col = Column::try_from(column)?;
            columns.push(col);
        }

        Ok(Table {
            name: TableName(value.name.to_string()),
            primary_key_index,
            columns,
        })
    }
}

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

    pub fn get_column_index(&self, name: &str) -> Result<ColumnIndex, Error> {
        for (i, column) in self.columns.iter().enumerate() {
            if column.name.0 == name {
                return Ok(ColumnIndex(i));
            }
        }
        Err(Error::ColumnNotFound(name.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub table: TableName,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use crate::sql_stmt;

    use super::*;

    #[test]
    fn test_table_conversion() {
        let sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING);";
        let ast_table = sql_stmt!(CreateTable, sql);
        let table = Table::try_from(&ast_table).unwrap();
        assert_eq!(table.name, TableName("test_table".to_string()));
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.primary_key_index, ColumnIndex(0));
    }

    #[test]
    fn test_column_conversion() {
        let sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING);";
        let ast_table = sql_stmt!(CreateTable, sql);
        let column = Column::try_from(&ast_table.columns[0]).unwrap();
        assert_eq!(column.name, ColumnName("id".to_string()));
        assert_eq!(column.data_type, DataType::Integer);
    }

    #[test]
    fn test_column_references() {
        let sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, FOREIGN KEY (name) REFERENCES other_table (other_name));";
        let ast_table = sql_stmt!(CreateTable, sql);
        let column = Column::try_from(&ast_table.columns[2]).unwrap();
        assert_eq!(column.name, ColumnName("name".to_string()));
        assert_eq!(
            column.references.as_ref().unwrap().table,
            TableName("other_table".to_string())
        );
    }
}
