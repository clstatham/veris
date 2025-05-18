use derive_more::Display;
use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{error::Error, wrap};

use super::value::DataType;

wrap! {
    #[derive(Clone, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct TableName(String);

    #[derive(Clone, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct ColumnName(String);

    #[derive(Clone, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct ColumnIndex(usize);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: TableName,
    pub primary_key_index: ColumnIndex,
    pub columns: Vec<Column>,
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: ColumnName,
    pub data_type: DataType,
}

impl TryFrom<&ast::ColumnDef> for Column {
    type Error = Error;

    fn try_from(value: &ast::ColumnDef) -> Result<Self, Self::Error> {
        Ok(Column {
            name: ColumnName(value.name.to_string()),
            data_type: DataType::try_from(&value.data_type)?,
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
}
