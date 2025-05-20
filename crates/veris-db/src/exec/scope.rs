use std::collections::{HashMap, HashSet};

use crate::{
    error::Error,
    types::{
        schema::{ColumnIndex, ColumnName, Table, TableName},
        value::ColumnLabel,
    },
};

#[derive(Debug, Default)]
pub struct Scope {
    columns: Vec<ColumnLabel>,
    tables: HashSet<TableName>,
    qualified: HashMap<(TableName, ColumnName), ColumnIndex>,
    unqualified: HashMap<ColumnName, Vec<ColumnIndex>>,
}

impl Scope {
    pub fn merge_with(&mut self, scope: Self) -> Result<(), Error> {
        for table in scope.tables {
            if self.tables.contains(&table) {
                return Err(Error::DuplicateTable(table));
            }
            self.tables.insert(table);
        }
        for label in scope.columns {
            self.add_column(label)?;
        }

        Ok(())
    }

    pub fn add_table(&mut self, table: &Table, alias: Option<&TableName>) -> Result<(), Error> {
        let name = alias.unwrap_or(&table.name);
        if self.tables.contains(name) {
            return Err(Error::DuplicateTable(name.clone()));
        }

        for column in &table.columns {
            let label = ColumnLabel::Qualified(name.clone(), column.name.clone());
            self.add_column(label)?;
        }

        self.tables.insert(name.clone());

        Ok(())
    }

    pub fn add_column(&mut self, label: ColumnLabel) -> Result<ColumnIndex, Error> {
        if self.columns.contains(&label) {
            return Err(Error::DuplicateColumn(label));
        }
        let index = ColumnIndex::new(self.columns.len());

        if let ColumnLabel::Qualified(table, column) = &label {
            self.qualified
                .insert((table.clone(), column.clone()), index.clone());
        }

        if let ColumnLabel::Unqualified(column) | ColumnLabel::Qualified(_, column) = &label {
            self.unqualified
                .entry(column.clone())
                .or_default()
                .push(index.clone());
        }

        self.columns.push(label);
        Ok(index)
    }

    pub fn get_column_index(
        &self,
        table: Option<&TableName>,
        name: &ColumnName,
    ) -> Option<ColumnIndex> {
        if self.columns.is_empty() {
            return None;
        }
        if let Some(table) = table {
            if !self.tables.contains(table) {
                return None;
            }
            if let Some(index) = self.qualified.get(&(table.clone(), name.clone())) {
                return Some(index.clone());
            }
        } else if let Some(indices) = self.unqualified.get(name) {
            if indices.len() == 1 {
                return Some(indices[0].clone());
            } else {
                return None;
            }
        }
        None
    }

    pub fn get_column_label(
        &self,
        table: Option<&TableName>,
        name: &ColumnName,
    ) -> Option<ColumnLabel> {
        if let Some(index) = self.get_column_index(table, name) {
            return Some(self.columns[*index.inner()].clone());
        }
        None
    }
}
