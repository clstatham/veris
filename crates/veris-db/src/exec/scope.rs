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
    pub fn add_table(&mut self, table: &Table) -> Result<(), Error> {
        if self.tables.contains(&table.name) {
            return Ok(());
        }

        self.tables.insert(table.name.clone());
        for (i, column) in table.columns.iter().enumerate() {
            let index = ColumnIndex::new(i);
            let label = ColumnLabel::Qualified(table.name.clone(), column.name.clone());
            self.qualified
                .insert((table.name.clone(), column.name.clone()), index.clone());
            self.unqualified
                .entry(column.name.clone())
                .or_default()
                .push(index);
            self.columns.push(label);
        }

        Ok(())
    }

    pub fn get_columm_index(
        &self,
        table: Option<&TableName>,
        name: &ColumnName,
    ) -> Option<ColumnIndex> {
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
}
