use std::collections::{HashMap, HashSet};

use sqlparser::ast;

use crate::{
    error::Error,
    types::{schema::Table, value::ColumnLabel},
};

use super::aggregate::aggregate_function_args;

#[derive(Debug, Default)]
pub struct Scope {
    columns: Vec<ColumnLabel>,
    tables: HashSet<String>,
    qualified: HashMap<(String, String), usize>,
    unqualified: HashMap<String, Vec<usize>>,
    aggregates: HashMap<ast::Function, usize>,
}

impl Scope {
    pub fn from_table(table: &Table, alias: Option<&String>) -> Result<Self, Error> {
        let mut scope = Self::default();
        scope.add_table(table, alias)?;
        Ok(scope)
    }

    pub fn spawn(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            ..Default::default()
        }
    }

    pub fn merge_with(&mut self, scope: Self) -> Result<(), Error> {
        for table in scope.tables {
            self.tables.insert(table);
        }
        let offset = self.columns.len();
        for label in scope.columns {
            self.add_column(label)?;
        }
        for (agg, index) in scope.aggregates {
            self.aggregates.entry(agg).or_insert(index + offset);
        }

        Ok(())
    }

    pub fn add_table(&mut self, table: &Table, alias: Option<&String>) -> Result<(), Error> {
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

    pub fn add_column(&mut self, label: ColumnLabel) -> Result<usize, Error> {
        let index = self.columns.len();

        if let ColumnLabel::Qualified(table, column) = &label {
            self.qualified
                .insert((table.clone(), column.clone()), index);
        }

        if let ColumnLabel::Unqualified(column) | ColumnLabel::Qualified(_, column) = &label {
            self.unqualified
                .entry(column.clone())
                .or_default()
                .push(index);
        }

        self.columns.push(label);
        Ok(index)
    }

    pub fn add_aggregate(&mut self, expr: ast::Function) -> Result<usize, Error> {
        if self.aggregates.contains_key(&expr) {
            return Err(Error::DuplicateAggregate(expr.to_string()));
        }

        let args = aggregate_function_args(&expr)?;
        if args.len() != 1 {
            return Err(Error::NotYetSupported(
                "Aggregate function with multiple arguments".to_string(),
            ));
        }
        let arg = args[0].clone();

        let label = if let ast::Expr::Identifier(ident) = &arg {
            ColumnLabel::Unqualified(ident.value.clone())
        } else if let ast::Expr::CompoundIdentifier(idents) = &arg {
            assert_eq!(idents.len(), 2);
            ColumnLabel::Qualified(idents[0].value.clone(), idents[1].value.clone())
        } else {
            ColumnLabel::None
        };

        let index = self.add_column(label)?;
        self.aggregates.insert(expr, index);
        Ok(index)
    }

    pub fn get_aggregate_index(&self, func: &ast::Function) -> Option<usize> {
        self.aggregates.get(func).cloned()
    }

    pub fn get_column_index(&self, table: Option<&String>, name: &String) -> Option<usize> {
        if self.columns.is_empty() {
            return None;
        }
        if let Some(table) = table {
            if !self.tables.contains(table) {
                return None;
            }
            if let Some(index) = self.qualified.get(&(table.clone(), name.clone())) {
                return Some(*index);
            }
        } else if let Some(indices) = self.unqualified.get(name) {
            if indices.len() == 1 {
                return Some(indices[0]);
            } else {
                // ambiguous column name
                return None;
            }
        }
        None
    }

    pub fn get_column_label(&self, index: usize) -> Result<&ColumnLabel, Error> {
        self.columns
            .get(index)
            .ok_or(Error::InvalidColumnIndex(index))
    }
}
