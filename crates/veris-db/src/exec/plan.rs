use std::fmt::{self};

use crate::{
    engine::Transaction,
    error::Error,
    types::{schema::Table, value::ColumnLabel},
};

use super::{Executor, aggregate::Aggregate, expr::Expr, join::JoinType, session::StatementResult};

pub enum Plan {
    CreateTable(Table),
    DropTable(String),
    Insert {
        table: Table,
        source: Box<Plan>,
    },
    Delete {
        table: Table,
        source: Expr,
    },
    Query(Box<Plan>),
    Aggregate {
        source: Box<Plan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Aggregate>,
    },
    Filter {
        source: Box<Plan>,
        predicate: Expr,
    },
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        on: Option<Expr>,
        join_type: JoinType,
    },
    Nothing {
        columns: Vec<ColumnLabel>,
    },
    Project {
        source: Box<Plan>,
        columns: Vec<Expr>,
        aliases: Vec<ColumnLabel>,
    },
    Scan {
        table: Table,
        filter: Option<Expr>,
        alias: Option<String>,
    },
    Values {
        rows: Vec<Vec<Expr>>,
    },
}

impl Plan {
    pub fn execute(self, transaction: &impl Transaction) -> Result<StatementResult, Error> {
        log::debug!("Executing plan:\n{}", self);
        Executor::new(transaction).execute(self)
    }

    pub fn num_columns(&self) -> usize {
        match self {
            Plan::CreateTable { .. } => 0,
            Plan::DropTable { .. } => 0,
            Plan::Delete { .. } => 0,
            Plan::Insert { source, .. } => source.num_columns(),
            Plan::Query(source) => source.num_columns(),
            Plan::Aggregate {
                group_by,
                aggregates,
                ..
            } => group_by.len() + aggregates.len(),
            Plan::Filter { source, .. } => source.num_columns(),
            Plan::Join { left, right, .. } => left.num_columns() + right.num_columns(),
            Plan::Nothing { columns } => columns.len(),
            Plan::Project { columns, .. } => columns.len(),
            Plan::Scan { table, .. } => table.columns.len(),
            Plan::Values { rows } => rows.first().map_or(0, |r| r.len()),
        }
    }

    pub fn column_label(&self, index: usize) -> ColumnLabel {
        match self {
            Plan::CreateTable { .. } => ColumnLabel::None,
            Plan::DropTable { .. } => ColumnLabel::None,
            Plan::Delete { .. } => ColumnLabel::None,
            Plan::Insert { source, .. } => source.column_label(index),
            Plan::Query(source) => source.column_label(index),
            Plan::Aggregate {
                source, group_by, ..
            } => match group_by.get(index) {
                Some(Expr::Column(i)) => source.column_label(*i),
                Some(_) | None => ColumnLabel::None,
            },
            Plan::Filter { source, .. } => source.column_label(index),
            Plan::Join {
                left,
                right,
                join_type,
                ..
            } => match join_type {
                JoinType::Inner => {
                    if index < left.num_columns() {
                        left.column_label(index)
                    } else {
                        right.column_label(index - left.num_columns())
                    }
                }
                JoinType::Left => {
                    if index < left.num_columns() {
                        left.column_label(index)
                    } else {
                        right.column_label(index - left.num_columns())
                    }
                }
                JoinType::Right => {
                    if index < right.num_columns() {
                        right.column_label(index)
                    } else {
                        left.column_label(index - right.num_columns())
                    }
                }
            },
            Plan::Nothing { columns } => columns.get(index).cloned().unwrap_or(ColumnLabel::None),
            Plan::Project {
                source,
                columns,
                aliases,
            } => match aliases.get(index) {
                Some(ColumnLabel::None) | None => match columns.get(index) {
                    Some(Expr::Column(i)) => source.column_label(*i),
                    Some(_) | None => ColumnLabel::None,
                },
                Some(label) => label.clone(),
            },
            Plan::Scan { table, alias, .. } => ColumnLabel::Qualified(
                alias.clone().unwrap_or_else(|| table.name.clone()),
                table.columns[index].name.clone(),
            ),
            Plan::Values { .. } => ColumnLabel::None,
        }
    }

    pub fn format(
        &self,
        f: &mut fmt::Formatter<'_>,
        prefix: &str,
        root: bool,
        last_child: bool,
    ) -> fmt::Result {
        let prefix = if !last_child {
            write!(f, "{}├── ", prefix)?;
            format!("{}│   ", prefix)
        } else if !root {
            write!(f, "{}└── ", prefix)?;
            format!("{}    ", prefix)
        } else {
            write!(f, "{}", prefix)?;
            prefix.to_string()
        };

        match self {
            Plan::CreateTable(table) => {
                writeln!(f, "CreateTable: {}", table.name)?;

                for column in &table.columns {
                    writeln!(f, "{}  └── {:?}", prefix, column)?;
                }
            }
            Plan::DropTable(table) => {
                writeln!(f, "DropTable: {}", table)?;
            }
            Plan::Insert { table, source } => {
                writeln!(f, "Insert: {}", table.name)?;
                source.format(f, &prefix, false, true)?;
            }
            Plan::Delete { table, source } => {
                writeln!(f, "Delete: {}", table.name)?;
                writeln!(f, "{}└── {}", prefix, source)?;
            }
            Plan::Query(source) => {
                writeln!(f, "Query")?;
                source.format(f, &prefix, false, true)?;
            }
            Plan::Aggregate {
                source,
                group_by,
                aggregates,
            } => {
                writeln!(f, "Aggregate ({} groups)", group_by.len())?;
                for group in group_by.iter() {
                    writeln!(f, "{}├── {}", prefix, group)?;
                }
                for aggregate in aggregates.iter() {
                    writeln!(f, "{}├── {}", prefix, aggregate)?;
                }
                source.format(f, &prefix, false, true)?;
            }
            Plan::Filter { source, predicate } => {
                writeln!(f, "Filter: {}", predicate)?;
                source.format(f, &prefix, false, true)?;
            }
            Plan::Join {
                left,
                right,
                on,
                join_type,
            } => {
                writeln!(
                    f,
                    "Join: {} ({:?})",
                    on.as_ref().map_or("None".to_string(), |e| e.to_string()),
                    join_type
                )?;
                left.format(f, &prefix, false, false)?;
                right.format(f, &prefix, false, true)?;
            }
            Plan::Nothing { .. } => {
                writeln!(f, "Nothing")?;
            }
            Plan::Project {
                source,
                columns,
                aliases,
            } => {
                writeln!(f, "Project")?;
                for (i, column) in columns.iter().enumerate() {
                    writeln!(f, "{}├── {}: {}", prefix, aliases[i], column)?;
                }
                source.format(f, &prefix, false, true)?;
            }
            Plan::Scan { table, .. } => {
                writeln!(f, "Scan")?;
                writeln!(f, "{}└── {}", prefix, table.name)?;
            }
            Plan::Values { rows } => {
                writeln!(f, "Values")?;
                for row in rows {
                    writeln!(f, "{}└── {:?}", prefix, row)?;
                }
            }
        }

        Ok(())
    }
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format(f, "", true, true)
    }
}
