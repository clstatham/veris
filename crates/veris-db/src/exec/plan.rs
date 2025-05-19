use sqlparser::ast;

use crate::{
    engine::{Catalog, Transaction},
    error::Error,
    types::{
        schema::{ColumnIndex, Table, TableName},
        value::ColumnLabel,
    },
};

use super::{ExecResult, Executor, expr::Expr, scope::Scope};

pub struct Planner<'a, C: Catalog> {
    catalog: &'a C,
}

impl<'a, C: Catalog> Planner<'a, C> {
    pub fn new(catalog: &'a C) -> Self {
        Self { catalog }
    }

    pub fn plan(&self, statement: &ast::Statement) -> Result<Plan, Error> {
        match statement {
            ast::Statement::CreateTable(stmt) => self.plan_create_table(stmt),
            ast::Statement::Drop {
                object_type, names, ..
            } => {
                if object_type == &ast::ObjectType::Table {
                    if let Some(name) = names.first() {
                        let table = name.to_string();
                        return self.plan_drop_table(&table);
                    }
                }
                Err(Error::NotYetSupported(statement.to_string()))
            }
            // ast::Statement::Delete(stmt) => self.plan_delete(stmt),
            ast::Statement::Insert(stmt) => self.plan_insert(stmt),
            ast::Statement::Query(stmt) => self.plan_select(stmt),
            // ast::Statement::List(stmt) => self.plan_list(stmt),
            stmt => Err(Error::NotYetSupported(stmt.to_string())),
        }
    }

    fn plan_create_table(&self, table: &ast::CreateTable) -> Result<Plan, Error> {
        let table = Table::try_from(table).map_err(|e| Error::FromAstError(e.to_string()))?;
        Ok(Plan::CreateTable(table))
    }

    fn plan_drop_table(&self, table: &str) -> Result<Plan, Error> {
        let table = TableName::new(table.to_string());
        Ok(Plan::DropTable(table))
    }

    fn plan_insert(&self, stmt: &ast::Insert) -> Result<Plan, Error> {
        let table = {
            let ast::TableObject::TableName(ref name) = stmt.table else {
                return Err(Error::NotYetSupported(stmt.to_string()));
            };
            TableName::new(name.to_string())
        };
        let table = self
            .catalog
            .get_table(&table)?
            .ok_or_else(|| Error::TableDoesNotExist(table.clone()))?;
        let source = if let Some(source) = stmt.source.as_deref() {
            Node::from_query(source, self.catalog)?
        } else {
            return Err(Error::NotYetSupported(stmt.to_string()));
        };
        Ok(Plan::Insert { table, source })
    }

    fn plan_select(&self, stmt: &ast::Query) -> Result<Plan, Error> {
        let node = Node::from_query(stmt, self.catalog)?;
        Ok(Plan::Select(node))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    CreateTable(Table),
    DropTable(TableName),
    Delete {
        table: TableName,
        primary_key: ColumnIndex,
        source: Node,
    },
    Insert {
        table: Table,
        source: Node,
    },
    Select(Node),
}

impl Plan {
    pub fn execute(self, txn: &impl Transaction) -> Result<ExecResult, Error> {
        Executor::new(txn).execute(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Values {
        rows: Vec<Vec<Expr>>,
    },
    Scan {
        table: Table,
        filter: Option<Expr>,
    },
    Filter {
        source: Box<Node>,
        predicate: Expr,
    },
    Project {
        source: Box<Node>,
        expressions: Vec<Expr>,
    },
}

impl Node {
    pub fn num_columns(&self) -> usize {
        match self {
            Self::Scan { table, .. } => table.columns.len(),
            Self::Values { rows } => rows.first().map(|row| row.len()).unwrap_or(0),
            Self::Filter { source, .. } => source.num_columns(),
            Self::Project { expressions, .. } => expressions.len(),
        }
    }

    pub fn column_label(&self, index: &ColumnIndex) -> ColumnLabel {
        match self {
            Self::Scan { table, .. } => {
                let column = &table.columns[*index.inner()];
                ColumnLabel::Qualified(table.name.clone(), column.name.clone())
            }
            Self::Values { .. } => ColumnLabel::None,
            Self::Filter { source, .. } => source.column_label(index),
            Self::Project {
                source,
                expressions,
            } => match expressions.get(*index.inner()) {
                Some(Expr::Column(index)) => source.column_label(index),
                Some(_) | None => ColumnLabel::None,
            },
        }
    }

    pub fn from_query(query: &ast::Query, catalog: &impl Catalog) -> Result<Self, Error> {
        let node = match &*query.body {
            ast::SetExpr::Values(vals) => Self::from_values(vals)?,
            ast::SetExpr::Select(select) => Self::from_select(select, catalog)?,
            _ => return Err(Error::NotYetSupported(query.to_string())),
        };

        Ok(node)
    }

    fn from_values(vals: &ast::Values) -> Result<Self, Error> {
        let scope = Scope::default();
        let mut rows = Vec::new();
        for row in &vals.rows {
            let mut values = Vec::new();
            for expr in row {
                let value = Expr::build(expr, &scope)?;
                values.push(value);
            }
            rows.push(values);
        }
        Ok(Node::Values { rows })
    }

    fn from_select(select: &ast::Select, catalog: &impl Catalog) -> Result<Self, Error> {
        let mut scope = Scope::default();
        let table_name = TableName::new(select.from[0].to_string());
        let table = catalog
            .get_table(&table_name)?
            .ok_or(Error::TableDoesNotExist(table_name))?;
        scope.add_table(&table)?;
        let mut node = Node::Scan {
            table,
            filter: None,
        };
        if let Some(where_clause) = &select.selection {
            node = Node::Filter {
                source: Box::new(node),
                predicate: Expr::build(where_clause, &scope)?,
            }
        };
        let mut expressions = Vec::new();
        if select.projection.len() == 1
            && matches!(
                select.projection.first(),
                Some(ast::SelectItem::Wildcard(_))
            )
        {
            return Ok(node);
        } else {
            for item in &select.projection {
                match item {
                    ast::SelectItem::UnnamedExpr(expr) => {
                        let expr = Expr::build(expr, &scope)?;
                        expressions.push(expr);
                    }
                    _ => {
                        return Err(Error::NotYetSupported(item.to_string()));
                    }
                }
            }
            node = Node::Project {
                source: Box::new(node),
                expressions,
            };
        }

        Ok(node)
    }
}
