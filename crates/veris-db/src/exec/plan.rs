use sqlparser::ast;

use crate::{
    engine::{Catalog, Transaction},
    error::Error,
    types::{
        schema::{ColumnIndex, Table, TableName},
        value::ColumnLabel,
    },
};

use super::{Executor, expr::Expr, scope::Scope, session::StatementResult};

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
    pub fn execute(self, txn: &impl Transaction) -> Result<StatementResult, Error> {
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
    HashJoin {
        left: Box<Node>,
        left_col: ColumnIndex,
        right: Box<Node>,
        right_col: ColumnIndex,
        outer: bool,
    },
    NestedLoopJoin {
        left: Box<Node>,
        right: Box<Node>,
        predicate: Option<Expr>,
        outer: bool,
    },
}

impl Node {
    pub fn num_columns(&self) -> usize {
        match self {
            Self::Scan { table, .. } => table.columns.len(),
            Self::Values { rows } => rows.first().map(|row| row.len()).unwrap_or(0),
            Self::Filter { source, .. } => source.num_columns(),
            Self::Project { expressions, .. } => expressions.len(),
            Self::HashJoin { left, right, .. } | Self::NestedLoopJoin { left, right, .. } => {
                left.num_columns() + right.num_columns()
            }
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
            Self::HashJoin { left, right, .. } | Self::NestedLoopJoin { left, right, .. } => {
                if *index.inner() < left.num_columns() {
                    left.column_label(index)
                } else {
                    right.column_label(&(index.clone() - left.num_columns()))
                }
            }
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
            let mut exprs = Vec::new();
            for expr in row {
                let expr = Expr::build(expr, &scope)?;
                exprs.push(expr);
            }
            rows.push(exprs);
        }
        Ok(Node::Values { rows })
    }

    fn from_select(select: &ast::Select, catalog: &impl Catalog) -> Result<Self, Error> {
        let mut scope = Scope::default();
        if select.from.is_empty() {
            return Err(Error::InvalidSql(select.to_string()));
        }

        let mut node = Self::from_from(&select.from[0], &mut scope, catalog)?;
        for from in select.from.iter().skip(1) {
            let right = Self::from_from(from, &mut scope, catalog)?;
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                right: Box::new(right),
                predicate: None,
                outer: false,
            };
        }

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

    fn from_relation(
        relation: &ast::TableFactor,
        scope: &mut Scope,
        catalog: &impl Catalog,
    ) -> Result<Node, Error> {
        let node = match relation {
            ast::TableFactor::Table { name, alias, .. } => {
                let table_name = TableName::new(name.to_string());
                let table = catalog
                    .get_table(&table_name)?
                    .ok_or(Error::TableDoesNotExist(table_name))?;
                let alias = alias.as_ref().map(|a| TableName::new(a.name.value.clone()));
                scope.add_table(&table, alias.as_ref())?;
                Node::Scan {
                    table,
                    filter: None,
                }
            }
            ast::TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                assert!(alias.is_none());
                Self::from_from(table_with_joins, scope, catalog)?
            }
            _ => return Err(Error::NotYetSupported(format!("{relation:?}"))),
        };

        Ok(node)
    }

    fn from_from(
        from: &ast::TableWithJoins,
        scope: &mut Scope,
        catalog: &impl Catalog,
    ) -> Result<Node, Error> {
        let mut node = Self::from_relation(&from.relation, scope, catalog)?;

        for join in &from.joins {
            let right = Self::from_relation(&join.relation, scope, catalog)?;
            let mut predicate = None;
            match &join.join_operator {
                ast::JoinOperator::Join(constraint) => match constraint {
                    ast::JoinConstraint::None => {}
                    ast::JoinConstraint::On(expr) => {
                        predicate = Some(Expr::build(expr, scope)?);
                    }
                    _ => {
                        return Err(Error::NotYetSupported(join.to_string()));
                    }
                },
                ast::JoinOperator::Left(constraint) => match constraint {
                    ast::JoinConstraint::None => {}
                    ast::JoinConstraint::On(expr) => {
                        predicate = Some(Expr::build(expr, scope)?);
                    }
                    _ => {
                        return Err(Error::NotYetSupported(join.to_string()));
                    }
                },
                ast::JoinOperator::CrossJoin => {
                    // Cross join does not have an ON clause
                }
                _ => {
                    return Err(Error::NotYetSupported(join.to_string()));
                }
            }
            node = Node::NestedLoopJoin {
                left: Box::new(node),
                right: Box::new(right),
                predicate,
                outer: false,
            };
        }

        Ok(node)
    }
}
