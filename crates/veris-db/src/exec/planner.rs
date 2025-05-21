use sqlparser::ast;

use crate::{
    engine::Catalog,
    error::Error,
    types::{
        schema::{Column, ForeignKey, Table},
        value::{ColumnLabel, DataType, Value},
    },
};

use super::{
    aggregate::{Aggregate, aggregate_function_args, is_aggregate},
    expr::Expr,
    join::JoinType,
    plan::Plan,
    scope::Scope,
};

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
            ast::Statement::Delete(stmt) => self.plan_delete(stmt),
            ast::Statement::Insert(stmt) => self.plan_insert(stmt),
            ast::Statement::Query(stmt) => self.plan_query(stmt),
            stmt => Err(Error::NotYetSupported(stmt.to_string())),
        }
    }

    fn plan_create_table(&self, table: &ast::CreateTable) -> Result<Plan, Error> {
        let mut primary_key_index = 0;
        if let Some(primary_key) = table.primary_key.as_ref() {
            if let ast::Expr::Value(v) = &**primary_key {
                if let ast::Value::Number(a, _) = &v.value {
                    primary_key_index = a
                        .parse()
                        .map_err(|_| Error::InvalidPrimaryKey(primary_key.clone()))?;
                } else {
                    return Err(Error::InvalidPrimaryKey(primary_key.clone()));
                }
            } else {
                return Err(Error::InvalidPrimaryKey(primary_key.clone()));
            }
        }
        let mut columns = Vec::new();
        for column in table.columns.iter() {
            let mut nullable = true;
            let mut references = None;
            let mut has_secondary_index = false;
            for option in &column.options {
                match &option.option {
                    ast::ColumnOption::Null => nullable = true,
                    ast::ColumnOption::NotNull => nullable = false,
                    ast::ColumnOption::ForeignKey {
                        foreign_table,
                        referred_columns,
                        on_delete,
                        on_update,
                        characteristics,
                    } => {
                        let foreign_key = ForeignKey {
                            table: foreign_table.to_string(),
                            columns: referred_columns.iter().map(|col| col.to_string()).collect(),
                        };
                        references = Some(foreign_key);
                        has_secondary_index = true;
                        if on_delete.is_some() || on_update.is_some() {
                            return Err(Error::NotYetSupported(
                                "Foreign key with ON DELETE or ON UPDATE".to_string(),
                            ));
                        }
                        if characteristics.is_some() {
                            return Err(Error::NotYetSupported(
                                "Foreign key with characteristics".to_string(),
                            ));
                        }
                    }
                    _ => {}
                }
            }

            let col = Column {
                name: column.name.to_string(),
                data_type: DataType::try_from(&column.data_type)?,
                nullable,
                references,
                has_secondary_index,
            };
            columns.push(col);
        }

        let table = Table {
            name: table.name.to_string(),
            columns,
            primary_key_index,
        };

        Ok(Plan::CreateTable(table))
    }

    fn plan_drop_table(&self, table: &str) -> Result<Plan, Error> {
        Ok(Plan::DropTable(table.to_string()))
    }

    fn plan_insert(&self, stmt: &ast::Insert) -> Result<Plan, Error> {
        let table = {
            let ast::TableObject::TableName(ref name) = stmt.table else {
                return Err(Error::NotYetSupported(stmt.to_string()));
            };
            name.to_string()
        };
        let table = self
            .catalog
            .get_table(&table)?
            .ok_or_else(|| Error::TableDoesNotExist(table.clone()))?;
        let source = if let Some(source) = stmt.source.as_deref() {
            self.plan_query(source)?
        } else {
            return Err(Error::NotYetSupported(stmt.to_string()));
        };
        Ok(Plan::Insert {
            table,
            source: Box::new(source),
        })
    }

    fn plan_delete(&self, stmt: &ast::Delete) -> Result<Plan, Error> {
        let mut tables = Vec::new();
        for table in &stmt.tables {
            tables.push(table.to_string());
        }
        if tables.len() != 1 {
            return Err(Error::NotYetSupported(stmt.to_string()));
        }

        #[allow(clippy::unwrap_used)] // We know there is only one table
        let table = tables.pop().unwrap();
        let table = self
            .catalog
            .get_table(&table)?
            .ok_or_else(|| Error::TableDoesNotExist(table.clone()))?;
        let scope = Scope::from_table(&table, None)?;
        let source = if let Some(source) = stmt.selection.as_ref() {
            Self::build_expr(source, &scope)?
        } else {
            return Err(Error::NotYetSupported(stmt.to_string()));
        };
        Ok(Plan::Delete { table, source })
    }

    fn plan_query(&self, stmt: &ast::Query) -> Result<Plan, Error> {
        match &*stmt.body {
            ast::SetExpr::Values(values) => self.plan_values(values),
            ast::SetExpr::Select(select) => self.plan_select(select),
            ast::SetExpr::Query(query) => self.plan_query(query),
            _ => Err(Error::NotYetSupported(stmt.to_string())),
        }
    }

    fn plan_values(&self, values: &ast::Values) -> Result<Plan, Error> {
        let mut rows = Vec::new();
        for row in &values.rows {
            let mut values = Vec::new();
            for value in row {
                let value = Self::build_expr(value, &Scope::default())?;
                values.push(value);
            }
            rows.push(values);
        }
        Ok(Plan::Values { rows })
    }

    fn plan_select(&self, stmt: &ast::Select) -> Result<Plan, Error> {
        log::debug!("Planning select: {}", stmt);
        let mut scope = Scope::default();

        let mut plan = Plan::Nothing {
            columns: Vec::new(),
        };

        for from in &stmt.from {
            let mut table_plan = self.plan_scan(&from.relation, &mut scope)?;
            for join in &from.joins {
                table_plan = self.plan_join(table_plan, join, &mut scope)?;
            }
            if matches!(plan, Plan::Nothing { .. }) {
                plan = table_plan;
            } else {
                plan = Plan::Join {
                    left: Box::new(plan),
                    right: Box::new(table_plan),
                    on: None,
                    join_type: JoinType::Inner,
                };
            }
        }

        if let Some(where_clause) = &stmt.selection {
            let predicate = Self::build_expr(where_clause, &scope)?;
            plan = Plan::Filter {
                source: Box::new(plan),
                predicate,
            };
        }

        let mut group_by = Vec::new();
        match &stmt.group_by {
            ast::GroupByExpr::Expressions(exprs, _) => {
                for expr in exprs {
                    let expr = Self::build_expr(expr, &scope)?;
                    group_by.push(expr);
                }
            }
            _ => {
                return Err(Error::NotYetSupported(stmt.to_string()));
            }
        }
        let functions_and_aggregates = self.collect_aggregates(&stmt.projection, &scope)?;
        if !group_by.is_empty() || !functions_and_aggregates.is_empty() {
            let mut child_scope = scope.spawn();

            for expr in &group_by {
                if let Expr::Column(index) = expr {
                    let label = scope.get_column_label(*index)?;
                    child_scope.add_column(label.clone())?;
                } else {
                    return Err(Error::NotYetSupported(expr.to_string()));
                }
            }

            let mut aggregates = Vec::new();
            for (func, agg) in functions_and_aggregates {
                child_scope.add_aggregate(func)?;
                aggregates.push(agg);
            }

            scope = child_scope;
            plan = Plan::Aggregate {
                source: Box::new(plan),
                group_by,
                aggregates,
            };
        }

        let mut columns = Vec::new();
        let mut aliases = Vec::new();
        for projection in &stmt.projection {
            let (label, expr) = self.build_select_item(projection, &scope)?;
            if let Some(expr) = expr {
                columns.push(expr);
                aliases.push(label);
            } else {
                // Wildcard
                for i in 0..plan.num_columns() {
                    let label = plan.column_label(i);
                    columns.push(Expr::Column(i));
                    aliases.push(label);
                }
            }
        }
        if columns.is_empty() {
            return Err(Error::NotYetSupported(stmt.to_string()));
        }
        if columns.len() != aliases.len() {
            return Err(Error::NotYetSupported(stmt.to_string()));
        }

        plan = Plan::Project {
            source: Box::new(plan),
            columns,
            aliases,
        };

        Ok(Plan::Query(Box::new(plan)))
    }

    fn build_select_item(
        &self,
        item: &ast::SelectItem,
        scope: &Scope,
    ) -> Result<(ColumnLabel, Option<Expr>), Error> {
        log::debug!("Building select item: {}", item);
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                let expr = Self::build_expr(expr, scope)?;
                if let Expr::Column(index) = &expr {
                    let label = scope.get_column_label(*index)?;
                    Ok((label.clone(), Some(expr)))
                } else {
                    Ok((ColumnLabel::None, Some(expr)))
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                let expr = Self::build_expr(expr, scope)?;
                let label = ColumnLabel::Unqualified(alias.value.clone());
                Ok((label, Some(expr)))
            }
            ast::SelectItem::Wildcard(_) => Ok((ColumnLabel::Unqualified("*".to_string()), None)),
            _ => Err(Error::NotYetSupported(item.to_string())),
        }
    }

    fn plan_scan(&self, relation: &ast::TableFactor, scope: &mut Scope) -> Result<Plan, Error> {
        log::debug!("Planning scan: {}", relation);
        match relation {
            ast::TableFactor::Table { name, alias, .. } => {
                let table = name.to_string();
                let table = self
                    .catalog
                    .get_table(&table)?
                    .ok_or_else(|| Error::TableDoesNotExist(table.clone()))?;

                let alias = alias.as_ref().map(|alias| alias.to_string());

                scope.add_table(&table, alias.as_ref())?;

                Ok(Plan::Scan {
                    table: table.clone(),
                    filter: None,
                    alias,
                })
            }
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                let mut plan = self.plan_scan(&table_with_joins.relation, scope)?;
                for join in &table_with_joins.joins {
                    plan = self.plan_join(plan, join, scope)?;
                }
                Ok(plan)
            }
            _ => Err(Error::NotYetSupported(format!(
                "Unsupported table factor: {}",
                relation
            ))),
        }
    }

    fn plan_join(
        &self,
        plan: Plan,
        join: &ast::Join,
        parent_scope: &mut Scope,
    ) -> Result<Plan, Error> {
        log::debug!("Planning join: {}", join);
        let left = plan;
        let right = self.plan_scan(&join.relation, parent_scope)?;
        let constraint = match &join.join_operator {
            ast::JoinOperator::Join(c)
            | ast::JoinOperator::Inner(c)
            | ast::JoinOperator::Left(c)
            | ast::JoinOperator::Right(c)
            | ast::JoinOperator::LeftOuter(c)
            | ast::JoinOperator::RightOuter(c) => c,
            _ => {
                return Err(Error::NotYetSupported(format!(
                    "Unsupported join operator: {:?}",
                    join.join_operator
                )));
            }
        };

        let on = match constraint {
            ast::JoinConstraint::None => None,
            ast::JoinConstraint::On(expr) => {
                let expr = Self::build_expr(expr, parent_scope)?;
                Some(expr)
            }
            _ => {
                return Err(Error::NotYetSupported(format!(
                    "Unsupported join constraint: {:?}",
                    constraint
                )));
            }
        };

        let join_type = match &join.join_operator {
            ast::JoinOperator::Join(_) => JoinType::Inner,
            ast::JoinOperator::Inner(_) => JoinType::Inner,
            ast::JoinOperator::Left(_) => JoinType::Left,
            ast::JoinOperator::Right(_) => JoinType::Right,
            ast::JoinOperator::LeftOuter(_) => JoinType::Left,
            ast::JoinOperator::RightOuter(_) => JoinType::Right,
            _ => return Err(Error::NotYetSupported(join.to_string())),
        };

        Ok(Plan::Join {
            left: Box::new(left),
            right: Box::new(right),
            on,
            join_type,
        })
    }

    fn build_aggregate(
        &self,
        func: &ast::Function,
        scope: &Scope,
    ) -> Result<Option<Aggregate>, Error> {
        log::debug!("Building aggregate: {}", func);
        if !is_aggregate(func) {
            return Ok(None);
        }
        let args = aggregate_function_args(func)?;
        if args.len() != 1 {
            return Err(Error::NotYetSupported(
                "Aggregate function with multiple arguments".to_string(),
            ));
        }
        let expr = Self::build_expr(&args[0], scope)?;
        let aggregate = match func.name.to_string().to_lowercase().as_str() {
            "avg" => Aggregate::Average(expr),
            "count" => Aggregate::Count(expr),
            "max" => Aggregate::Max(expr),
            "min" => Aggregate::Min(expr),
            "sum" => Aggregate::Sum(expr),
            _ => {
                return Err(Error::NotYetSupported({
                    format!("Unsupported aggregate function: {}", func.name)
                }));
            }
        };
        Ok(Some(aggregate))
    }

    fn collect_aggregates(
        &self,
        exprs: &[ast::SelectItem],
        scope: &Scope,
    ) -> Result<Vec<(ast::Function, Aggregate)>, Error> {
        log::debug!(
            "Collecting aggregates: {:?}",
            exprs.iter().map(|e| e.to_string()).collect::<Vec<_>>()
        );
        let mut aggregates = Vec::new();
        for item in exprs {
            match item {
                ast::SelectItem::UnnamedExpr(ast::Expr::Function(func)) => {
                    if func.over.is_none() {
                        if let Some(agg) = self.build_aggregate(func, scope)? {
                            aggregates.push((func.clone(), agg));
                        }
                    }
                }
                ast::SelectItem::ExprWithAlias {
                    expr: ast::Expr::Function(func),
                    ..
                } => {
                    if func.over.is_none() {
                        if let Some(agg) = self.build_aggregate(func, scope)? {
                            aggregates.push((func.clone(), agg));
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(aggregates)
    }

    fn build_expr(expr: &ast::Expr, scope: &Scope) -> Result<Expr, Error> {
        log::debug!("Building expression: {}", expr);
        match expr {
            ast::Expr::Function(func) => {
                if let Some(agg) = scope.get_aggregate_index(func) {
                    Ok(Expr::Column(agg))
                } else {
                    Err(Error::NotYetSupported(format!(
                        "Unsupported function: {}",
                        func
                    )))
                }
            }
            ast::Expr::BinaryOp { left, op, right } => {
                let left = Self::build_expr(left, scope)?;
                let right = Self::build_expr(right, scope)?;
                Ok(Expr::BinaryOp(
                    Box::new(left),
                    op.try_into()?,
                    Box::new(right),
                ))
            }
            ast::Expr::Value(v) => Ok(Expr::Constant(Value::try_from_ast(&v.value, None)?)),
            ast::Expr::Identifier(i) => {
                let name = i.value.clone();
                if let Some(index) = scope.get_column_index(None, &name) {
                    Ok(Expr::Column(index))
                } else {
                    Err(Error::InvalidColumnLabel(name.to_string()))
                }
            }
            ast::Expr::CompoundIdentifier(idents) => {
                if idents.len() != 2 {
                    return Err(Error::NotYetSupported(expr.to_string()));
                }
                let table = idents[0].value.clone();
                let column = idents[1].value.clone();
                match scope.get_column_index(Some(&table), &column) {
                    Some(index) => Ok(Expr::Column(index)),
                    None => Err(Error::InvalidColumnLabel(format!("{}.{}", table, column))),
                }
            }
            _ => Err(Error::NotYetSupported(format!(
                "Unsupported expression: {}",
                expr
            ))),
        }
    }
}
