#[cfg(test)]
pub mod tests {
    #[macro_export]
    macro_rules! sql_stmt {
        ($stmt:ident, $sql:expr) => {{
            use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};
            let stmts = Parser::parse_sql(&GenericDialect {}, $sql).unwrap();
            let [ref stmt] = stmts[..] else {
                panic!("more than one statement")
            };
            let Statement::$stmt(stmt) = stmt.clone() else {
                panic!("statement type mismatch")
            };
            stmt
        }};
    }
}
