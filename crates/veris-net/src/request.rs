use std::fmt;

use derive_more::Display;
use serde::{Deserialize, Serialize};
use veris_db::exec::session::StatementResult;

#[derive(Debug, Serialize, Deserialize, Display)]
pub enum Request {
    Execute(String),
    Debug(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(Vec<(String, StatementResult)>),
    Debug(String),
    Error(String),
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Response::Execute(results) => {
                for (sql, result) in results {
                    writeln!(f, "SQL: {sql}")?;
                    writeln!(f, "Result: {result}")?;
                }
            }
            Response::Debug(debug_info) => {
                writeln!(f, "Debug Info: {debug_info}")?;
            }
            Response::Error(error_msg) => {
                writeln!(f, "Error: {error_msg}")?;
            }
        }
        Ok(())
    }
}
