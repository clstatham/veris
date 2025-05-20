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
    Execute(Vec<StatementResult>),
    Debug(String),
    Error(String),
}
