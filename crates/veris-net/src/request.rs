use derive_more::Display;
use serde::{Deserialize, Serialize};
use veris_db::types::value::Value;

#[derive(Debug, Serialize, Deserialize, Display)]
pub enum Request {
    Execute(String),
    Debug(String),
}

#[derive(Debug, Serialize, Deserialize, Display)]
pub enum Response {
    Execute(Value),
    Debug(String),
    Error(String),
}
