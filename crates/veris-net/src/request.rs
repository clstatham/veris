use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
    Debug(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(()),
    Debug(String),
    Error(String),
}
