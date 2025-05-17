use std::hash::Hash;

use derive_more::{Deref, DerefMut, Display};
use serde::{Deserialize, Serialize};
use sqlparser::ast;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Hash, Serialize, Deserialize, Display)]
pub enum DataType {
    #[display("BOOLEAN")]
    Boolean,
    #[display("INTEGER")]
    Integer,
    #[display("FLOAT")]
    Float,
    #[display("STRING")]
    String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConvertDataTypeError {
    #[error("Invalid data type: {0}")]
    InvalidDataType(ast::DataType),
}

impl TryFrom<&ast::DataType> for DataType {
    type Error = ConvertDataTypeError;

    fn try_from(value: &ast::DataType) -> Result<Self, Self::Error> {
        match value {
            ast::DataType::Boolean => Ok(DataType::Boolean),
            ast::DataType::Integer(_) | ast::DataType::Int(_) => Ok(DataType::Integer),
            ast::DataType::Float(_) => Ok(DataType::Float),
            ast::DataType::String(_) => Ok(DataType::String),
            _ => Err(ConvertDataTypeError::InvalidDataType(value.clone())),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Integer(_) => Some(DataType::Integer),
            Value::Float(_) => Some(DataType::Float),
            Value::String(_) => Some(DataType::String),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Integer(a), Value::Float(b)) => *a as f64 == *b,
            (Value::Float(a), Value::Integer(b)) => *a == *b as f64,
            (Value::Float(a), Value::Float(b)) => a == b || a.is_nan() && b.is_nan(),
            (Value::String(a), Value::String(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::Float(v) => {
                if (v.is_nan() || *v == 0.0) && v.is_sign_negative() {
                    (-v).to_bits().hash(state);
                } else {
                    v.to_bits().hash(state);
                }
            }
            Value::String(v) => v.hash(state),
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.total_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),

            (Self::Null, _) => std::cmp::Ordering::Less,
            (_, Self::Null) => std::cmp::Ordering::Greater,
            (Self::Boolean(_), _) => std::cmp::Ordering::Less,
            (_, Self::Boolean(_)) => std::cmp::Ordering::Greater,
            (Self::Integer(_), _) => std::cmp::Ordering::Less,
            (_, Self::Integer(_)) => std::cmp::Ordering::Greater,
            (Self::Float(_), _) => std::cmp::Ordering::Less,
            (_, Self::Float(_)) => std::cmp::Ordering::Greater,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("Error converting value from AST")]
pub enum ConvertValueError {
    #[error("Invalid value: {0}")]
    InvalidValue(Box<ast::Value>),
}

impl TryFrom<&ast::Value> for Value {
    type Error = ConvertValueError;

    fn try_from(value: &ast::Value) -> Result<Self, Self::Error> {
        match value {
            ast::Value::Null => Ok(Value::Null),
            ast::Value::Boolean(b) => Ok(Value::Boolean(*b)),
            ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Err(ConvertValueError::InvalidValue(Box::new(value.clone())))
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                Ok(Value::String(s.clone()))
            }

            _ => Err(ConvertValueError::InvalidValue(Box::new(value.clone()))),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Deref, DerefMut)]
pub struct Row(Vec<Value>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_data_type() {
        let ast_data_type = ast::DataType::Boolean;
        let data_type = DataType::try_from(&ast_data_type).unwrap();
        assert_eq!(data_type, DataType::Boolean);

        let ast_data_type = ast::DataType::Integer(None);
        let data_type = DataType::try_from(&ast_data_type).unwrap();
        assert_eq!(data_type, DataType::Integer);

        let ast_data_type = ast::DataType::Float(None);
        let data_type = DataType::try_from(&ast_data_type).unwrap();
        assert_eq!(data_type, DataType::Float);

        let ast_data_type = ast::DataType::String(None);
        let data_type = DataType::try_from(&ast_data_type).unwrap();
        assert_eq!(data_type, DataType::String);
    }

    #[test]
    fn test_convert_invalid_data_type() {
        let ast_data_type = ast::DataType::Date;
        let result = DataType::try_from(&ast_data_type);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ConvertDataTypeError::InvalidDataType(ast_data_type)
        );
    }

    #[test]
    fn test_convert_value() {
        let ast_value = ast::Value::Boolean(true);
        let value = Value::try_from(&ast_value).unwrap();
        assert_eq!(value, Value::Boolean(true));

        let ast_value = ast::Value::Number("42".to_string(), false);
        let value = Value::try_from(&ast_value).unwrap();
        assert_eq!(value, Value::Integer(42));

        let ast_value = ast::Value::SingleQuotedString("Hello".to_string());
        let value = Value::try_from(&ast_value).unwrap();
        assert_eq!(value, Value::String("Hello".to_string()));
    }

    #[test]
    fn test_convert_invalid_value() {
        let ast_value = ast::Value::Null;
        let result = Value::try_from(&ast_value);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);

        let ast_value = ast::Value::Number("invalid".to_string(), false);
        let result = Value::try_from(&ast_value);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ConvertValueError::InvalidValue(Box::new(ast_value))
        );
    }
}
