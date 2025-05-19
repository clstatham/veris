use std::hash::Hash;

use derive_more::{Deref, DerefMut, Display, From};
use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{encoding::ValueEncoding, error::Error};

use super::schema::{ColumnName, TableName};

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

impl TryFrom<&ast::DataType> for DataType {
    type Error = Error;

    fn try_from(value: &ast::DataType) -> Result<Self, Self::Error> {
        match value {
            ast::DataType::Boolean => Ok(DataType::Boolean),
            ast::DataType::Integer(_) | ast::DataType::Int(_) => Ok(DataType::Integer),
            ast::DataType::Float(_) => Ok(DataType::Float),
            ast::DataType::String(_) => Ok(DataType::String),
            _ => Err(Error::InvalidDataType(value.clone())),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Display)]
pub enum Value {
    #[display("NULL")]
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl ValueEncoding for Value {}

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

impl TryFrom<&ast::Value> for Value {
    type Error = Error;

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
                    Err(Error::InvalidValue(Box::new(value.clone())))
                }
            }
            ast::Value::SingleQuotedString(s) => Ok(Value::String(s.clone())),

            _ => Err(Error::InvalidValue(Box::new(value.clone()))),
        }
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deref,
    DerefMut,
    Display,
    From,
)]
#[display("{:?}", self.0)]
pub struct Row(Box<[Value]>);

impl ValueEncoding for Row {}

impl FromIterator<Value> for Row {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        let vec: Vec<Value> = iter.into_iter().collect();
        Row(vec.into_boxed_slice())
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub trait RowIter: Iterator<Item = Result<Row, Error>> {}
// dyn_clone::clone_trait_object!(RowIter);
impl<T: Iterator<Item = Result<Row, Error>>> RowIter for T {}

pub type Rows = Box<dyn RowIter>;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum ColumnLabel {
    #[display("")]
    None,
    #[display("{}", 0)]
    Unqualified(ColumnName),
    #[display("{}.{}", 0, 1)]
    Qualified(TableName, ColumnName),
}

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
        assert_eq!(result.unwrap_err(), Error::InvalidDataType(ast_data_type));
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
            Error::InvalidValue(Box::new(ast_value))
        );
    }
}
