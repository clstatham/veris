use std::hash::Hash;

use chrono::NaiveDate;
use derive_more::{Deref, DerefMut, Display, From};
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{encoding::ValueEncoding, error::Error};

use super::schema::{ColumnName, TableName};

#[derive(Clone, Copy, Debug, PartialEq, Hash, Serialize, Deserialize, Eq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    Decimal {
        precision: Option<u64>,
        scale: Option<u64>,
    },
    String {
        length: Option<u64>,
    },
    Date,
}

impl TryFrom<&ast::DataType> for DataType {
    type Error = Error;

    fn try_from(value: &ast::DataType) -> Result<Self, Self::Error> {
        match value {
            ast::DataType::Boolean => Ok(DataType::Boolean),
            ast::DataType::Integer(_) | ast::DataType::Int(_) => Ok(DataType::Integer),
            ast::DataType::Float(_) => Ok(DataType::Float),
            ast::DataType::Decimal(dec) => match dec {
                ast::ExactNumberInfo::None => Ok(DataType::Decimal {
                    precision: None,
                    scale: None,
                }),
                ast::ExactNumberInfo::Precision(p) => Ok(DataType::Decimal {
                    precision: Some(*p),
                    scale: None,
                }),
                ast::ExactNumberInfo::PrecisionAndScale(p, s) => Ok(DataType::Decimal {
                    precision: Some(*p),
                    scale: Some(*s),
                }),
            },
            ast::DataType::String(length) => Ok(DataType::String { length: *length }),
            ast::DataType::Varchar(length) => Ok(DataType::String {
                length: (*length).map(|l| match l {
                    ast::CharacterLength::IntegerLength { length, .. } => length,
                    ast::CharacterLength::Max => u64::MAX,
                }),
            }),
            ast::DataType::Date => Ok(DataType::Date),
            _ => Err(Error::InvalidDataType(value.clone())),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Decimal {
                precision: Some(p),
                scale: Some(s),
            } => write!(f, "DECIMAL({}, {})", p, s),
            DataType::Decimal {
                precision: Some(p),
                scale: None,
            } => write!(f, "DECIMAL({})", p),
            DataType::Decimal {
                precision: None,
                scale: Some(s),
            } => write!(f, "DECIMAL(0, {})", s),
            DataType::Decimal {
                precision: None,
                scale: None,
            } => write!(f, "DECIMAL"),
            DataType::String { length } => match length {
                Some(l) => write!(f, "VARCHAR({})", l),
                None => write!(f, "VARCHAR"),
            },
            DataType::Date => write!(f, "DATE"),
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
    Date(NaiveDate),
}

impl ValueEncoding for Value {}

impl Value {
    pub fn is_truthy(&self) -> bool {
        matches!(self, Value::Boolean(true))
    }

    pub fn is_compatible(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (Value::Null, _) => true,
            (Value::Boolean(_), DataType::Boolean) => true,
            (Value::Integer(_), DataType::Integer) => true,
            (Value::Float(_), DataType::Float) => true,
            (Value::Float(f), DataType::Decimal { precision, scale }) => {
                if let Some(p) = precision {
                    if let Some(s) = scale {
                        let f_str = f.to_string();
                        if f_str.len() > *p as usize {
                            return false;
                        }
                        if let Some(dot_pos) = f_str.find('.') {
                            if f_str.len() - dot_pos - 1 > *s as usize {
                                return false;
                            }
                        }
                    } else {
                        let f_str = f.to_string();
                        if f_str.len() > *p as usize {
                            return false;
                        }
                    }
                }
                true
            }
            (Value::String(s), DataType::String { length }) => {
                length.is_none_or(|l| s.len() <= l as usize)
            }
            (Value::Date(_), DataType::Date) => true,

            (Value::String(s), DataType::Integer) => s.parse::<i64>().is_ok(),
            (Value::String(s), DataType::Float) => s.parse::<f64>().is_ok(),
            (Value::String(s), DataType::Date) => NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok(),

            _ => false,
        }
    }

    pub fn try_cast(&self, data_type: &DataType) -> Result<Value, Error> {
        match (self, data_type) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Boolean(b), DataType::Boolean) => Ok(Value::Boolean(*b)),
            (Value::Integer(i), DataType::Integer) => Ok(Value::Integer(*i)),
            (Value::Float(f), DataType::Float) => Ok(Value::Float(*f)),
            (Value::String(s), DataType::String { length }) => {
                if length.is_none_or(|l| s.len() <= l as usize) {
                    Ok(Value::String(s.clone()))
                } else {
                    Err(Error::InvalidCast(self.clone(), *data_type))
                }
            }
            (Value::Date(d), DataType::Date) => Ok(Value::Date(*d)),

            (Value::Float(f), DataType::Decimal { precision, scale }) => {
                if let Some(p) = precision {
                    if let Some(s) = scale {
                        let f_str = f.to_string();
                        if f_str.len() > *p as usize {
                            return Err(Error::InvalidCast(self.clone(), *data_type));
                        }
                        if let Some(dot_pos) = f_str.find('.') {
                            if f_str.len() - dot_pos - 1 > *s as usize {
                                return Err(Error::InvalidCast(self.clone(), *data_type));
                            }
                        }
                    } else {
                        let f_str = f.to_string();
                        if f_str.len() > *p as usize {
                            return Err(Error::InvalidCast(self.clone(), *data_type));
                        }
                    }
                }
                Ok(Value::Float(*f))
            }

            (Value::String(s), DataType::Integer) => s
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| Error::InvalidCast(self.clone(), *data_type)),
            (Value::String(s), DataType::Float) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| Error::InvalidCast(self.clone(), *data_type)),
            (Value::String(s), DataType::Date) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(Value::Date)
                .map_err(|_| Error::InvalidDate(s.clone())),

            _ => Err(Error::InvalidCast(self.clone(), *data_type)),
        }
    }

    pub fn try_from_ast(value: &ast::Value, type_hint: Option<DataType>) -> Result<Self, Error> {
        match value {
            ast::Value::Null => Ok(Value::Null),
            ast::Value::Boolean(b) => Ok(Value::Boolean(*b)),
            ast::Value::Number(n, _) => {
                if let Some(type_hint) = type_hint {
                    match type_hint {
                        DataType::Integer => {
                            if let Ok(i) = n.parse::<i64>() {
                                return Ok(Value::Integer(i));
                            }
                        }
                        DataType::Float => {
                            if let Ok(f) = n.parse::<f64>() {
                                return Ok(Value::Float(f));
                            }
                        }
                        DataType::Decimal { .. } => {
                            if let Ok(f) = n.parse::<f64>() {
                                return Ok(Value::Float(f));
                            }
                        }
                        _ => {}
                    }
                }
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Err(Error::InvalidValue(Box::new(value.clone())))
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                if let Some(type_hint) = type_hint {
                    match type_hint {
                        DataType::String { .. } => return Ok(Value::String(s.clone())),
                        DataType::Date => {
                            if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                return Ok(Value::Date(date));
                            } else {
                                return Err(Error::InvalidDate(s.clone()));
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Value::String(s.clone()))
            }

            _ => Err(Error::InvalidValue(Box::new(value.clone()))),
        }
    }

    pub fn is_undefined(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Float(f) if f.is_nan() => true,
            _ => false,
        }
    }

    pub fn checked_add(&self, other: &Self) -> Result<Self, Error> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(
                a.checked_add(*b).ok_or(Error::IntegerOverflow)?,
            )),
            (Value::Integer(a), Value::Float(b)) => Ok(Self::Float(*a as f64 + *b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Self::Float(*a + *b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Self::Float(*a + *b)),
            // todo
            _ => Err(Error::NotYetSupported(format!("{self} + {other}"))),
        }
    }

    pub fn checked_div(&self, other: &Self) -> Result<Self, Error> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(
                a.checked_div(*b).ok_or(Error::IntegerOverflow)?,
            )),
            (Value::Integer(a), Value::Float(b)) => Ok(Self::Float(*a as f64 / *b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Self::Float(*a / *b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Self::Float(*a / *b)),
            // todo
            _ => Err(Error::NotYetSupported(format!("{self} / {other}"))),
        }
    }

    pub fn checked_sub(&self, other: &Self) -> Result<Self, Error> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(
                a.checked_sub(*b).ok_or(Error::IntegerOverflow)?,
            )),
            (Value::Integer(a), Value::Float(b)) => Ok(Self::Float(*a as f64 - *b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Self::Float(*a - *b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Self::Float(*a - *b)),
            // todo
            _ => Err(Error::NotYetSupported(format!("{self} - {other}"))),
        }
    }

    pub fn checked_mul(&self, other: &Self) -> Result<Self, Error> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(
                a.checked_mul(*b).ok_or(Error::IntegerOverflow)?,
            )),
            (Value::Integer(a), Value::Float(b)) => Ok(Self::Float(*a as f64 * *b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Self::Float(*a * *b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Self::Float(*a * *b)),
            // todo
            _ => Err(Error::NotYetSupported(format!("{self} * {other}"))),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "'{}'", v),
            Value::Date(v) => write!(f, "'{}'", v),
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
            (Value::Date(a), Value::Date(b)) => a == b,
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
            Value::Date(v) => v.hash(state),
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
            (Value::Date(a), Value::Date(b)) => a.cmp(b),

            (Self::Null, _) => std::cmp::Ordering::Less,
            (_, Self::Null) => std::cmp::Ordering::Greater,
            (Self::Boolean(_), _) => std::cmp::Ordering::Less,
            (_, Self::Boolean(_)) => std::cmp::Ordering::Greater,
            (Self::Integer(_), _) => std::cmp::Ordering::Less,
            (_, Self::Integer(_)) => std::cmp::Ordering::Greater,
            (Self::Float(_), _) => std::cmp::Ordering::Less,
            (_, Self::Float(_)) => std::cmp::Ordering::Greater,
            (Self::String(_), _) => std::cmp::Ordering::Less,
            (_, Self::String(_)) => std::cmp::Ordering::Greater,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
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
#[display("{:?}", self.iter().as_slice())]
pub struct Row(Vec<Value>);

impl ValueEncoding for Row {}

impl FromIterator<Value> for Row {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        let vec: Vec<Value> = iter.into_iter().collect();
        Row(vec)
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub trait RowIter: Iterator<Item = Result<Row, Error>> + DynClone {}
dyn_clone::clone_trait_object!(RowIter);
impl<T: Iterator<Item = Result<Row, Error>> + DynClone> RowIter for T {}

pub type Rows = Box<dyn RowIter>;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnLabel {
    None,
    Unqualified(ColumnName),
    Qualified(TableName, ColumnName),
}

impl ColumnLabel {
    pub fn table_name(&self) -> Option<&TableName> {
        match self {
            ColumnLabel::None => None,
            ColumnLabel::Unqualified(_) => None,
            ColumnLabel::Qualified(table, _) => Some(table),
        }
    }
    pub fn column_name(&self) -> Option<&ColumnName> {
        match self {
            ColumnLabel::None => None,
            ColumnLabel::Unqualified(name) => Some(name),
            ColumnLabel::Qualified(_, name) => Some(name),
        }
    }
}

impl TryFrom<&ast::ObjectName> for ColumnLabel {
    type Error = Error;

    fn try_from(value: &ast::ObjectName) -> Result<Self, Self::Error> {
        if value.0.len() == 1 {
            Ok(ColumnLabel::Unqualified(ColumnName::new(
                value.0[0].to_string(),
            )))
        } else if value.0.len() == 2 {
            Ok(ColumnLabel::Qualified(
                TableName::new(value.0[0].to_string()),
                ColumnName::new(value.0[1].to_string()),
            ))
        } else {
            Err(Error::InvalidColumnLabel(value.to_string()))
        }
    }
}

impl std::fmt::Display for ColumnLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnLabel::None => write!(f, ""),
            ColumnLabel::Unqualified(name) => write!(f, "{}", name),
            ColumnLabel::Qualified(table, column) => write!(f, "{}.{}", table, column),
        }
    }
}
