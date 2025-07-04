use std::{fmt, hash::Hash};

use chrono::NaiveDate;
use derive_more::{Deref, DerefMut, Index, IndexMut, Into, IntoIterator};
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use sqlparser::ast;

use crate::{Result, encoding::ValueEncoding, error::Error};

/// The data type of a value in the database.
#[derive(Clone, Default, Copy, Debug, PartialEq, Hash, Serialize, Deserialize, Eq)]
pub enum DataType {
    /// A boolean value.
    Boolean,

    /// An integer value.
    #[default]
    Integer,

    /// A floating-point value.
    Float,

    /// A decimal value.
    Decimal {
        /// The maximum number of digits in the number.
        precision: Option<u64>,
        /// The number of digits to the right of the decimal point.
        scale: Option<u64>,
    },

    /// A string value.
    String {
        /// The maximum length of the string.
        length: Option<u64>,
    },

    /// A date value.
    Date,
}

impl TryFrom<&ast::DataType> for DataType {
    type Error = Error;

    fn try_from(value: &ast::DataType) -> Result<Self> {
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

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Decimal {
                precision: Some(p),
                scale: Some(s),
            } => write!(f, "DECIMAL({},{})", p, s),
            DataType::Decimal {
                precision: Some(p),
                scale: None,
            } => write!(f, "DECIMAL({})", p),
            DataType::Decimal {
                precision: None,
                scale: Some(s),
            } => write!(f, "DECIMAL(0,{})", s),
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

/// A value in the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// A null value.
    Null,

    /// A boolean value.
    Boolean(bool),

    /// An integer value.
    Integer(i64),

    /// A floating-point value.
    Float(f64),

    /// A string value.
    String(String),

    /// A date value.
    Date(NaiveDate),
}

impl ValueEncoding for Value {}

impl Value {
    /// Returns whether the value is a true boolean value.
    pub fn is_truthy(&self) -> bool {
        matches!(self, Value::Boolean(true))
    }

    /// Checks if the value is compatible with the given data type.
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

    /// Attempts to cast the value to the given data type.
    pub fn try_cast(&self, data_type: &DataType) -> Result<Value> {
        match (self, data_type) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Boolean(b), DataType::Boolean) => Ok(Value::Boolean(*b)),
            (Value::Integer(i), DataType::Integer) => Ok(Value::Integer(*i)),
            (Value::Float(f), DataType::Float) => Ok(Value::Float(*f)),
            (Value::String(s), DataType::String { length }) => {
                if length.is_none_or(|l| s.len() <= l as usize) {
                    Ok(Value::String(s.clone()))
                } else {
                    Err(Error::InvalidCast {
                        value: self.clone(),
                        to: *data_type,
                    })
                }
            }
            (Value::Date(d), DataType::Date) => Ok(Value::Date(*d)),

            (Value::Float(f), DataType::Decimal { precision, scale }) => {
                if let Some(p) = precision {
                    if let Some(s) = scale {
                        let f_str = f.to_string();
                        if f_str.len() > *p as usize {
                            return Err(Error::InvalidCast {
                                value: self.clone(),
                                to: *data_type,
                            });
                        }
                        if let Some(dot_pos) = f_str.find('.') {
                            if f_str.len() - dot_pos - 1 > *s as usize {
                                return Err(Error::InvalidCast {
                                    value: self.clone(),
                                    to: *data_type,
                                });
                            }
                        }
                    } else {
                        let f_str = f.to_string();
                        if f_str.len() > *p as usize {
                            return Err(Error::InvalidCast {
                                value: self.clone(),
                                to: *data_type,
                            });
                        }
                    }
                }
                Ok(Value::Float(*f))
            }

            (Value::String(s), DataType::Integer) => {
                s.parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| Error::InvalidCast {
                        value: self.clone(),
                        to: *data_type,
                    })
            }
            (Value::String(s), DataType::Float) => {
                s.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| Error::InvalidCast {
                        value: self.clone(),
                        to: *data_type,
                    })
            }
            (Value::String(s), DataType::Date) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(Value::Date)
                .map_err(|_| Error::InvalidDate(s.clone())),

            _ => Err(Error::InvalidCast {
                value: self.clone(),
                to: *data_type,
            }),
        }
    }

    /// Attempts to create a value from an AST value and an optional type hint.
    pub fn try_from_ast(value: &ast::Value, type_hint: Option<DataType>) -> Result<Self> {
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

    /// Returns whether the value is undefined (null or NaN).
    pub fn is_undefined(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Float(f) if f.is_nan() => true,
            _ => false,
        }
    }

    /// Attempts to add two values together.
    /// This may result in a different type than one of the original values (e.g. adding an integer and a float results in a float).
    pub fn checked_add(&self, other: &Self) -> Result<Self> {
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

    /// Attempts to divide two values.
    /// This may result in a different type than one of the original values (e.g. dividing an integer by a float results in a float).
    pub fn checked_div(&self, other: &Self) -> Result<Self> {
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

    /// Attempts to subtract two values.
    /// This may result in a different type than one of the original values (e.g. subtracting an integer from a float results in a float).
    pub fn checked_sub(&self, other: &Self) -> Result<Self> {
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

    /// Attempts to multiply two values.
    /// This may result in a different type than one of the original values (e.g. multiplying an integer and a float results in a float).
    pub fn checked_mul(&self, other: &Self) -> Result<Self> {
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

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<NaiveDate> for Value {
    fn from(value: NaiveDate) -> Self {
        Value::Date(value)
    }
}

/// A row of values in the database.
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
    Index,
    IndexMut,
)]
pub struct Row(Vec<Value>);

impl ValueEncoding for Row {}

impl Row {
    /// Creates a new row from a vector of values.
    pub fn new(values: impl Into<Row>) -> Self {
        values.into()
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.iter().map(|v| v.to_string()))
            .finish()
    }
}

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

impl From<Vec<Value>> for Row {
    fn from(value: Vec<Value>) -> Self {
        Row(value)
    }
}

impl<const N: usize> From<[Value; N]> for Row {
    fn from(value: [Value; N]) -> Self {
        Row(value.to_vec())
    }
}

macro_rules! impl_into_row {
    ($($name:ident),+) => {
        #[allow(non_snake_case)]
        impl<$($name),+> From<($($name,)+)> for Row
        where
            $($name: Into<Value>,)+
        {
            fn from(value: ($($name,)+)) -> Self {
                let ($($name,)+) = value;
                Row(vec![$($name.into()),+])
            }
        }
    };
}

impl_into_row!(A);
impl_into_row!(A, B);
impl_into_row!(A, B, C);
impl_into_row!(A, B, C, D);
impl_into_row!(A, B, C, D, E);
impl_into_row!(A, B, C, D, E, F);
impl_into_row!(A, B, C, D, E, F, G);
impl_into_row!(A, B, C, D, E, F, G, H);

/// A trait for iterating over rows.
/// It is implemented for any cloneable type that implements the `Iterator` trait and returns `Result<Row>`.
pub trait RowIterImpl: Iterator<Item = Result<Row>> + DynClone {}
dyn_clone::clone_trait_object!(RowIterImpl);
impl<T: Iterator<Item = Result<Row>> + DynClone> RowIterImpl for T {}

/// A wrapper around a row iterator.
/// This allows for dynamic dispatch of row iterators.
#[derive(Clone, Deref, DerefMut, Into)]
pub struct RowIter(Box<dyn RowIterImpl>);

impl RowIter {
    /// Creates a new `RowIter` from a row iterator.
    pub fn new(rows: impl RowIterImpl + 'static) -> Self {
        RowIter(Box::new(rows))
    }

    /// Creates a new `RowIter` from a boxed row iterator.
    pub fn new_boxed(rows: Box<dyn RowIterImpl>) -> Self {
        RowIter(rows)
    }
}

impl Iterator for RowIter {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

/// A collection of rows.
/// This is a wrapper around a vector of `Row` objects.
#[derive(Clone, Debug, Serialize, Deserialize, Deref, DerefMut, IntoIterator)]
pub struct Rows(Vec<Row>);

impl Rows {
    /// Creates a new `Rows` object.
    pub fn new(rows: impl Into<Vec<Row>>) -> Self {
        Rows(rows.into())
    }

    /// Creates a new `Rows` object from a vector of rows.
    pub fn from_vec(rows: Vec<Row>) -> Self {
        Rows(rows)
    }

    /// Consumes and converts the `Rows` object into a vector of rows.
    pub fn into_row_vec(self) -> Vec<Row> {
        self.0
    }
}

macro_rules! impl_into_rows {
    ($($name:ident),+) => {
        #[allow(non_snake_case)]
        impl<$($name),+> From<($($name,)+)> for Rows
        where
            $($name: Into<Row>,)+
        {
            fn from(value: ($($name,)+)) -> Self {
                let ($($name,)+) = value;
                Rows(vec![$($name.into()),+])
            }
        }
    };
}

impl_into_rows!(A);
impl_into_rows!(A, B);
impl_into_rows!(A, B, C);
impl_into_rows!(A, B, C, D);
impl_into_rows!(A, B, C, D, E);
impl_into_rows!(A, B, C, D, E, F);
impl_into_rows!(A, B, C, D, E, F, G);
impl_into_rows!(A, B, C, D, E, F, G, H);

/// A label for a column in a query.
/// This can be either a qualified name (e.g. `table.column`), an unqualified name (e.g. `column`),
/// or no name at all.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnLabel {
    /// No label.
    None,
    /// An unqualified label (e.g. `column`).
    Unqualified(
        /// The name of the column.
        String,
    ),
    /// A qualified label (e.g. `table.column`).
    Qualified(
        /// The name of the table.
        String,
        /// The name of the column.
        String,
    ),
}

impl ColumnLabel {
    pub fn table_name(&self) -> Option<&String> {
        match self {
            ColumnLabel::None => None,
            ColumnLabel::Unqualified(_) => None,
            ColumnLabel::Qualified(table, _) => Some(table),
        }
    }
    pub fn column_name(&self) -> Option<&String> {
        match self {
            ColumnLabel::None => None,
            ColumnLabel::Unqualified(name) => Some(name),
            ColumnLabel::Qualified(_, name) => Some(name),
        }
    }
}

impl TryFrom<&ast::ObjectName> for ColumnLabel {
    type Error = Error;

    fn try_from(value: &ast::ObjectName) -> Result<Self> {
        if value.0.len() == 1 {
            Ok(ColumnLabel::Unqualified(value.0[0].to_string()))
        } else if value.0.len() == 2 {
            Ok(ColumnLabel::Qualified(
                value.0[0].to_string(),
                value.0[1].to_string(),
            ))
        } else {
            Err(Error::InvalidColumnLabel(value.to_string()))
        }
    }
}

impl fmt::Display for ColumnLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnLabel::None => write!(f, ""),
            ColumnLabel::Unqualified(name) => write!(f, "{}", name),
            ColumnLabel::Qualified(table, column) => write!(f, "{}.{}", table, column),
        }
    }
}
