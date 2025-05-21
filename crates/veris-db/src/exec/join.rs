use std::iter::Peekable;

use crate::{
    error::Error,
    types::value::{Row, RowIter, Value},
};

use super::expr::Expr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct NestedLoopJoiner {
    left: Peekable<RowIter>,
    right: Peekable<RowIter>,
    left_orig: Peekable<RowIter>,
    right_orig: Peekable<RowIter>,
    left_cols: usize,
    right_cols: usize,
    left_matched: bool,
    right_matched: bool,
    on: Option<Expr>,
    join_type: JoinType,
}

impl NestedLoopJoiner {
    pub fn new(
        left: RowIter,
        right: RowIter,
        left_cols: usize,
        right_cols: usize,
        on: Option<Expr>,
        join_type: JoinType,
    ) -> Self {
        let left = left.peekable();
        let right = right.peekable();
        let left_orig = left.clone();
        let right_orig = right.clone();
        Self {
            left,
            right,
            left_orig,
            right_orig,
            left_cols,
            right_cols,
            left_matched: false,
            right_matched: false,
            on,
            join_type,
        }
    }

    fn try_next(&mut self) -> Result<Option<Row>, Error> {
        match self.join_type {
            JoinType::Inner => self.inner_join(),
            JoinType::Left => self.left_join(),
            JoinType::Right => self.right_join(),
        }
    }

    fn inner_join(&mut self) -> Result<Option<Row>, Error> {
        while let Some(Ok(left)) = self.left.peek() {
            while let Some(right) = self.right.next().transpose()? {
                let row = left.iter().cloned().chain(right).collect();
                if let Some(pred) = &self.on {
                    match pred.eval(Some(&row))? {
                        Value::Boolean(true) => {}
                        Value::Boolean(false) | Value::Null => continue,
                        val => {
                            return Err(Error::InvalidFilterResult(val));
                        }
                    }
                }
                self.left_matched = true;
                self.right_matched = true;
                return Ok(Some(row));
            }

            self.right = self.right_orig.clone();
            self.right_matched = false;
            self.left_matched = false;
            self.left.next().transpose()?;
        }

        self.left.next().transpose()
    }

    fn left_join(&mut self) -> Result<Option<Row>, Error> {
        while let Some(Ok(left)) = self.left.peek() {
            while let Some(right) = self.right.next().transpose()? {
                let row = left.iter().cloned().chain(right).collect();
                if let Some(pred) = &self.on {
                    match pred.eval(Some(&row))? {
                        Value::Boolean(true) => {}
                        Value::Boolean(false) | Value::Null => continue,
                        val => {
                            return Err(Error::InvalidFilterResult(val));
                        }
                    }
                }
                self.right_matched = true;
                return Ok(Some(row));
            }

            if !self.right_matched {
                let null_row = std::iter::repeat_n(Value::Null, self.right_cols);
                let row = left.iter().cloned().chain(null_row).collect();
                self.right_matched = true;
                return Ok(Some(row));
            }

            self.right = self.right_orig.clone();
            self.right_matched = false;
            self.left.next().transpose()?;
        }

        self.left.next().transpose()
    }

    fn right_join(&mut self) -> Result<Option<Row>, Error> {
        Err(Error::NotYetSupported(
            "Right join is not implemented".to_string(),
        ))
    }
}

impl Iterator for NestedLoopJoiner {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
