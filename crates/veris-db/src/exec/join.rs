use std::{collections::HashMap, iter::Peekable};

use crate::{
    error::Error,
    types::{
        schema::ColumnIndex,
        value::{Row, Rows, Value},
    },
};

use super::expr::Expr;

#[derive(Clone)]
pub struct NestedLoopJoiner {
    left: Peekable<Rows>,
    right: Rows,
    right_orig: Rows,
    right_cols: usize,
    right_matched: bool,
    predicate: Option<Expr>,
    outer: bool,
}

impl NestedLoopJoiner {
    pub fn new(
        left: Rows,
        right: Rows,
        right_cols: usize,
        predicate: Option<Expr>,
        outer: bool,
    ) -> Self {
        Self {
            left: left.peekable(),
            right_orig: right.clone(),
            right,
            right_cols,
            right_matched: false,
            predicate,
            outer,
        }
    }

    fn try_next(&mut self) -> Result<Option<Row>, Error> {
        while let Some(Ok(left)) = self.left.peek() {
            while let Some(right) = self.right.next().transpose()? {
                let row = left.iter().cloned().chain(right).collect();
                if let Some(predicate) = self.predicate.as_ref() {
                    match predicate.evaluate(Some(&row))? {
                        Value::Boolean(true) => {
                            return Ok(Some(row));
                        }
                        Value::Boolean(false) | Value::Null => continue,
                        result => {
                            return Err(Error::InvalidFilterResult(result));
                        }
                    }
                }
                self.right_matched = true;
                return Ok(Some(row));
            }

            // no right match
            if !self.right_matched && self.outer {
                self.right_matched = true;
                return Ok(Some(
                    left.iter()
                        .cloned()
                        .chain(std::iter::repeat_n(Value::Null, self.right_cols))
                        .collect(),
                ));
            }

            // end of right source, on the the next left row
            self.right = self.right_orig.clone();
            self.right_matched = false;
            self.left.next().transpose()?;
        }

        self.left.next().transpose()
    }
}

impl Iterator for NestedLoopJoiner {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

#[derive(Clone)]
pub struct HashJoiner {
    left: Rows,
    left_col: ColumnIndex,
    right: HashMap<Value, Vec<Row>>,
    right_cols: usize,
    outer: bool,
    pending: Rows,
}

impl HashJoiner {
    pub fn new(
        left: Rows,
        left_col: ColumnIndex,
        mut right: Rows,
        right_col: ColumnIndex,
        right_cols: usize,
        outer: bool,
    ) -> Result<Self, Error> {
        let mut right_map: HashMap<Value, Vec<Row>> = HashMap::new();
        while let Some(row) = right.next().transpose()? {
            let value = row[*right_col.inner()].clone();
            if value == Value::Null {
                continue;
            }
            right_map.entry(value).or_default().push(row);
        }
        Ok(Self {
            left,
            left_col,
            right: right_map,
            right_cols,
            outer,
            pending: Box::new(std::iter::empty()),
        })
    }

    fn try_next(&mut self) -> Result<Option<Row>, Error> {
        if let Some(row) = self.pending.next().transpose()? {
            return Ok(Some(row));
        }

        while let Some(left) = self.left.next().transpose()? {
            if let Some(right) = self.right.get(&left[*self.left_col.inner()]).cloned() {
                self.pending = Box::new(
                    right
                        .into_iter()
                        .map(move |right| left.iter().cloned().chain(right).collect())
                        .map(Ok),
                );
                return self.pending.next().transpose();
            } else if self.outer {
                return Ok(Some(
                    left.into_iter()
                        .chain(std::iter::repeat_n(Value::Null, self.right_cols))
                        .collect(),
                ));
            }
        }

        Ok(None)
    }
}

impl Iterator for HashJoiner {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
