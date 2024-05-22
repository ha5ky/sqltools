// MIT License
//
// Copyright (c) 2024 hu5ky
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use anyhow::{anyhow, Error};
use sqlparser::ast::{Expr, Offset as SqlOffset, Select, SetExpr, Statement, Value};

#[derive(Debug, Default)]
pub struct Sql<'a> {
    pub(crate) selection: Vec<Expr>,
    pub(crate) condition: Option<Expr>,
    pub(crate) source: &'a str,
    pub(crate) order_by: Vec<(&'a str, bool)>,
    pub(crate) offset: Option<i64>,
    pub(crate) limit: Option<usize>,
}

impl<'a> TryFrom<&'a Statement> for Sql<'a> {
    type Error = Error;

    fn try_from(sql: &'a Statement) -> Result<Self, Self::Error> {
        match sql {
            Statement::Query(q) => {
                let Select {
                    from: table_with_joins,
                    selection: where_clause,
                    projection,

                    group_by: _,
                    ..
                } = match &*q.body {
                    SetExpr::Select(statement) => statement.as_ref(),
                    _ => return Err(anyhow!("We only support Select Query at the moment")),
                };
                Ok(Sql::default())
            }
            _ => Ok(Sql::default()),
        }
    }
}

#[derive(Debug)]
// pub struct Offset<'a>(pub(crate) &'a SqlOffset);
pub struct Offset(pub(crate) SqlOffset);

// impl<'a> From<Offset<'a>> for i64 {
//     fn from(offset: Offset) -> Self {
//          match offset.0 {
//             SqlOffset {
//                 value: Expr::Value(Value::Number(v, _b)),
//                 ..
//             } => v.parse().unwrap_or(0),
//             _ => 0,
//         }
//     }
// }
impl<'a> From<Offset> for i64 {
    fn from(offset: Offset) -> Self {
        match offset.0 {
            SqlOffset {
                value: Expr::Value(Value::Number(v, _)),
                ..
            } => v.parse().unwrap_or(0),
            _ => 0,
        }
    }
}
