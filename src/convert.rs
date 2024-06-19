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
use anyhow::{anyhow, Error, Ok};
use polars::prelude::*;
use sqlparser::ast::{
    Expr as SqlExpr, Offset as SqlOffset, OrderByExpr, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value
};

#[derive(Debug, Default)]
pub struct Sql<'a> {
    pub(crate) selection: Vec<Expr>,
    pub(crate) condition: Option<Expr>,
    pub(crate) source: &'a str,
    pub(crate) order_by: Vec<(&'a str, bool)>,
    pub(crate) offset: Option<i64>,
    pub(crate) limit: Option<usize>,
}

// TODO: improving the rest of Sql attributes.
#[derive(Debug)]
pub struct Offset(pub(crate) SqlOffset);
#[derive(Debug)]
pub struct Source<'a>(pub(crate) &'a [TableWithJoins]);
#[derive(Debug)]
pub struct OrderBy<'a>(pub(crate) &'a OrderByExpr);
#[derive(Debug)]
pub struct Limit(pub(crate) SqlExpr);
#[derive(Debug)]
pub struct Condition(pub(crate) Option<SqlExpr>);
#[derive(Debug)]
pub struct Projection(pub(crate) SelectItem);

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
                let source = table_with_joins;
                Ok(Sql::default())
            }
            _ => Ok(Sql::default()),
        }
    }
}

impl<'a> From<Offset> for i64 {
    fn from(offset: Offset) -> Self {
        match offset.0 {
            SqlOffset {
                value: SqlExpr::Value(Value::Number(v, _)),
                ..
            } => v.parse().unwrap_or(0),
            _ => 0,
        }
    }
}

impl<'a> TryFrom<Source<'a>> for &'a str {
    type Error = Error;
    fn try_from(source: Source<'a>) -> Result<Self, Self::Error> {
        if source.0.len() != 1 {
            return Err(anyhow!(
                "we only support one single data source at the moment"
            ));
        }

        if source.0.len() == 0 {
            return Err(anyhow!("there is no data source"));
        }

        let table = source.0.first().unwrap();
        if !table.joins.is_empty() {
            return Err(anyhow!("We do not support joint data source at the moment"));
        }
        match &table.relation {
            TableFactor::Table { name, .. } => Ok(&name.0.first().unwrap().value),
            _ => Err(anyhow!("we only support table new")),
        }
    }
}

impl<'a> TryFrom<OrderBy<'a>> for (String, bool) {
    type Error = Error;
    fn try_from(order_by: OrderBy<'a>) -> Result<Self, Self::Error> {
        let name = match &order_by.0.expr {
            SqlExpr::Identifier(id) => id.to_string(),
            expr => {
                return Err(anyhow!(
                    "we only support inentifier for order by, got {}",
                    expr
                ))
            }
        };
        let asc = order_by.0.asc.unwrap_or(true);
        Ok((name, asc))
    }
}
