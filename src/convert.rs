use anyhow::{anyhow, Error};
use sqlparser::ast::{Expr, Offset as SqlOffset, OffsetRows, Select, SetExpr, Statement, Value};

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
                value: Expr::Value(Value::Number(v, _b)),
                ..
            } => v.parse().unwrap_or(0),
            _ => 0,
        }
    }
}
