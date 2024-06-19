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

use std::ops::{Deref, DerefMut};

use anyhow::{anyhow, Ok, Result};
use polars::prelude::*;
use sqlparser::{ast::Expr, parser::Parser};
use tracing::info;

use crate::{convert::Sql, dialect::OrinDialect, fetcher::retrieve_data, loader::detect_content};

#[derive(Debug)]
pub struct DataSet(pub DataFrame);

impl Deref for DataSet {
    type Target = DataFrame;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DataSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DataSet {
    fn to_csv(&mut self) -> Result<String> {
        let mut buf = vec![];
        let mut writer = CsvWriter::new(&mut buf);
        writer.finish(self);
        Ok(String::from_utf8(buf)?)
    }
}

pub async fn query<T: AsRef<str>>(sql: T) -> Result<DataSet> {
    println!("{}", sql.as_ref());
    let ast = Parser::parse_sql(&OrinDialect::default(), sql.as_ref())?;
    println!("{:#?}", ast);
    if ast.len() != 1 {
        Err::<T, anyhow::Error>(anyhow!("only support single sql at the monment"));
    }

    let sql1 = &ast[0];
    info!("sql ==================== {:#?}", sql1);
    let Sql {
        source,
        condition,
        selection,
        offset,
        limit,
        order_by,
    } = sql1.try_into()?;

    info!("retrieving data from {source}");
    info!("retrieving data from {:#?}", condition);

    let ds = detect_content(retrieve_data(source).await?).load()?;

    let mut filtered = match condition {
        Some(expr) => ds.0.lazy().filter(expr),
        None => ds.0.lazy(),
    };

    filtered = order_by.into_iter().fold(filtered, |acc, (col, desc)| {
        acc.sort(
            &[col],
            SortMultipleOptions::new().with_order_descending(desc),
        )
    });

    if offset.is_some() || limit.is_some() {
        filtered = filtered.slice(offset.unwrap_or(0), limit.unwrap_or(usize::MAX) as u32);
    }

    Ok(DataSet(filtered.select(selection).collect()?))
}
