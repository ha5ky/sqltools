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
use std::default;

use sqlparser::{
    dialect::{self, Dialect, GenericDialect},
    parser::Parser,
};
#[derive(Debug, Default)]
pub struct OrinDialect;

// support identifier can be a url
impl Dialect for OrinDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ch == '_' || ch == '.'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ('0'..='9').contains(&ch)
            || [':', '<', '>', '/', '?', '&', '=', '_', '.', '-'].contains(&ch)
    }
}

fn example_sql() -> String {
    let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";
    // let url = "owid-covid-latest.csv";
    let sql = format!(
        "select location name, total_cases, new_cases, total_deaths, new_deaths \
    from {} \
    where new_deaths > 500 \
    order by new_cases desc \
    limit 6 offset 5",
        url
    );
    sql
}

#[cfg(test)]
mod test {
    use super::*;
    use sqlparser::parser::Parser;

    #[test]
    fn it_works() {
        let sql = example_sql();
        println!("{:#?}", Parser::parse_sql(&OrinDialect::default(), &sql));
        // assert!(Parser::parse_sql(&OrinDialect::default(), &sql).is_ok());
    }
}
