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
