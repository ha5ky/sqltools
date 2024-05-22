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
// be used to understand and use DataFrame
use anyhow::{Ok, Result};
use polars::prelude::*;
use std::{
    env,
    fs::File,
    io::{Cursor, Read},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";
    // let url = "http://vidp-dr-dev-oss.oss-cn-hangzhou.aliyuncs.com/test/owid-covid-latest.csv";
    let curr_pwd = env::current_exe()?;
    println!("{:?}", curr_pwd);
    let url = "../../../owid-covid-latest.csv";
    let mut buf = String::new();
    let data = File::open(url)?.read_to_string(&mut buf)?;
    println!("{}", buf);
    // let data = reqwest::get(url).await?.text().await?;
    // println!("{}", data);

    // 使用polars直接请求
    let df = CsvReader::new(Cursor::new(buf))
        .infer_schema(Some(16))
        .finish()?;

    let mask = df.column("new_deaths")?.gt(5)?;
    let filtered = df.filter(&mask)?;
    // let filtered = df.filter(&df["new_deaths"].gt(500))?;
    println!(
        "{:?}",
        filtered.select([
            "location",
            "total_cases",
            "new_cases",
            "total_deaths",
            "new_deaths"
        ])
    );
    Ok(())
}
