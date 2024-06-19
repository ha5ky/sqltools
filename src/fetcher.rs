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
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use tokio::fs;

#[async_trait]
pub trait Fetcher {
    type Error;
    async fn fetch(&self) -> Result<String, Self::Error>;
}

struct UrlFetcher<'a>(pub(crate) &'a str);

#[async_trait]
impl<'a> Fetcher for UrlFetcher<'a> {
    type Error = Error;

    // http://
    async fn fetch(&self) -> Result<String, Self::Error> {
        Ok(reqwest::get(self.0).await?.text().await?)
    }
}
struct FileFetcher<'a>(pub(crate) &'a str);

#[async_trait]
impl<'a> Fetcher for FileFetcher<'a> {
    type Error = Error;

    // file://
    async fn fetch(&self) -> Result<String, Self::Error> {
        let data = fs::read_to_string(&self.0[7..]).await?;
        Ok(data)
        // Ok(fs::read_to_string(&self.0[7..]).await?)
    }
}

pub async fn retrieve_data(source: impl AsRef<str>) -> Result<String, Error> {
    println!("========= {:#?}",source.as_ref());
    match &source.as_ref()[..4] {
        "http" => UrlFetcher(source.as_ref()).fetch().await,
        "file" => FileFetcher(source.as_ref()).fetch().await,
        _ => Err(anyhow!(
            "not support scheme, we only support http/https/file at the moment."
        )),
    }
}

// TODO: we will extract the scheme before retieve_data, retrieve_data's pattern shouldn't use such as
//          "http" => UrlFetcher(source.as_ref()).fetch().await,
//          "file" => FileFetcher(source.as_ref()).fetch().await,
//
// fn extract_scheme(source: impl AsRef<str>+ 'static) -> Result<Box<dyn Fetcher>> {
//     match &source.as_ref()[..4] {
//         "http" => Ok(Box::new(UrlFetcher(source.as_ref().to_owned()))),
//         "file" => Ok(Box::new(FileFetcher(source.as_ref()))),
//         _ => Err(anyhow!(
//             "not support scheme, we only support http/https/file at the moment."
//         )),
//     }
// }
