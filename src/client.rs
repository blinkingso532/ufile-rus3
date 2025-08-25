use std::{collections::HashMap, str::FromStr, time::Duration};

use anyhow::Error;
use reqwest::{
    Body, Client, ClientBuilder, Method, Proxy, Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::result::Result;
use tokio::fs::File;

use crate::api::{ProgressStream, object::BaseResponse};

#[repr(transparent)]
#[derive(Clone)]
pub struct HttpClient {
    inner: Client,
}

pub struct HttpClientBuilder {
    builder: ClientBuilder,
}

impl HttpClient {
    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder::new()
    }

    pub fn get_client(&self) -> &Client {
        &self.inner
    }

    pub fn into_inner(self) -> Client {
        self.inner
    }
}

impl HttpClientBuilder {
    pub fn new() -> Self {
        Self {
            builder: ClientBuilder::new()
                .connect_timeout(Duration::from_secs(5))
                // read timeout set to 30 seconds.
                .read_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(3600))
                // 5 minutes not working then release connection.
                .pool_idle_timeout(Duration::from_secs(300))
                .pool_max_idle_per_host(5)
                // only support http1
                .http1_only()
                .user_agent(format!("ufile-rus3-sdk/{}", crate::VERSION)),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.timeout(timeout);
        self
    }

    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.builder = self.builder.connect_timeout(connect_timeout);
        self
    }

    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.builder = self.builder.default_headers(headers);
        self
    }

    pub fn with_proxy(mut self, proxy: Proxy) -> Self {
        self.builder = self.builder.proxy(proxy);
        self
    }

    pub fn with_pool_idle_timeout(mut self, pool_idle_timeout: Duration) -> Self {
        self.builder = self.builder.pool_idle_timeout(pool_idle_timeout);
        self
    }

    pub fn with_read_timeout(mut self, read_timeout: Duration) -> Self {
        self.builder = self.builder.read_timeout(read_timeout);
        self
    }

    pub fn with_max_idle_per_host(mut self, max_idle_per_host: usize) -> Self {
        self.builder = self.builder.pool_max_idle_per_host(max_idle_per_host);
        self
    }

    pub fn build(self) -> Result<HttpClient, Error> {
        Ok(HttpClient {
            inner: self.builder.build()?,
        })
    }
}
impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl HttpClient {
    /// This method support only files which is smaller than 512MB,
    /// otherwise, will return error.
    /// If you are trying to upload a file that is more than 512MB, please use multipart upload which
    /// is supported by ucloud (Which should create multiple slices to upload).
    pub async fn send_file(
        &self,
        url: &str,
        method: Method,
        headers: &[(&str, &str)],
        file: File,
    ) -> Result<BaseResponse, Error> {
        let headers = headers
            .iter()
            .map(|(header_name, header_value)| {
                Ok((
                    HeaderName::from_str(header_name).map_err(|e| Error::msg(e.to_string()))?,
                    HeaderValue::from_str(header_value).map_err(|e| Error::msg(e.to_string()))?,
                ))
            })
            .collect::<Result<HeaderMap, Error>>()?;
        // Check authorization
        let signature = headers.get("Authorization");
        if signature.is_none() {
            return Err(Error::msg("No authorization header found"));
        }
        let std_file = file.into_std().await;
        let response = self
            .inner
            .request(method, Url::from_str(url)?)
            .headers(headers)
            .body(Body::wrap_stream(ProgressStream::from(std_file)))
            .send()
            .await?;
        let response_headers = response
            .headers()
            .iter()
            .map(|(key, value)| Ok((key.to_string(), String::from_utf8(value.as_bytes().into())?)))
            .collect::<Result<HashMap<String, String>, Error>>()?;
        let status = response.status();
        Ok(if status.is_success() {
            // 2xx
            BaseResponse {
                headers: response_headers,
                ret_code: 0,
                message: None,
            }
        } else {
            response.json::<BaseResponse>().await?
        })
    }
}
