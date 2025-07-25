use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use anyhow::Error;
use reqwest::{
    Body, Method, Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::result::Result;
use tokio::fs::File;

use crate::api::{object::BaseResponse, stream::ProgressStream};

#[derive(Clone)]
pub struct ApiClient {
    inner_client: Arc<reqwest::Client>,
}

impl Default for ApiClient {
    fn default() -> Self {
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(Duration::from_secs(5))
            // read timeout set to 30 seconds.
            .read_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(3600))
            // 5 minutes not working then release connection.
            .pool_idle_timeout(Duration::from_secs(300))
            .pool_max_idle_per_host(5)
            // only support http1
            .http1_only()
            .user_agent("ufile-rus3-sdk")
            .build()
            .unwrap();
        Self {
            inner_client: Arc::new(client),
        }
    }
}

impl ApiClient {
    /// Create an instance of ApiClient.
    /// If you are creating customized client, please pass it to this method.
    pub fn new(custom_client: Option<Arc<reqwest::Client>>) -> Self {
        if let Some(client) = custom_client {
            Self {
                inner_client: client,
            }
        } else {
            Self::default()
        }
    }

    /// Get a client instance reference.
    pub(crate) fn get_client(&self) -> Arc<reqwest::Client> {
        Arc::clone(&self.inner_client)
    }

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
        let client = self.get_client();
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
        let response = client
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
