use std::{collections::HashMap, str::FromStr, time::Duration};

use crate::{
    AuthorizationService,
    api::{
        BaseResponse, ByteStream, GenPrivateUrlRequestBuilder, HeadFileRequestBuilder,
        MultipartAbortRequestBuilder, MultipartFileRequestBuilder, MultipartFinishRequestBuilder,
        MultipartInitRequestBuilder, ObjectConfig, ProgressStream, PutFileRequestBuilder,
    },
};
use anyhow::Error;
use reqwest::{Body, Client, ClientBuilder, Method, Proxy, Url, header::HeaderMap};

#[derive(Clone)]
pub struct S3Client {
    http_client: HttpClient,
    auth_service: AuthorizationService,
}

impl S3Client {
    pub fn new() -> Self {
        Self {
            http_client: HttpClientBuilder::default().build().unwrap(),
            auth_service: AuthorizationService,
        }
    }

    pub fn with_http_client(mut self, http_client: HttpClient) -> Self {
        self.http_client = http_client;
        self
    }

    pub fn with_auth_service(mut self, auth_service: AuthorizationService) -> Self {
        self.auth_service = auth_service;
        self
    }

    pub fn http_client(&self) -> HttpClient {
        self.http_client.clone()
    }

    pub fn authorization_service(&self) -> AuthorizationService {
        self.auth_service
    }

    /// Put object request builder.
    #[must_use]
    pub fn put_object(&self, object_config: ObjectConfig) -> PutFileRequestBuilder {
        PutFileRequestBuilder::default()
            .object_config(object_config)
            .client(self.http_client())
    }

    /// Init multipart upload request builder.
    pub fn multipart_init(&self, object_config: ObjectConfig) -> MultipartInitRequestBuilder {
        MultipartInitRequestBuilder::default()
            .object_config(object_config)
            .client(self.http_client())
    }

    /// Upload multipart file slice request builder.
    pub fn multipart_upload(&self, object_config: ObjectConfig) -> MultipartFileRequestBuilder {
        MultipartFileRequestBuilder::default()
            .object_config(object_config)
            .client(self.http_client())
    }

    /// Finish multipart upload request builder.
    pub fn multipart_finish(&self, object_config: ObjectConfig) -> MultipartFinishRequestBuilder {
        MultipartFinishRequestBuilder::default()
            .object_config(object_config)
            .client(self.http_client())
    }

    /// Abort multipart upload request builder.
    pub fn multipart_abort(&self, object_config: ObjectConfig) -> MultipartAbortRequestBuilder {
        MultipartAbortRequestBuilder::default()
            .object_config(object_config)
            .client(self.http_client())
    }

    /// Get file heads request builder.
    pub fn head_object(&self, object_config: ObjectConfig) -> HeadFileRequestBuilder {
        HeadFileRequestBuilder::default()
            .object_config(object_config)
            .client(self.http_client())
    }

    /// Generate private url request builder.
    pub fn gen_private_url(&self) -> GenPrivateUrlRequestBuilder {
        GenPrivateUrlRequestBuilder::default()
    }
}

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
        headers: HeaderMap,
        stream: ByteStream,
    ) -> Result<BaseResponse, Error> {
        // Check authorization
        let signature = headers.get("Authorization");
        if signature.is_none() {
            return Err(Error::msg("No authorization header found"));
        }
        let response = self
            .inner
            .request(method, Url::from_str(url)?)
            .headers(headers)
            .body(Body::wrap_stream(ProgressStream::from(stream)))
            .send()
            .await?;
        tracing::debug!("send file response: {:?}", response);
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
