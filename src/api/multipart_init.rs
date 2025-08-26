use anyhow::Error;
use chrono::Local;
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName},
};

use crate::{
    AuthorizationService,
    api::{ApiOperation, ObjectOptAuthParamBuilder, object::InitMultipartState},
    define_api_request, define_operation_struct,
};

define_operation_struct!(MultipartInitOperation);

define_api_request!(
    MultipartInitRequest,
    MultipartInitOperationBuilder,
    InitMultipartState,
    {
    /// Required: Key name
    #[builder(setter(into))]
    pub key_name: String,

    /// Required: Content type
    #[builder(setter(into))]
    pub mime_type: String,

    /// Required: Bucket name
    #[builder(setter(into))]
    pub bucket_name: String,

    /// Optional: Metadata
    #[builder(setter(into, strip_option), default)]
    pub metadata: ::std::option::Option<::std::collections::HashMap<String, String>>,

    /// Optional: Storage type
    #[builder(setter(into, strip_option), default)]
    pub storage_type: ::std::option::Option<String>,

    /// Optional: Security token
    #[builder(setter(into, strip_option), default)]
    pub security_token: ::std::option::Option<String>,
    }
);

#[async_trait::async_trait]
impl ApiOperation for MultipartInitOperation {
    type Request = MultipartInitRequest;
    type Response = InitMultipartState;
    type Error = Error;

    async fn execute(&self, request: Self::Request) -> Result<Self::Response, Self::Error> {
        let MultipartInitRequest {
            key_name,
            mime_type,
            bucket_name,
            metadata,
            storage_type,
            security_token,
            ..
        } = request;
        let date = Local::now().format("&Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParamBuilder::default()
            .method(Method::POST)
            .bucket(bucket_name.as_str())
            .key_name(key_name.as_str())
            .content_type(mime_type.as_str())
            .date(date.as_str())
            .build()?;
        let authorization =
            AuthorizationService.authorization(auth_object, self.object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", mime_type.parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        if let Some(ref storage_type) = storage_type
            && !storage_type.is_empty()
        {
            headers.insert("X-Ufile-Storage-Class", storage_type.parse().unwrap());
        }
        if let Some(ref security_token) = security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        // We must add metadata to headers if metadata is not empty.
        if let Some(ref metadata) = metadata
            && !metadata.is_empty()
        {
            for (k, v) in metadata {
                headers.insert(
                    format!("X-Ufile-Meta-{k}").parse::<HeaderName>().unwrap(),
                    v.parse().unwrap(),
                );
            }
        }
        let url = self
            .object_config
            .generate_final_host(bucket_name.as_str(), key_name.as_str());
        let url = format!("{url}?uploads");
        // do request to remote server to create initialization of the multipart upload task.
        let resp = self
            .client
            .get_client()
            .post(url)
            .headers(headers)
            .json("")
            .send()
            .await?;
        ::tracing::debug!("Init multipart file response: {:?}", resp);
        if resp.status().is_success() {
            let mut resp: InitMultipartState = resp.json().await?;
            resp.mime_type.replace(mime_type.clone());
            return Ok(resp);
        }
        Err(Error::msg("Failed to init multipart file"))
    }
}
