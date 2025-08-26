//! This module contains an api to get the metadata of a file from the remote server ucloud.cn.

use std::collections::HashMap;

use anyhow::Error;
use chrono::Local;
use reqwest::{Method, header::HeaderMap};

use crate::{
    AuthorizationService,
    api::{
        ApiOperation,
        object::{BaseResponse, HeadFileResponse, ObjectOptAuthParamBuilder},
    },
    define_api_request, define_operation_struct,
};
define_operation_struct!(HeadFileOperation);

define_api_request!(HeadFileRequest,
HeadFileOperationBuilder,
HeadFileResponse,
{
    /// Required: Bucket name
    #[builder(setter(into))]
    pub bucket_name: String,

    /// Required: Key name of the file or object name.
    #[builder(setter(into))]
    pub key_name: String,

    /// Optional: `STS` temporary security token. but not implementated at now.
    #[builder(setter(into, strip_option), default)]
    pub security_token: Option<String>,
});

#[async_trait::async_trait]
impl ApiOperation for HeadFileOperation {
    type Request = HeadFileRequest;
    type Response = HeadFileResponse;
    type Error = Error;

    async fn execute(&self, req: Self::Request) -> Result<Self::Response, Self::Error> {
        let HeadFileRequest {
            bucket_name,
            key_name,
            security_token,
            ..
        } = req;
        let date = Local::now().format("&Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParamBuilder::default()
            .method(Method::HEAD)
            .bucket(bucket_name.clone())
            .key_name(key_name.clone())
            .content_type("application/json")
            .date(date.as_str())
            .build()?;
        let authorization =
            AuthorizationService.authorization(auth_object, self.object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        if let Some(ref security_token) = security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        let url = self
            .object_config
            .generate_final_host(bucket_name.as_str(), key_name.as_str());
        // Request to get the file metadata containing content-size and content-type.
        let resp = self
            .client
            .get_client()
            .head(url)
            .headers(headers)
            .send()
            .await?;
        ::tracing::debug!("get file head response: {:?}", resp);
        if resp.status().is_success() {
            // Request success.
            let headers = resp
                .headers()
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_lowercase(),
                        v.to_str().unwrap().to_lowercase(),
                    )
                })
                .collect::<HashMap<String, String>>();
            return Ok(HeadFileResponse {
                content_type: headers.get("content-type").unwrap().into(),
                content_length: headers
                    .get("content-length")
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
                etag: headers.get("etag").map(|v| v.to_string()),
                last_modified: headers.get("last-modified").map(Into::into),
                headers: Some(headers),
            });
        }
        let resp = resp.json::<BaseResponse>().await?;
        tracing::debug!(
            "Failed to get file head for: {} with error: {:?}",
            key_name,
            resp
        );
        Err(Error::msg("Failed to get file head"))
    }
}
