//! This module contains an api to get the metadata of a file from the remote server ucloud.cn.

use std::collections::HashMap;

use anyhow::Error;
use builder_pattern::Builder;
use chrono::Local;
use reqwest::{Method, header::HeaderMap};

use crate::{
    api::{
        ApiOperation,
        object::{BaseResponse, HeadFileResponse, ObjectOptAuthParam},
        validator::{is_bucket_name_not_empty, is_key_name_not_empty},
    },
    define_operation_struct,
};

#[derive(Builder)]
pub struct HeadFileConfig {
    /// Bucket name
    #[validator(is_bucket_name_not_empty)]
    #[into]
    pub bucket_name: String,

    /// Key name of the file which means file name in cloud.
    #[validator(is_key_name_not_empty)]
    #[into]
    pub key_name: String,

    /// `STS` temporary security token. but not implementated at now.
    #[default(None)]
    pub security_token: Option<String>,
}

define_operation_struct!(HeadFileOperation, HeadFileConfig);

#[async_trait::async_trait]
impl ApiOperation for HeadFileOperation {
    type Response = HeadFileResponse;
    type Error = Error;

    async fn execute(&self) -> Result<Self::Response, Self::Error> {
        let config = &self.config;
        let date = Local::now().format("&Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParam::new()
            .method(Method::HEAD)
            .bucket(config.bucket_name.clone())
            .key_name(config.key_name.clone())
            .content_type(Some("application/json".into()))
            .date(Some(date.clone()))
            .build();
        let authorization = self
            .auth_service
            .authorization(&auth_object, self.object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        if let Some(ref security_token) = config.security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        let url = self
            .object_config
            .generate_final_host(config.bucket_name.as_str(), config.key_name.as_str());
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
            config.key_name,
            resp
        );
        Err(Error::msg("Failed to get file head"))
    }
}
