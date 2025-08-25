//! As you can init a task to upload a file, and then you can abort the task if you want.
//! So this module is used to create a task to abort the multipart upload task.

use std::collections::HashMap;

use anyhow::Error;
use builder_pattern::Builder;
use chrono::Local;
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName},
};

use crate::{
    api::{
        ApiOperation,
        object::{BaseResponse, InitMultipartState, ObjectOptAuthParam},
    },
    define_operation_struct,
};

/// This api used to abort the multipart upload task.
#[derive(Builder)]
pub struct MultipartAbortConfig {
    /// State of multipart upload task.
    pub state: InitMultipartState,

    /// User custom headers metadata.
    #[default(None)]
    pub metadata: Option<HashMap<String, String>>,

    /// Security Token
    #[default(None)]
    pub security_token: Option<String>,
}

define_operation_struct!(MultipartAbortOperation, MultipartAbortConfig);

#[async_trait::async_trait]
impl ApiOperation for MultipartAbortOperation {
    type Response = ();
    type Error = Error;

    async fn execute(&self) -> Result<Self::Response, Self::Error> {
        let config = &self.config;
        let mime_type = config
            .state
            .mime_type
            .clone()
            .ok_or(Error::msg("mime type is unset."))?;
        // let mime_type = "text/plain".to_string();
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParam::new()
            .method(Method::DELETE)
            .bucket(config.state.bucket.clone())
            .key_name(config.state.key_name.clone())
            .content_type(Some(mime_type.clone()))
            .date(Some(date.clone()))
            .build();
        let authorization = self
            .auth_service
            .authorization(&auth_object, self.object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", mime_type.parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        if let Some(ref security_token) = config.security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        // We must add metadata to headers if metadata is not empty.
        let url = self
            .object_config
            .generate_final_host(config.state.bucket.as_str(), config.state.key_name.as_str());
        let url = format!("{}?uploadId={}", url, config.state.upload_id,);
        if let Some(ref metadata) = config.metadata {
            for (k, v) in metadata {
                headers.insert(
                    format!("X-Ufile-Meta-{k}").parse::<HeaderName>().unwrap(),
                    v.parse().unwrap(),
                );
            }
        }
        let resp = self
            .client
            .get_client()
            .delete(url)
            .headers(headers)
            .send()
            .await?;
        tracing::info!("Abort multipart upload task: {:?}", resp);
        if resp.status().is_success() {
            return Ok(());
        }
        let base_response: BaseResponse = resp.json().await?;
        tracing::error!("Finish multipart upload task failed: {:?}", base_response);
        Err(Error::msg("Failed to finish multipart upload task."))
    }
}
