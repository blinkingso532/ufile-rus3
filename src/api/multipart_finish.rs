//! This module used to finish the multipart upload task.

use std::{collections::HashMap, fmt::Display};

use anyhow::Error;
use chrono::Local;
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName},
};

use crate::{
    api::{
        ApiOperation,
        object::{
            BaseResponse, FinishUploadResponse, InitMultipartState, MultipartUploadState,
            ObjectOptAuthParam,
        },
    },
    define_operation_struct,
};

define_operation_struct!(MultipartFinishOperation, MultipartFinishConfig);

/// UNCHANGED（默认值）:保持初始化时设置的用户自定义元数据不变。
///
/// REPLACE：忽略初始化分片时设置的用户自定义元数据，直接采用Finish请求中指定的元数据。
#[allow(unused)]
#[derive(Debug, Clone, Copy)]
pub enum MetadataDirective {
    Unchanged,
    Replace,
}

impl Display for MetadataDirective {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataDirective::Unchanged => write!(f, "UNCHANGED"),
            MetadataDirective::Replace => write!(f, "REPLACE"),
        }
    }
}

#[derive(Builder, Clone)]
pub struct MultipartFinishConfig {
    /// State of multipart upload task.
    pub state: InitMultipartState,

    pub part_states: Vec<MultipartUploadState>,

    /// new object name used to replace old one if finish multipart upload task successfully.
    #[default(None)]
    pub new_object: Option<String>,

    /// UNCHANGED（默认值）:保持初始化时设置的用户自定义元数据不变。
    ///
    /// REPLACE：忽略初始化分片时设置的用户自定义元数据，直接采用Finish请求中指定的元数据。
    #[default(None)]
    pub metadata_directive: Option<MetadataDirective>,

    /// User custom headers metadata.
    #[default(None)]
    pub metadata: Option<HashMap<String, String>>,

    /// Security Token
    #[default(None)]
    pub security_token: Option<String>,
}

#[async_trait::async_trait]
impl ApiOperation for MultipartFinishOperation {
    type Response = FinishUploadResponse;
    type Error = Error;

    async fn execute(&self) -> Result<Self::Response, Self::Error> {
        let mut config = self.config.clone();
        let mime_type = config
            .state
            .mime_type
            .clone()
            .ok_or(Error::msg("mime type is unset."))?;
        // let mime_type = "text/plain".to_string();
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParam::new()
            .method(Method::POST)
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
        if let Some(ref directive) = config.metadata_directive {
            headers.insert(
                "X-Ufile-Metadata-Directive",
                directive.to_string().parse().unwrap(),
            );
        }
        // We must add metadata to headers if metadata is not empty.
        let url = self
            .object_config
            .generate_final_host(config.state.bucket.as_str(), config.state.key_name.as_str());
        let url = format!(
            "{}?uploadId={}&newKey={}",
            url,
            config.state.upload_id,
            config.new_object.as_ref().unwrap_or(&String::new())
        );
        // calc body.
        config
            .part_states
            .sort_by(|a, b| a.part_number.cmp(&b.part_number));
        let body_buffer = config
            .part_states
            .iter()
            .map(|item| item.etag.clone())
            .collect::<Vec<_>>()
            .join(",");
        tracing::debug!("Finish multipart upload task body: {:?}", body_buffer);
        headers.insert(
            "Content-Length",
            body_buffer.len().to_string().parse().unwrap(),
        );
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
            .post(url)
            .headers(headers)
            .body(body_buffer)
            .send()
            .await?;
        tracing::info!("Finish multipart upload task: {:?}", resp);
        if resp.status().is_success() {
            let response_headers = resp.headers();
            let response_headers = response_headers
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        String::from_utf8_lossy(v.as_bytes()).to_string(),
                    )
                })
                .collect::<HashMap<String, String>>();
            let mut response_body: FinishUploadResponse = resp.json().await?;
            if let Some(etag) = response_headers.get("etag") {
                response_body.etag = etag.to_string();
            }
            response_body.headers.extend(response_headers);
            return Ok(response_body);
        }
        let base_response: BaseResponse = resp.json().await?;
        tracing::error!("Finish multipart upload task failed: {:?}", base_response);
        Err(Error::msg("Failed to finish multipart upload task."))
    }
}
