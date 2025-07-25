//! This module used to finish the multipart upload task.

use std::{collections::HashMap, fmt::Display, sync::Arc};

use anyhow::Error;
use builder_pattern::Builder;
use chrono::Local;
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName},
};

use crate::api::{
    AuthorizationService,
    client::ApiClient,
    object::{
        BaseResponse, FinishUploadResponse, InitMultipartState, MultipartUploadState, ObjectConfig,
        ObjectOptAuthParam,
    },
    traits::ApiExecutor,
};

/// UNCHANGED（默认值）:保持初始化时设置的用户自定义元数据不变。
///
/// REPLACE：忽略初始化分片时设置的用户自定义元数据，直接采用Finish请求中指定的元数据。
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

#[derive(Builder)]
pub struct FinishMultipartFileApi {
    /// State of multipart upload task.
    pub state: InitMultipartState,

    pub part_states: Vec<MultipartUploadState>,

    /// new object name used to replace old one if finish multipart upload task successfully.
    #[default(None)]
    pub new_object: Option<String>,

    /// UNCHANGED（默认值）:保持初始化时设置的用户自定义元数据不变。
    ///
    /// REPLACE：忽略初始化分片时设置的用户自定义元数据，直接采用Finish请求中指定的元数据。
    pub metadata_directive: Option<MetadataDirective>,

    /// User custom headers metadata.
    #[default(None)]
    pub metadata: Option<HashMap<String, String>>,

    /// Security Token
    #[default(None)]
    pub security_token: Option<String>,
}

#[async_trait::async_trait]
impl ApiExecutor<FinishUploadResponse> for FinishMultipartFileApi {
    async fn execute(
        &mut self,
        object_config: ObjectConfig,
        api_client: Arc<ApiClient>,
        auth_service: AuthorizationService,
    ) -> Result<FinishUploadResponse, Error> {
        let mime_type = self
            .state
            .mime_type
            .clone()
            .take()
            .ok_or(Error::msg("mime type is unset."))?;
        // let mime_type = "text/plain".to_string();
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParam::new()
            .method(Method::POST)
            .bucket(self.state.bucket.clone())
            .key_name(self.state.key_name.clone())
            .content_type(Some(mime_type.clone()))
            .date(Some(date.clone()))
            .build();
        let authorization = auth_service.authorization(&auth_object, object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", mime_type.parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        if let Some(ref security_token) = self.security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        if let Some(ref directive) = self.metadata_directive {
            headers.insert(
                "X-Ufile-Metadata-Directive",
                directive.to_string().parse().unwrap(),
            );
        }
        // We must add metadata to headers if metadata is not empty.
        let url = object_config
            .generate_final_host(self.state.bucket.as_str(), self.state.key_name.as_str());
        let url = format!(
            "{}?uploadId={}&newKey={}",
            url,
            self.state.upload_id,
            self.new_object.as_ref().unwrap_or(&String::new())
        );
        // calc body.
        self.part_states
            .sort_by(|a, b| a.part_number.cmp(&b.part_number));
        let body_buffer = self
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
        if let Some(ref metadata) = self.metadata {
            for (k, v) in metadata {
                headers.insert(
                    format!("X-Ufile-Meta-{}", k).parse::<HeaderName>().unwrap(),
                    v.parse().unwrap(),
                );
            }
        }
        let resp = api_client
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
