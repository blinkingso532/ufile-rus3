//! This module used to finish the multipart upload task.

use std::{collections::HashMap, fmt::Display};

use anyhow::Error;
use chrono::Local;
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName},
};

use crate::{
    AuthorizationService,
    api::{
        ApiOperation, ObjectOptAuthParamBuilder,
        object::{BaseResponse, FinishUploadResponse, InitMultipartState, MultipartUploadState},
    },
    define_api_request, define_operation_struct,
};

define_operation_struct!(MultipartFinishOperation);
define_api_request!(
    MultipartFinishRequest,
    MultipartFinishOperationBuilder,
    FinishUploadResponse,
    {
        /// Required: Slice initial state
        pub state: InitMultipartState,

        /// Required: Slice states
        pub part_states: Vec<MultipartUploadState>,

        /// Optional: new object name used to replace old one if finish multipart upload task successfully.
        #[builder(setter(into, strip_option), default)]
        pub new_object: Option<String>,

        /// Optional: UNCHANGED（默认值）:保持初始化时设置的用户自定义元数据不变。
        ///
        /// REPLACE：忽略初始化分片时设置的用户自定义元数据，直接采用Finish请求中指定的元数据。
        #[builder(setter(into, strip_option), default)]
        pub metadata_directive: Option<MetadataDirective>,

        /// Optional: User custom headers metadata.
        #[builder(setter(into, strip_option), default)]
        pub metadata: Option<HashMap<String, String>>,

        /// Optional: Security Token
        #[builder(setter(into, strip_option), default)]
        pub security_token: Option<String>,
    }
);

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

#[async_trait::async_trait]
impl ApiOperation for MultipartFinishOperation {
    type Request = MultipartFinishRequest;
    type Response = FinishUploadResponse;
    type Error = Error;

    async fn execute(&self, req: Self::Request) -> Result<Self::Response, Self::Error> {
        let MultipartFinishRequest {
            state,
            mut part_states,
            new_object,
            metadata_directive,
            metadata,
            security_token,
            ..
        } = req;
        let mime_type = state
            .mime_type
            .clone()
            .ok_or(Error::msg("mime type is unset."))?;
        // let mime_type = "text/plain".to_string();
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParamBuilder::default()
            .method(Method::POST)
            .bucket(state.bucket.as_str())
            .key_name(state.key_name.as_str())
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
        if let Some(ref security_token) = security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        if let Some(ref directive) = metadata_directive {
            headers.insert(
                "X-Ufile-Metadata-Directive",
                directive.to_string().parse().unwrap(),
            );
        }
        // We must add metadata to headers if metadata is not empty.
        let url = self
            .object_config
            .generate_final_host(state.bucket.as_str(), state.key_name.as_str());
        let url = format!(
            "{}?uploadId={}&newKey={}",
            url,
            state.upload_id,
            new_object.as_ref().unwrap_or(&String::new())
        );
        // calc body.
        part_states.sort_by(|a, b| a.part_number.cmp(&b.part_number));
        let body_buffer = part_states
            .iter()
            .map(|item| item.etag.clone())
            .collect::<Vec<_>>()
            .join(",");
        tracing::debug!("Finish multipart upload task body: {:?}", body_buffer);
        headers.insert(
            "Content-Length",
            body_buffer.len().to_string().parse().unwrap(),
        );
        if let Some(ref metadata) = metadata {
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
