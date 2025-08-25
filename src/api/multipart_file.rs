//! This mod create a new way to upload file to ucloud s3 server.
//! It is adapted to be an option when your file is more than 1GB or more.
//! You should not use put_file_api as if the file is overflow 1GB.
//! Please check your file size before use this mod for when your file is smaller than 1GB
//! or more smaller file, then use put_file_api.

use std::collections::HashMap;

use crate::api::traits::ApiOperation;
use anyhow::Error;
use bytes::Bytes;
use chrono::Local;
use reqwest::{Method, header::HeaderMap};

use crate::{
    api::{
        object::{InitMultipartState, MultipartUploadState, ObjectOptAuthParam},
        validator::is_buffer_not_empty,
    },
    define_operation_struct,
};

define_operation_struct!(MultipartFileOperation, MultipartFileConfig);

#[derive(Builder)]
pub struct MultipartFileConfig {
    /// Slice initial state
    pub state: InitMultipartState,

    /// Slice data
    #[validator(is_buffer_not_empty)]
    pub buffer: Bytes,

    /// slice data size, This is used to set Content-Length for content-length must be set
    /// and must equal to the initial blk size returned by init mulipart api.
    pub buffer_size: u64,

    /// Index of slices
    pub part_index: usize,

    /// Whether to verify md5 of the slice
    #[default(false)]
    pub is_verify_md5: bool,

    ///  temporary `STS` token
    #[default(None)]
    pub security_token: Option<String>,
}

#[async_trait::async_trait]
impl ApiOperation for MultipartFileOperation {
    type Response = MultipartUploadState;
    type Error = Error;

    async fn execute(&self) -> Result<MultipartUploadState, Error> {
        let config = &self.config;
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let mime_type = config
            .state
            .mime_type
            .clone()
            .ok_or(Error::msg("mime type is unset."))?;
        let content_md5 = if config.is_verify_md5 {
            Some(format!(
                "{:x}",
                ::md5::compute(config.buffer.iter().as_slice())
            ))
        } else {
            None
        };
        // d35b134713ee4a6cb7606962941d7b46
        tracing::debug!("content_md5: {content_md5:?}");
        let auth_object = ObjectOptAuthParam::new()
            .method(Method::PUT)
            .bucket(config.state.bucket.clone())
            .key_name(config.state.key_name.clone())
            .content_type(Some(mime_type.clone()))
            .date(Some(date.clone()))
            .content_md5(content_md5.clone())
            .build();
        let authorization = self
            .auth_service
            .authorization(&auth_object, self.object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", mime_type.parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        headers.insert(
            "Content-Length",
            config.buffer.len().to_string().parse().unwrap(),
        );
        if let Some(content_md5) = content_md5 {
            headers.insert("Content-MD5", content_md5.parse().unwrap());
        }

        if let Some(ref security_token) = config.security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        // We must add metadata to headers if metadata is not empty.
        let url = self
            .object_config
            .generate_final_host(config.state.bucket.as_str(), config.state.key_name.as_str());
        let url = format!(
            "{url}?uploadId={}&partNumber={}",
            config.state.upload_id, config.part_index
        );
        let resp = self
            .client
            .get_client()
            .put(url)
            .headers(headers)
            .body(config.buffer.to_vec())
            .send()
            .await?;
        tracing::debug!("Upload part file response: {resp:?}");
        if resp.status().is_success() {
            let headers: HashMap<String, String> = resp
                .headers()
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        String::from_utf8_lossy(v.as_bytes()).to_string(),
                    )
                })
                .collect();
            let mut body: MultipartUploadState = resp.json().await?;
            body.headers.extend(headers);
            if let Some(etag) = body.headers.get("etag") {
                // get etag and set back to response.
                body.etag = remove_quotes(etag).to_string();
            }
            return Ok(body);
        }
        Err(Error::msg("Failed to upload part file"))
    }
}

fn remove_quotes(s: &str) -> String {
    s.trim_matches(|c| c == '\"' || c == '\'').to_string()
}
