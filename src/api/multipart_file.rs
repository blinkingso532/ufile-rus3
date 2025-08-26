use std::collections::HashMap;

use crate::{
    AuthorizationService,
    api::{ObjectOptAuthParamBuilder, traits::ApiOperation},
    define_api_request,
};
use anyhow::Error;
use bytes::Bytes;
use chrono::Local;
use reqwest::{Method, header::HeaderMap};

use crate::{
    api::object::{InitMultipartState, MultipartUploadState},
    define_operation_struct,
};

define_operation_struct!(MultipartFileOperation);
define_api_request!(
    MultipartFileRequest,
    MultipartFileOperationBuilder,
    MultipartUploadState,
    {
        /// Required: Slice initial state
        pub state: InitMultipartState,

        /// Required: Slice data
        pub buffer: Bytes,

        /// Required: Slice data size
        pub buffer_size: u64,

        /// Required: Index of slices
        pub part_index: usize,

        /// Optional: Content-MD5
        #[builder(setter(into, strip_option), default)]
        pub content_md5: Option<String>,

        ///  Optional: temporary `STS` token
        #[builder(setter(into, strip_option), default)]
        pub security_token: Option<String>,
    }
);

#[async_trait::async_trait]
impl ApiOperation for MultipartFileOperation {
    type Request = MultipartFileRequest;
    type Response = MultipartUploadState;
    type Error = Error;

    async fn execute(&self, request: Self::Request) -> Result<MultipartUploadState, Error> {
        let MultipartFileRequest {
            state,
            buffer,
            part_index,
            content_md5,
            security_token,
            ..
        } = request;
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let mime_type = state
            .mime_type
            .clone()
            .ok_or(Error::msg("mime type is unset."))?;
        let auth_object = ObjectOptAuthParamBuilder::default()
            .method(Method::PUT)
            .bucket(state.bucket.as_str())
            .key_name(state.key_name.as_str())
            .content_type(mime_type.as_str())
            .date(date.as_str())
            .content_md5(content_md5.clone().unwrap_or_default())
            .build()?;
        let authorization =
            AuthorizationService.authorization(auth_object, self.object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", mime_type.parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        headers.insert("Content-Length", buffer.len().to_string().parse().unwrap());
        if let Some(content_md5) = content_md5 {
            headers.insert("Content-MD5", content_md5.parse().unwrap());
        }

        if let Some(ref security_token) = security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        // We must add metadata to headers if metadata is not empty.
        let url = self
            .object_config
            .generate_final_host(state.bucket.as_str(), state.key_name.as_str());
        let url = format!(
            "{url}?uploadId={}&partNumber={}",
            state.upload_id, part_index
        );
        let resp = self
            .client
            .get_client()
            .put(url)
            .headers(headers)
            .body(buffer.to_vec())
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
