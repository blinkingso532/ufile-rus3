use std::collections::HashMap;

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
        object::{BaseResponse, InitMultipartState},
    },
    define_api_request, define_operation_struct,
};

define_operation_struct!(MultipartAbortOperation);

define_api_request!(
    MultipartAbortRequest,
    MultipartAbortOperationBuilder,
    (),
    {
        /// Required: State of multipart upload task.
        pub state: InitMultipartState,

        /// Optional: User custom headers metadata.
        #[builder(setter(into, strip_option), default)]
        pub metadata: Option<HashMap<String, String>>,

        /// Security Token
        #[builder(setter(into, strip_option), default)]
        pub security_token: Option<String>,
    }
);

#[async_trait::async_trait]
impl ApiOperation for MultipartAbortOperation {
    type Request = MultipartAbortRequest;
    type Response = ();
    type Error = Error;

    async fn execute(&self, request: Self::Request) -> Result<Self::Response, Self::Error> {
        let MultipartAbortRequest {
            state,
            metadata,
            security_token,
            ..
        } = request;
        let mime_type = state
            .mime_type
            .clone()
            .ok_or(Error::msg("mime type is unset."))?;
        // let mime_type = "text/plain".to_string();
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParamBuilder::default()
            .method(Method::DELETE)
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
        // We must add metadata to headers if metadata is not empty.
        let url = self
            .object_config
            .generate_final_host(state.bucket.as_str(), state.key_name.as_str());
        let url = format!("{}?uploadId={}", url, state.upload_id,);
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
