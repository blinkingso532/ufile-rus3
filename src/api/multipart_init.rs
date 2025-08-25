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
        object::{InitMultipartState, ObjectOptAuthParam},
        validator::{is_bucket_name_not_empty, is_key_name_not_empty, is_mime_type_valid},
    },
    define_operation_struct,
};

#[derive(Builder)]
pub struct MultipartInitConfig {
    /// Required
    ///
    /// 上传云端后的文件名
    #[validator(is_key_name_not_empty)]
    #[into]
    #[public]
    key_name: String,

    /// Required
    ///
    /// 上传对象的mimeType
    #[validator(is_mime_type_valid)]
    #[into]
    #[public]
    mime_type: String,

    /// Required
    ///
    /// 要上传的目标Bucket
    #[validator(is_bucket_name_not_empty)]
    #[into]
    #[public]
    bucket_name: String,

    /// 用户自定义文件元数据
    #[default(None)]
    #[public]
    metadata: Option<HashMap<String, String>>,

    /// 文件存储类型，分别是标准、低频、冷存，对应有效值：STANDARD | IA | ARCHIVE
    #[default(None)]
    #[public]
    storage_type: Option<String>,

    /// 安全令牌（STS临时凭证）
    #[default(None)]
    #[public]
    security_token: Option<String>,
}

define_operation_struct!(MultipartInitOperation, MultipartInitConfig);

#[async_trait::async_trait]
impl ApiOperation for MultipartInitOperation {
    type Response = InitMultipartState;
    type Error = Error;

    async fn execute(&self) -> Result<Self::Response, Self::Error> {
        let config = &self.config;
        let date = Local::now().format("&Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParam::new()
            .method(Method::POST)
            .bucket(config.bucket_name.clone())
            .key_name(config.key_name.clone())
            .content_type(Some(config.mime_type.clone()))
            .date(Some(date.clone()))
            .build();
        let authorization = self
            .auth_service
            .authorization(&auth_object, self.object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", config.mime_type.parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        if let Some(ref storage_type) = config.storage_type
            && !storage_type.is_empty()
        {
            headers.insert("X-Ufile-Storage-Class", storage_type.parse().unwrap());
        }
        if let Some(ref security_token) = config.security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        // We must add metadata to headers if metadata is not empty.
        if let Some(ref metadata) = config.metadata
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
            .generate_final_host(config.bucket_name.as_str(), config.key_name.as_str());
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
            resp.mime_type.replace(config.mime_type.clone());
            return Ok(resp);
        }
        Err(Error::msg("Failed to init multipart file"))
    }
}
