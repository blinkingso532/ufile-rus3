use std::{collections::HashMap, sync::Arc};

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
    object::{InitMultipartState, ObjectConfig, ObjectOptAuthParam},
    traits::ApiExecutor,
    validator::{is_bucket_name_not_empty, is_key_name_not_empty, is_mime_type_valid},
};

#[derive(Builder)]
pub struct InitMultipartFileApi {
    /// Required
    ///
    /// 上传云端后的文件名
    #[validator(is_key_name_not_empty)]
    pub key_name: String,

    /// Required
    ///
    /// 上传对象的mimeType
    #[validator(is_mime_type_valid)]
    pub mime_type: String,

    /// Required
    ///
    /// 要上传的目标Bucket
    #[validator(is_bucket_name_not_empty)]
    pub bucket_name: String,

    /// 用户自定义文件元数据
    #[default(None)]
    pub metadata: Option<HashMap<String, String>>,

    /// 文件存储类型，分别是标准、低频、冷存，对应有效值：STANDARD | IA | ARCHIVE
    #[default(None)]
    pub storage_type: Option<String>,

    /// 安全令牌（STS临时凭证）
    #[default(None)]
    pub security_token: Option<String>,
}

#[async_trait::async_trait]
impl ApiExecutor<InitMultipartState> for InitMultipartFileApi {
    async fn execute(
        &mut self,
        object_config: ObjectConfig,
        api_client: Arc<ApiClient>,
        auth_service: AuthorizationService,
    ) -> Result<InitMultipartState, Error> {
        let date = Local::now().format("&Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParam::new()
            .method(Method::POST)
            .bucket(self.bucket_name.clone())
            .key_name(self.key_name.clone())
            .content_type(Some(self.mime_type.clone()))
            .date(Some(date.clone()))
            .build();
        let authorization = auth_service.authorization(&auth_object, object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", self.mime_type.parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        if let Some(ref storage_type) = self.storage_type
            && !storage_type.is_empty()
        {
            headers.insert("X-Ufile-Storage-Class", storage_type.parse().unwrap());
        }
        if let Some(ref security_token) = self.security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        // We must add metadata to headers if metadata is not empty.
        if let Some(ref metadata) = self.metadata
            && !metadata.is_empty()
        {
            for (k, v) in metadata {
                headers.insert(
                    format!("X-Ufile-Meta-{}", k).parse::<HeaderName>().unwrap(),
                    v.parse().unwrap(),
                );
            }
        }
        let url =
            object_config.generate_final_host(self.bucket_name.as_str(), self.key_name.as_str());
        let url = format!("{url}?uploads");
        // do request to remote server to create initialization of the multipart upload task.
        let resp = api_client
            .get_client()
            .post(url)
            .headers(headers)
            .json("")
            .send()
            .await?;
        ::tracing::debug!("Init multipart file response: {:?}", resp);
        if resp.status().is_success() {
            let mut resp: InitMultipartState = resp.json().await?;
            resp.mime_type.replace(self.mime_type.clone());
            return Ok(resp);
        }
        Err(Error::msg("Failed to init multipart file"))
    }
}
