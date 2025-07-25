use builder_pattern::Builder;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::sync::Arc;
use tokio::io::AsyncSeekExt;
use tokio::{fs::File as TokioFile, io};

use crate::api::{
    object::{ObjectConfig, PutObjectResultResponse},
    traits::ApiExecutor,
    validator::{
        is_bucket_name_not_empty, is_file_valid, is_key_name_not_empty, is_mime_type_valid,
    },
};

use anyhow::Error;
use chrono::Local;
use reqwest::Method;
use tokio::io::AsyncReadExt;

use crate::api::{AuthorizationService, client::ApiClient, object::ObjectOptAuthParam};

/// Put file api param definition.
#[derive(Builder)]
pub struct PutFileApi {
    /// Required
    /// 云端对象名称
    #[validator(is_key_name_not_empty)]
    pub key_name: String,

    /// Required
    /// 要上传的文件
    #[validator(is_file_valid)]
    pub file: Option<StdFile>,

    /// Required
    /// 要上传的文件mimeType
    #[validator(is_mime_type_valid)]
    pub mime_type: String,

    /**
     * Required
     * Bucket空间名称
     */
    #[validator(is_bucket_name_not_empty)]
    pub bucket_name: String,

    /**
     * 是否需要上传MD5校验码
     */
    #[default(None)]
    pub is_verify_md5: Option<bool>,

    /// 用户自定义文件元数据
    #[default(None)]
    pub metadatas: Option<HashMap<String, String>>,

    /// 文件存储类型，分别是标准、低频、冷存，对应有效值：STANDARD | IA | ARCHIVE
    #[default(None)]
    pub storage_type: Option<String>,

    /// 图片处理服务
    #[default(None)]
    pub iop_cmd: Option<String>,

    /// 安全令牌
    #[default(None)]
    pub security_token: Option<String>,
}

#[async_trait::async_trait]
impl ApiExecutor<PutObjectResultResponse> for PutFileApi {
    /// Implementation to execute the file upload api request.
    async fn execute(
        &mut self,
        object_config: ObjectConfig,
        api_client: Arc<ApiClient>,
        auth_service: AuthorizationService,
    ) -> Result<PutObjectResultResponse, Error> {
        // auth service before send file.
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let mut auth_object = ObjectOptAuthParam::new()
            .method(Method::PUT)
            .bucket(self.bucket_name.clone())
            .key_name(self.key_name.clone())
            .content_type(Some(self.mime_type.clone()))
            .date(Some(date.clone()))
            .build();
        let file = self.file.take();
        if file.is_none() {
            return Err(Error::msg("File must not be null."));
        }
        let file = file.unwrap();
        let mut file = TokioFile::from(file);
        let mut headers = Vec::<(&str, &str)>::new();
        let mut content_md5 = None;
        if let Some(md5) = self.is_verify_md5
            && md5
        {
            // calc file's md5
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).await?;
            file.seek(io::SeekFrom::Start(0)).await?;
            let digest = ::md5::compute(buffer.as_slice());
            let md5_string = format!("{digest:x}");
            auth_object.content_md5 = Some(md5_string.clone());
            content_md5 = Some(md5_string);
        }
        if let Some(ref content_md5) = content_md5 {
            headers.push(("Content-MD5", content_md5));
        }

        let authorization = auth_service.authorization(&auth_object, object_config.clone())?;
        headers.push(("Authorization", authorization.as_str()));
        let content_length = file.metadata().await?.len().to_string();
        headers.extend_from_slice(&[
            ("Content-Type", self.mime_type.as_str()),
            ("Accept", "*/*"),
            ("Content-Length", content_length.as_str()),
            ("Date", date.as_str()),
        ]);
        if let Some(ref storage_type) = self.storage_type {
            // custom headers here.
            headers.push(("X-Ufile-Storage-Class", storage_type));
        }
        if let Some(ref security_token) = self.security_token {
            headers.push(("SecurityToken", security_token));
        }
        let mut extended_headers = Vec::<(String, String)>::new();
        if let Some(ref metadatas) = self.metadatas
            && !metadatas.is_empty()
        {
            metadatas.iter().for_each(|(key, value)| {
                let key = format!("X-Ufile-Meta-{key}");
                extended_headers.push((key, value.to_string()));
            });
        }
        headers.extend_from_slice(
            extended_headers
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<(&str, &str)>>()
                .as_slice(),
        );
        let mut url = object_config.generate_final_host(&self.bucket_name, &self.key_name);
        if let Some(ref iop_cmd) = self.iop_cmd {
            url = format!("{url}?{iop_cmd}");
        }

        let response = api_client
            .send_file(url.as_str(), Method::PUT, headers.as_slice(), file)
            .await?;
        let mut put_file_response = PutObjectResultResponse::from(response);
        let e_tag = put_file_response.resp.headers.get("ETag");

        if let Some(e_tag) = e_tag {
            put_file_response.etag = e_tag.to_string();
        }
        Ok(put_file_response)
    }
}
