use std::collections::HashMap;
use std::path::PathBuf;
use tokio::io;
use tokio::io::AsyncSeekExt;

use crate::{
    api::{object::PutObjectResultResponse, traits::ApiOperation},
    define_operation_struct,
};

use anyhow::Error;
use chrono::Local;
use reqwest::Method;
use tokio::io::AsyncReadExt;

use crate::api::object::ObjectOptAuthParam;

define_operation_struct!(PutFileOperation, PutFileConfig);

/// Put file operation configuration
#[derive(Builder)]
pub struct PutFileConfig {
    /// Required: Cloud object name
    #[public]
    #[into]
    key_name: String,

    /// Required: File to upload
    #[public]
    #[into]
    file: PathBuf,

    /// Required: File MIME type
    #[public]
    #[into]
    mime_type: String,

    /// Required: Bucket name
    #[public]
    #[into]
    bucket_name: String,

    /// Whether to upload MD5 checksum
    #[public]
    #[default(None)]
    is_verify_md5: Option<bool>,

    /// User custom metadata
    #[default(None)]
    #[public]
    metadatas: Option<HashMap<String, String>>,

    /// Storage type: STANDARD | IA | ARCHIVE
    #[default(None)]
    #[public]
    storage_type: Option<String>,

    /// Image processing service
    #[default(None)]
    #[public]
    iop_cmd: Option<String>,

    /// Security token
    #[default(None)]
    #[public]
    security_token: Option<String>,
}

#[async_trait::async_trait]
impl ApiOperation for PutFileOperation {
    type Response = PutObjectResultResponse;
    type Error = Error;

    async fn execute(&self) -> Result<Self::Response, Self::Error> {
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let mut auth_object = ObjectOptAuthParam::new()
            .method(Method::PUT)
            .bucket(self.config.bucket_name.clone())
            .key_name(self.config.key_name.clone())
            .content_type(Some(self.config.mime_type.clone()))
            .date(Some(date.clone()))
            .build();

        let mut file = tokio::fs::File::open(self.config.file.as_path()).await?;
        let mut headers = Vec::<(&str, &str)>::new();
        let mut content_md5 = None;

        if let Some(md5) = self.config.is_verify_md5
            && md5
        {
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

        let authorization = self
            .auth_service
            .authorization(&auth_object, self.object_config.clone())?;
        headers.push(("Authorization", authorization.as_str()));

        let content_length = file.metadata().await?.len().to_string();
        headers.extend_from_slice(&[
            ("Content-Type", self.config.mime_type.as_str()),
            ("Accept", "*/*"),
            ("Content-Length", content_length.as_str()),
            ("Date", date.as_str()),
        ]);

        if let Some(ref storage_type) = self.config.storage_type {
            headers.push(("X-Ufile-Storage-Class", storage_type));
        }

        if let Some(ref security_token) = self.config.security_token {
            headers.push(("SecurityToken", security_token));
        }

        let mut extended_headers = Vec::<(String, String)>::new();
        if let Some(ref metadatas) = self.config.metadatas
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

        let mut url = self
            .object_config
            .generate_final_host(&self.config.bucket_name, &self.config.key_name);
        if let Some(ref iop_cmd) = self.config.iop_cmd {
            url = format!("{url}?{iop_cmd}");
        }

        let response = self
            .client
            .send_file(url.as_str(), Method::PUT, headers.as_slice(), file)
            .await?;

        let mut put_file_response = PutObjectResultResponse::from(response);
        if let Some(e_tag) = put_file_response.resp.headers.get("etag") {
            put_file_response.etag = e_tag.to_string();
        }

        Ok(put_file_response)
    }
}
