use crate::api::{ApiOperation, put_file::PutFileConfig, put_file::PutFileOperation, validator::*};
use crate::client::HttpClient;
use crate::{AuthorizationService, api::ObjectConfig};
use anyhow::Error;
use builder_pattern::Builder;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Builder)]
pub struct PutObjectRequest {
    /// Required: Cloud object name
    #[validator(is_key_name_not_empty)]
    #[public]
    #[into]
    key_name: String,

    /// Required: File to upload
    #[validator(is_path_buf_valid)]
    #[public]
    #[into]
    file: PathBuf,

    /// Required: File MIME type
    #[validator(is_mime_type_valid)]
    #[public]
    #[into]
    mime_type: String,

    /// Required: Bucket name
    #[validator(is_bucket_name_not_empty)]
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

    // 依赖组件
    #[public]
    object_config: ObjectConfig,
    #[public]
    http_client: HttpClient,
    #[public]
    auth_service: AuthorizationService,
}

impl PutObjectRequest {
    // 构建并执行操作
    pub async fn send(self) -> Result<crate::api::object::PutObjectResultResponse, Error> {
        let config = PutFileConfig::new()
            .key_name(self.key_name.as_str())
            .file(self.file.clone())
            .mime_type(self.mime_type)
            .bucket_name(self.bucket_name)
            .is_verify_md5(self.is_verify_md5)
            .metadatas(self.metadatas)
            .storage_type(self.storage_type)
            .iop_cmd(self.iop_cmd)
            .security_token(self.security_token)
            .build();

        let operation = PutFileOperation::new()
            .config(config)
            .object_config(self.object_config)
            .auth_service(self.auth_service)
            .client(self.http_client)
            .build();

        operation.execute().await
    }
}
