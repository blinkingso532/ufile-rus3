use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use anyhow::Error;
use builder_pattern::Builder;
use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::api::{HmacSha1Signer, Signer};

/// U-cloud protocol
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub enum UfileProtocol {
    Http,
    #[default]
    Https,
}

impl Display for UfileProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UfileProtocol::Http => write!(f, "http"),
            UfileProtocol::Https => write!(f, "https"),
        }
    }
}

#[derive(Debug, Clone, Builder)]
pub struct ObjectOptAuthParam {
    /// Required.
    /// Specify the http method.
    pub(crate) method: Method,
    /// Required.
    /// Specify the name of the bucket.
    pub(crate) bucket: String,
    /// Required.
    /// Specify the name of the object.
    pub(crate) key_name: String,
    /// Content-Type.
    /// Specify the content type of the file.
    #[default(None)]
    pub(crate) content_type: Option<String>,
    /// Content-MD5.
    /// Specify the md5 of the file.
    #[default(None)]
    pub(crate) content_md5: Option<String>,
    /// Date.
    /// Specify the date of the request.
    #[default(None)]
    pub(crate) date: Option<String>,
    /// Specify the source file to be copied.
    ///
    /// Example:
    /// ```
    /// let source = "ufile://bucket-name/file-name";
    /// ```
    #[default(None)]
    pub(crate) x_ufile_copy_source: Option<String>,
    /// X-UFile-Copy-Source-Range.
    /// Specify the range of the file to be copied.
    #[default(None)]
    pub(crate) x_ufile_copy_source_range: Option<String>,
}

/// Configuration for Ucloud object operations.
/// This struct holds the necessary information such as region, proxy suffix, and custom host
/// to interact with Ucloud object storage.
#[derive(Debug, Clone, Builder, Serialize, Deserialize)]
pub struct ObjectConfig {
    /// default http request endpoint.
    #[into]
    #[default("https://api.ucloud.cn".into())]
    pub endpoint: String,
    /// private key
    #[into]
    pub private_key: String,
    /// public key
    pub public_key: String,
    /// 仓库地区 (eg: 'cn-bj')
    #[serde(rename = "Region")]
    pub region: String,

    /// 代理后缀 (eg: 'ufileos.com')
    #[serde(rename = "ProxySuffix")]
    pub proxy_suffix: Option<String>,

    /// 自定义域名 (eg: 'api.ucloud.cn')：若配置了非空自定义域名，则使用自定义域名，不会使用 region + proxySuffix 拼接
    #[serde(rename = "CustomHost")]
    pub custom_host: Option<String>,

    /// protocol
    #[serde(skip)]
    #[default(UfileProtocol::Https)]
    pub protocol: UfileProtocol,
}

impl Default for ObjectConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://api.ucloud.cn".to_string(),
            private_key: "".to_string(),
            public_key: "".to_string(),
            region: "cn-sh2".to_string(),
            proxy_suffix: None,
            custom_host: None,
            protocol: UfileProtocol::Https,
        }
    }
}

impl ObjectConfig {
    /// A method to generate the final request full hosts.
    pub fn generate_final_host(&self, bucket_name: &str, key_name: &str) -> String {
        let key_name = urlencoding::encode(key_name);
        if let Some(ref custom_hosts) = self.custom_host {
            format!("{}/{}", custom_hosts, key_name.as_ref())
        } else {
            let bucket_name = urlencoding::encode(bucket_name);
            let region = urlencoding::encode(&self.region);
            let proxy_suffix = if let Some(ref suffix) = self.proxy_suffix {
                suffix
            } else {
                ""
            };
            let proxy_suffix = urlencoding::encode(proxy_suffix);
            format!(
                "{}://{}.{}.{}/{}",
                self.protocol,
                bucket_name.as_ref(),
                region.as_ref(),
                proxy_suffix.as_ref(),
                key_name.as_ref()
            )
        }
    }

    /// This method is used to generate private url which contains signature and expire time.
    ///
    /// # Arguments
    ///
    /// * `method` - The http method.
    /// * `bucket_name` - The name of the bucket.
    /// * `key_name` - The name of the object.
    /// * `expires` - The expire time of the url. unit: second.
    pub fn authorization_private_url(
        &self,
        method: Method,
        bucket_name: &str,
        key_name: &str,
        expires: u64,
    ) -> Result<String, Error> {
        if bucket_name.is_empty() {
            return Err(Error::msg("bucket must not be empty."));
        }

        if key_name.is_empty() {
            return Err(Error::msg("key_name must not be empty."));
        }

        let method = method.to_string();
        if expires == 0 {
            return Err(Error::msg("expires must not be zero."));
        }
        let sign_data = format!(
            "{}\n{}\n{}\n{}\n/{}\n/{}",
            method, "", "", expires, bucket_name, key_name
        );
        // we should calculate signature here.
        HmacSha1Signer.signature(&self.private_key, &sign_data)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BaseResponse {
    #[serde(skip)]
    pub headers: HashMap<String, String>,
    #[serde(rename = "RetCode")]
    pub ret_code: i32,
    #[serde(rename = "Message", alias = "ErrMsg")]
    pub message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutObjectResultResponse {
    #[serde(flatten)]
    pub resp: BaseResponse,
    #[serde(rename = "ETag")]
    pub etag: String,
}

impl From<BaseResponse> for PutObjectResultResponse {
    fn from(resp: BaseResponse) -> Self {
        Self {
            resp,
            etag: String::new(),
        }
    }
}

/// This struct describe the init multipart upload task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InitMultipartState {
    /// 上传 ID
    pub upload_id: String,
    /// 块大小
    pub blk_size: u64,
    /// Target Bucket
    pub bucket: String,
    /// Cloud object name
    #[serde(rename = "Key", alias = "Key")]
    pub key_name: String,
    /// Mime type
    pub mime_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MultipartUploadState {
    #[serde(skip_deserializing)]
    pub headers: HashMap<String, String>,
    pub part_number: usize,
    #[serde(skip_deserializing)]
    pub etag: String,
}

/// This struct describe the response of finish multipart upload task.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FinishUploadResponse {
    #[serde(skip_deserializing)]
    pub headers: HashMap<String, String>,
    pub bucket: String,
    pub key: String,
    pub file_size: isize,
    #[serde(skip_deserializing)]
    pub etag: String,
}

/// This struct describe the response headers of head file api request.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeadFileResponse {
    #[serde(skip_deserializing)]
    pub headers: Option<HashMap<String, String>>,
    /// Http response headers
    pub etag: Option<String>,
    /// Content-Type of the file
    pub content_type: String,
    /// Content-length of the file.
    pub content_length: u64,
    /// Last modified time.
    pub last_modified: Option<String>,
}

#[test]
fn test_serde() {
    let a = r#"{
        "Bucket": "Success",
        "Key": "1234567890",
        "FileSize": 1024
    }"#;

    let a: FinishUploadResponse = serde_json::from_str(a).unwrap();
    println!("{:?}", a);
}
