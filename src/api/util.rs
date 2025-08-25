//! This module contains the API for generating private URL.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Error;
use builder_pattern::Builder;
use reqwest::Method;

use crate::api::{ApiOperation, ObjectConfig, Sealed};

/// This struct describe the request of generating private URL.
///
/// # Example
///
/// ```
/// let api = GenPrivateUrlApi::new();
/// let url = api.create_authrorized_url(Method::GET, "bucket", "key", 60);
/// ```
#[derive(Builder)]
pub struct GenPrivateUrlConfig {
    /// Bucket name
    pub bucket_name: String,

    /// Obejct name.
    pub key_name: String,

    /// Expire time duration from now in seconds.
    /// expires must be greater than 0.
    pub expires: u64,

    /// IOP command used for image objects.
    /// If specified, the generated URL will contains the iop command as query params.
    #[default(None)]
    pub iop_cmd: Option<String>,

    /// Attachment filename,
    /// If specified, the generated URL will contains the attachment filename.
    /// The key name is `ufileattname`.
    #[default(None)]
    pub attachment_filename: Option<String>,

    /// STS temporary security token.
    /// If specified, the generated URL will contains the sts token.
    #[default(None)]
    pub security_token: Option<String>,
}

pub struct GenPrivateUrlOperation {
    config: GenPrivateUrlConfig,
    object_config: ObjectConfig,
}

impl GenPrivateUrlOperation {
    pub fn new(config: GenPrivateUrlConfig, object_config: ObjectConfig) -> Self {
        Self {
            config,
            object_config,
        }
    }
}

impl Sealed for GenPrivateUrlOperation {}

#[async_trait::async_trait]
impl ApiOperation for GenPrivateUrlOperation {
    type Response = String;
    type Error = Error;

    async fn execute(&self) -> Result<String, Error> {
        let config = &self.config;
        let signature = self.object_config.authorization_private_url(
            Method::GET,
            config.bucket_name.as_str(),
            config.key_name.as_str(),
            config.expires,
        )?;
        // calculate expire time since epoch time: (now - 1970-01-01 00:00:00) + expires
        let expire_time = config.expires + SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let url = self
            .object_config
            .generate_final_host(config.bucket_name.as_str(), config.key_name.as_str());
        let mut url = format!(
            "{}?UCloudPublicKey={}&Signature={}&Expires={}",
            url, self.object_config.public_key, signature, expire_time
        );
        // add attachment filename param if needed.
        if let Some(ref attachment_filename) = config.attachment_filename {
            url = format!("{url}&ufileattname={attachment_filename}");
        }
        // add security token param if needed.
        if let Some(ref security_token) = config.security_token {
            url = format!("{url}&SecurityToken={security_token}");
        }
        // add iop-cmd as query params if needed.
        if let Some(ref iop_cmd) = config.iop_cmd {
            url = format!("{url}&iopcmd={iop_cmd}");
        }
        Ok(url)
    }
}
