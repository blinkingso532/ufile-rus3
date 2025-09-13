//! This module contains the API for generating private URL.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Error;
use derive_builder::Builder;
use reqwest::Method;

use crate::api::{ApiOperation, ObjectConfig, Sealed};

#[derive(Builder)]
pub struct GenPublicUrlRequest {
    /// Requried: Bucket name.
    #[builder(setter(into))]
    pub bucket_name: String,

    /// Required: Object name.
    #[builder(setter(into))]
    pub key_name: String,

    /// Optional: IOP command for image operations.
    #[builder(setter(into, strip_option), default)]
    pub iop_cmd: Option<String>,
}
pub struct GenPublicUrlOperation {
    object_config: ObjectConfig,
}

impl GenPublicUrlOperation {
    pub fn new(object_config: ObjectConfig) -> Self {
        Self { object_config }
    }
}

impl Sealed for GenPublicUrlOperation {}

#[async_trait::async_trait]
impl ApiOperation for GenPublicUrlOperation {
    type Request = GenPublicUrlRequest;
    type Response = String;
    type Error = Error;

    async fn execute(&self, req: Self::Request) -> Result<String, Error> {
        let GenPublicUrlRequest {
            bucket_name,
            key_name,
            iop_cmd,
        } = req;
        let mut url = self
            .object_config
            .generate_final_host(bucket_name.as_str(), key_name.as_str());
        if let Some(ref iop_cmd) = iop_cmd {
            url.push_str("?iopcmd=");
            url.push_str(iop_cmd);
        }
        Ok(url)
    }
}

/// Request for generating private URL which will be expired in `expires` seconds.
#[derive(Builder)]
pub struct GenPrivateUrlRequest {
    /// Required: Bucket name.
    #[builder(setter(into))]
    pub bucket_name: String,

    /// Required: Object name.
    #[builder(setter(into))]
    pub key_name: String,

    /// Required: Expire time in `seconds`.
    /// Default: 86400
    #[builder(default = "86400")]
    pub expires: u64,

    /// Optional: IOP command for image operations.
    #[builder(setter(into, strip_option), default)]
    pub iop_cmd: Option<String>,

    /// Optional: Attachment filename.
    ///
    /// Default: None
    #[builder(setter(into, strip_option), default)]
    pub attachment_filename: Option<String>,

    /// Optional: Security token.
    ///
    /// Default: None
    #[builder(setter(into, strip_option), default)]
    pub security_token: Option<String>,
}

pub struct GenPrivateUrlOperation {
    object_config: ObjectConfig,
}

impl GenPrivateUrlOperation {
    pub fn new(object_config: ObjectConfig) -> Self {
        Self { object_config }
    }
}

impl Sealed for GenPrivateUrlOperation {}

#[async_trait::async_trait]
impl ApiOperation for GenPrivateUrlOperation {
    type Request = GenPrivateUrlRequest;
    type Response = String;
    type Error = Error;

    async fn execute(&self, req: Self::Request) -> Result<String, Error> {
        let GenPrivateUrlRequest {
            bucket_name,
            key_name,
            expires,
            attachment_filename,
            security_token,
            iop_cmd,
        } = req;
        // calculate expire time since epoch time: (now - 1970-01-01 00:00:00) + expires
        let expire_time =
            (expires + SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()).to_string();

        let signature = self.object_config.authorization_private_url(
            Method::GET,
            bucket_name.as_str(),
            key_name.as_str(),
            expire_time.as_str(),
        )?;

        let url = self
            .object_config
            .generate_final_host(bucket_name.as_str(), key_name.as_str());
        let mut url = format!(
            "{}?UCloudPublicKey={}&Signature={}&Expires={}",
            url,
            urlencoding::encode(self.object_config.public_key.as_str()),
            urlencoding::encode(signature.as_str()),
            urlencoding::encode(expire_time.as_str()),
        );
        // add attachment filename param if needed.
        if let Some(ref attachment_filename) = attachment_filename {
            url = format!(
                "{url}&ufileattname={}",
                urlencoding::encode(attachment_filename.as_str())
            );
        }
        // add security token param if needed.
        if let Some(ref security_token) = security_token {
            url = format!(
                "{url}&SecurityToken={}",
                urlencoding::encode(security_token.as_str())
            );
        }
        // add iop-cmd as query params if needed.
        if let Some(ref iop_cmd) = iop_cmd {
            url = format!("{url}&iopcmd={}", urlencoding::encode(iop_cmd.as_str()));
        }
        Ok(url)
    }
}
