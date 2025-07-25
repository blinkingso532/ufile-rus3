//! This module contains the API for generating private URL.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Error;
use builder_pattern::Builder;
use reqwest::Method;

use crate::api::{
    AuthorizationService, client::ApiClient, object::ObjectConfig, traits::ApiExecutor,
};

/// This struct describe the request of generating private URL.
///
/// # Example
///
/// ```
/// let api = GenPrivateUrlApi::new();
/// let url = api.create_authrorized_url(Method::GET, "bucket", "key", 60);
/// ```
#[derive(Builder)]
pub struct GenPrivateUrlApi {
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

#[async_trait::async_trait]
impl ApiExecutor<String> for GenPrivateUrlApi {
    async fn execute(
        &mut self,
        object_config: ObjectConfig,
        _: Arc<ApiClient>,
        _: AuthorizationService,
    ) -> Result<String, Error> {
        let signature = object_config.authorization_private_url(
            Method::GET,
            &self.bucket_name,
            &self.key_name,
            self.expires,
        )?;
        // calculate expire time since epoch time: (now - 1970-01-01 00:00:00) + expires
        let expire_time = self.expires + SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let url =
            object_config.generate_final_host(self.bucket_name.as_str(), self.key_name.as_str());
        let mut url = format!(
            "{}?UCloudPublicKey={}&Signature={}&Expires={}",
            url, object_config.public_key, signature, expire_time
        );
        // add attachment filename param if needed.
        if let Some(ref attachment_filename) = self.attachment_filename {
            url = format!("{}&ufileattname={}", url, attachment_filename);
        }
        // add security token param if needed.
        if let Some(ref security_token) = self.security_token {
            url = format!("{}&SecurityToken={}", url, security_token);
        }
        // add iop-cmd as query params if needed.
        if let Some(ref iop_cmd) = self.iop_cmd {
            url = format!("{}&iopcmd={}", url, iop_cmd);
        }
        Ok(url)
    }
}
