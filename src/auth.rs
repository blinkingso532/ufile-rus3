use anyhow::Error;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha1::Sha1;

use crate::api::object::{ObjectConfig, ObjectOptAuthParam};

// 签名器 trait
pub trait Signer {
    /// Method used to sign data.
    fn signature(&self, private_key: &str, data: &str) -> Result<String, Error>;
}

pub struct HmacSha1Signer;

impl Signer for HmacSha1Signer {
    fn signature(&self, private_key: &str, data: &str) -> Result<String, Error> {
        type HmacSha1 = Hmac<Sha1>;

        let mut mac = HmacSha1::new_from_slice(private_key.as_bytes())?;
        mac.update(data.as_bytes());
        let result = mac.finalize();
        let code_bytes = result.into_bytes();

        // 转换为Base64编码
        let signature = base64::engine::general_purpose::STANDARD.encode(code_bytes);
        Ok(signature)
    }
}

// 授权服务
#[derive(Debug, Clone, Copy)]
pub struct AuthorizationService;

impl AuthorizationService {
    pub fn authorization(
        &self,
        param: &ObjectOptAuthParam,
        object_config: ObjectConfig,
    ) -> Result<String, Error> {
        let method = &param.method;
        let bucket = param.bucket.as_str();
        let key_name = param.key_name.as_str();
        let content_type = param.content_type.as_deref().unwrap_or("");
        let content_md5 = param.content_md5.as_deref().unwrap_or("");
        let date = param.date.as_deref().unwrap_or("");

        // 处理特殊头部
        let x_ufile_copy_source = param
            .x_ufile_copy_source
            .as_deref()
            .map(|src| format!("x-ufile-copy-source:{src}\n"))
            .unwrap_or_default();

        let x_ufile_copy_source_range = param
            .x_ufile_copy_source_range
            .as_deref()
            .map(|range| format!("x-ufile-copy-source-range:{range}\n"))
            .unwrap_or_default();

        // 构建签名字符串
        let mut sign_data = String::new();
        sign_data.push_str(&format!("{}\n", method.as_str()));
        sign_data.push_str(&format!("{content_md5}\n"));
        sign_data.push_str(&format!("{content_type}\n"));
        sign_data.push_str(&format!("{date}\n"));
        sign_data.push_str(&x_ufile_copy_source);
        sign_data.push_str(&x_ufile_copy_source_range);
        sign_data.push_str(&format!("/{bucket}"));
        sign_data.push_str(&format!("/{key_name}"));

        if cfg!(debug_assertions) {
            ::tracing::debug!("[signData]: {sign_data}");
        }

        // 生成签名
        let signature =
            HmacSha1Signer.signature(object_config.private_key.as_str(), sign_data.as_str())?;

        // 构建最终授权字符串
        Ok(format!(
            "UCloud {}:{}",
            object_config.public_key.as_str(),
            signature
        ))
    }
}
