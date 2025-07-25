//! This module contains an api to get the metadata of a file from the remote server ucloud.cn.

use std::{collections::HashMap, sync::Arc};

use anyhow::Error;
use builder_pattern::Builder;
use chrono::Local;
use reqwest::{Method, header::HeaderMap};

use crate::api::{
    AuthorizationService,
    client::ApiClient,
    object::{BaseResponse, HeadFileResponse, ObjectConfig, ObjectOptAuthParam},
    traits::ApiExecutor,
    validator::{is_bucket_name_not_empty, is_key_name_not_empty},
};

/// This struct is used to get the metadata of a file from the remote server ucloud.cn.
/// If the file is not found, will return error.
/// But if the file is found, will return the metadata of the file at response headers,
/// which contains : Content-Type represent file's mime-type, Content-Length represent file's size,
/// ETag represent file's etag, Last-Modified represent file's last modified time.
/// Content-Range represent the range of the file which is returned.
/// We can use `Content-Range` to split file into pieces to download with `download_file_api`.
#[derive(Builder)]
pub struct HeadFileApi {
    /// Bucket name
    #[validator(is_bucket_name_not_empty)]
    #[into]
    pub bucket_name: String,

    /// Key name of the file which means file name in cloud.
    #[validator(is_key_name_not_empty)]
    #[into]
    pub key_name: String,

    /// `STS` temporary security token. but not implementated at now.
    #[default(None)]
    pub security_token: Option<String>,
}

#[async_trait::async_trait]
impl ApiExecutor<HeadFileResponse> for HeadFileApi {
    async fn execute(
        &mut self,
        object_config: ObjectConfig,
        api_client: Arc<ApiClient>,
        auth_service: AuthorizationService,
    ) -> Result<HeadFileResponse, Error> {
        let date = Local::now().format("&Y%m%d%H%M%S").to_string();
        let auth_object = ObjectOptAuthParam::new()
            .method(Method::HEAD)
            .bucket(self.bucket_name.clone())
            .key_name(self.key_name.clone())
            .content_type(Some("application/json".into()))
            .date(Some(date.clone()))
            .build();
        let authorization = auth_service.authorization(&auth_object, object_config.clone())?;
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());
        headers.insert("Authorization", authorization.parse().unwrap());
        if let Some(ref security_token) = self.security_token
            && !security_token.is_empty()
        {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }
        let url =
            object_config.generate_final_host(self.bucket_name.as_str(), self.key_name.as_str());
        // Request to get the file metadata containing content-size and content-type.
        let resp = api_client
            .get_client()
            .head(url)
            .headers(headers)
            .send()
            .await?;
        ::tracing::debug!("get file head response: {:?}", resp);
        if resp.status().is_success() {
            // Request success.
            let headers = resp
                .headers()
                .into_iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_lowercase(),
                        v.to_str().unwrap().to_lowercase(),
                    )
                })
                .collect::<HashMap<String, String>>();
            return Ok(HeadFileResponse {
                content_type: headers.get("content-type").unwrap().into(),
                content_length: headers
                    .get("content-length")
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
                etag: headers.get("etag").map(|v| v.to_string()),
                last_modified: headers.get("last-modified").map(Into::into),
                headers: Some(headers),
            });
        }
        let resp = resp.json::<BaseResponse>().await?;
        tracing::debug!(
            "Failed to get file head for: {} with error: {:?}",
            self.key_name,
            resp
        );
        Err(Error::msg("Failed to get file head"))
    }
}
