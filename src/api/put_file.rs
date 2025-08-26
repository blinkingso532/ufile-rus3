use reqwest::header::{HeaderMap, HeaderName};
use std::str::FromStr;

use crate::api::{object::PutObjectResultResponse, traits::ApiOperation};

use anyhow::Error;
use chrono::Local;
use reqwest::Method;

use crate::api::object::ObjectOptAuthParamBuilder;

use crate::{AuthorizationService, define_api_request, define_operation_struct};

define_operation_struct!(PutFileOperation);

define_api_request!(
    PutFileRequest,
    PutFileOperationBuilder,
    PutObjectResultResponse,
    {
        /// Required: Bucket name
        #[builder(setter(into))]
        pub bucket_name: String,

        /// Required: Object key name
        #[builder(setter(into))]
        pub key_name: String,

        /// Required: File MIME type
        #[builder(setter(into))]
        pub mime_type: String,

        /// Required: File stream.
        pub stream: crate::api::ByteStream,

        /// Optional: File length
        pub content_length: usize,

        /// Optional: File MD5 checksum
        #[builder(setter(into, strip_option), default)]
        pub content_md5: ::std::option::Option<String>,

        /// Optional: User custom metadata
        #[builder(setter(strip_option), default)]
        pub metadatas: ::std::option::Option<::std::collections::HashMap<String, String>>,

        /// Optional: Storage type: STANDARD | IA | ARCHIVE
        #[builder(setter(into, strip_option), default)]
        pub storage_type: ::std::option::Option<String>,

        /// Optional: Image processing service
        #[builder(setter(into, strip_option), default)]
        pub iop_cmd: ::std::option::Option<String>,

        /// Optional: Security token
        #[builder(setter(into, strip_option), default)]
        pub security_token: ::std::option::Option<String>,
    }
);

#[async_trait::async_trait]
impl ApiOperation for PutFileOperation {
    type Request = PutFileRequest;
    type Response = PutObjectResultResponse;
    type Error = Error;

    async fn execute(&self, req: Self::Request) -> Result<Self::Response, Self::Error> {
        let PutFileRequest {
            bucket_name,
            key_name,
            stream,
            mime_type,
            metadatas,
            content_length,
            content_md5,
            storage_type,
            iop_cmd,
            security_token,
            ..
        } = req;
        let date = Local::now().format("%Y%m%d%H%M%S").to_string();
        let content_type = mime_type.clone();
        let mut auth_object_builder = ObjectOptAuthParamBuilder::default();
        auth_object_builder
            .method(Method::PUT)
            .bucket(bucket_name.as_str())
            .key_name(key_name.as_str())
            .content_type(content_type.as_str())
            .date(date.as_str());

        let mut headers = HeaderMap::new();
        // add content md5 to auth object
        if let Some(content_md5) = content_md5 {
            auth_object_builder.content_md5(content_md5.as_str());
            headers.insert("Content-MD5", content_md5.parse().unwrap());
        }
        let auth_object = auth_object_builder.build()?;
        headers.insert(
            "Content-Length",
            content_length.to_string().parse().unwrap(),
        );

        let authorization =
            AuthorizationService.authorization(auth_object, self.object_config.clone())?;
        headers.insert("Authorization", authorization.parse().unwrap());
        headers.insert("Content-Type", content_type.parse().unwrap());
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Date", date.parse().unwrap());

        if let Some(storage_type) = storage_type {
            headers.insert("X-Ufile-Storage-Class", storage_type.parse().unwrap());
        }

        if let Some(security_token) = security_token {
            headers.insert("SecurityToken", security_token.parse().unwrap());
        }

        if let Some(metadatas) = metadatas
            && !metadatas.is_empty()
        {
            metadatas.iter().for_each(|(key, value)| {
                let key = format!("X-Ufile-Meta-{key}");
                headers.insert(
                    HeaderName::from_str(key.as_str()).unwrap(),
                    value.to_string().parse().unwrap(),
                );
            });
        }

        let mut url = self
            .object_config
            .generate_final_host(bucket_name.as_str(), key_name.as_str());
        if let Some(iop_cmd) = iop_cmd {
            url = format!("{url}?{iop_cmd}");
        }

        let response = self
            .client
            .send_file(url.as_str(), Method::PUT, headers, stream)
            .await?;
        tracing::debug!("put file response: {:?}", response);
        let mut put_file_response = PutObjectResultResponse::from(response);
        if let Some(e_tag) = put_file_response.resp.headers.get("etag") {
            put_file_response.etag = e_tag.to_string();
        }

        Ok(put_file_response)
    }
}
