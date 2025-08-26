//! This modules contains an api to download a file from the remote server ucloud.cn.

use std::{ops::Range, path::PathBuf, sync::Arc};

use anyhow::{Error, anyhow};
use derive_builder::Builder;
use reqwest::header::HeaderMap;

use crate::api::GenPrivateUrlRequestBuilder;
use crate::constant::{self, DEFAULT_CONCURRENCY};
use crate::{
    api::{ApiOperation, GenPrivateUrlOperation, ObjectConfig, Sealed, object::HeadFileResponse},
    client::HttpClient,
};

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct DownloadFileRequest {
    /// Required: Bucket name
    #[builder(setter(into))]
    pub bucket_name: String,

    /// Required: Key name or object name on ucloud.cn
    #[builder(setter(into))]
    pub key_name: String,

    /// Optional: Concurrency of download file.
    ///
    /// Default: 8 from `crate::constant::DEFAULT_CONCURRENCY`
    /// Default Chunk Size: 1024 * 1024 * 4 (4MB)
    #[builder(setter(into, strip_option), default)]
    pub concurrency: Option<u32>,

    /// Required: File profile response from head file api.
    pub head: HeadFileResponse,

    /// Required: The expires time of the private url.
    /// Default: 86400 (1 day)
    #[builder(default = "86400")]
    pub expires: u64,

    /// Optional: The dest path to save the file.
    #[builder(setter(into, strip_option), default)]
    pub dest: Option<PathBuf>,

    /// Optional: Whether to overwrite the dest file if it already exists.
    /// Default: true
    #[builder(default = "true")]
    pub overwrite: bool,

    /// Optional: The iop cmd to download the file which are images.
    ///
    /// Default: None
    #[builder(setter(into, strip_option), default)]
    pub iop_cmd: Option<String>,

    /// Optional: `STS` temporay security token used to authenticate the request.
    ///
    /// Default: None
    #[builder(setter(into, strip_option), default)]
    pub security_token: Option<String>,
}

pub struct DownloadFileOperation {
    client: HttpClient,
    object_config: ObjectConfig,
}

#[allow(unused)]
impl DownloadFileOperation {
    pub fn new(object_config: ObjectConfig, client: HttpClient) -> Self {
        Self {
            object_config,
            client,
        }
    }
}

impl Sealed for DownloadFileOperation {}

#[async_trait::async_trait]
impl ApiOperation for DownloadFileOperation {
    type Request = DownloadFileRequest;
    type Response = ();
    type Error = Error;

    async fn execute(&self, request: Self::Request) -> Result<Self::Response, Self::Error> {
        let DownloadFileRequest {
            bucket_name,
            key_name,
            concurrency,
            head,
            expires,
            dest,
            overwrite,
            iop_cmd,
            security_token,
        } = request;
        let total_file_size = head.content_length;
        // Calculate the chunks count will be downloaded.
        let chunk_count = (total_file_size + constant::MULTIPART_SIZE as u64 - 1)
            .div_ceil(constant::MULTIPART_SIZE as u64);
        // Separate file into chunks, considering the last chunk might be smaller than MULTIPART_SIZE
        let ranges = (0..chunk_count)
            .map(|i| {
                let start = i * constant::MULTIPART_SIZE as u64;
                let end = ((i + 1) * constant::MULTIPART_SIZE as u64).min(total_file_size);
                Range { start, end }
            })
            .collect::<Vec<_>>();
        // Download the file chunks concurrently and write to the dest file.
        let concurrency = if let Some(concurrency) = concurrency {
            concurrency as usize
        } else {
            DEFAULT_CONCURRENCY
        };
        let semphore = Arc::new(Semaphore::new(concurrency));
        let mut join_handles = vec![];
        // create handles with chunk count iterator.
        let gen_private_url_req = GenPrivateUrlRequestBuilder::default()
            .key_name(key_name.as_str())
            .bucket_name(bucket_name.as_str())
            .expires(expires)
            .build()?;
        let mut gen_private_url_operation = GenPrivateUrlOperation::new(self.object_config.clone());
        let download_url = gen_private_url_operation
            .execute(gen_private_url_req)
            .await?;

        // Determie destination path
        let dest_path = if let Some(ref dest) = dest {
            dest.clone()
        } else {
            PathBuf::from(key_name.as_str())
        };

        // Check if file exists and handle overwrite.
        if fs::try_exists(&dest_path).await? && !overwrite {
            return Err(anyhow!(
                "File {:?} already exists. Set overwrite=true to replace it.",
                dest_path
            ));
        }
        // Create the output file
        let file = fs::File::create(&dest_path).await?;
        let file = Arc::new(sync::Mutex::new(file));

        for range in ranges {
            let semphore = Arc::clone(&semphore);
            let url = download_url.clone();
            let security_token = security_token.clone();
            let file = Arc::clone(&file);
            let client = self.client.clone();

            let join_handle = tokio::spawn(async move {
                // Acquire a semaphore permit before download the chunk.
                let _permit = semphore.acquire().await.unwrap();
                // Download the chunk.
                let mut headers = HeaderMap::new();
                // create http headers
                headers.insert(
                    "Range",
                    format!("bytes={}-{}", range.start, range.end)
                        .parse()
                        .unwrap(),
                );
                if let Some(ref security_token) = security_token
                    && !security_token.is_empty()
                {
                    headers.insert("SecurityToken", security_token.parse().unwrap());
                }
                let response = client
                    .get_client()
                    .get(url)
                    .headers(headers)
                    .send()
                    .await
                    .map_err(|e| anyhow!("Request fialed: {}", e))?;
                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Downlaod failed with status: {}",
                        response.status()
                    ));
                }

                // Read response body.
                let data = response
                    .bytes()
                    .await
                    .map_err(|e| anyhow!("Failed to read response body: {}", e))?;

                // Write to file at correct offset.
                let mut file = file.lock().await;
                file.seek(io::SeekFrom::Start(range.start))
                    .await
                    .map_err(|e| anyhow!("Failed to seek to position {}: {}", range.start, e))?;
                file.write_all(&data)
                    .await
                    .map_err(|e| anyhow!("Failed to write data to file: {}", e))?;
                Ok(())
            });
            join_handles.push(join_handle);
        }

        // Wait for all chunks to complete
        for handle in join_handles {
            handle.await??;
        }
        Ok(())
    }
}
