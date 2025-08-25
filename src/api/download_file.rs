//! This modules contains an api to download a file from the remote server ucloud.cn.

use std::{ops::Range, path::PathBuf, sync::Arc};

use anyhow::{Error, anyhow};
use builder_pattern::Builder;
use reqwest::header::HeaderMap;
use tokio::{
    fs,
    io::{self, AsyncSeekExt, AsyncWriteExt},
    sync::{self, Semaphore},
};

use crate::constant::{self, DEFAULT_CONCURRENCY};
use crate::{
    api::{
        ApiOperation, GenPrivateUrlConfig, GenPrivateUrlOperation, ObjectConfig, Sealed,
        object::HeadFileResponse,
        validator::{is_bucket_name_not_empty, is_key_name_not_empty},
    },
    client::HttpClient,
};

#[derive(Builder)]
pub struct DownloadFileConfig {
    /// Which bucket the file is in, must not be empty.
    #[validator(is_bucket_name_not_empty)]
    #[into]
    pub bucket_name: String,

    /// The name of the file to download, must not be empty.
    /// If the file is in a folder, please use the full path.
    #[validator(is_key_name_not_empty)]
    #[into]
    pub key_name: String,

    /// By default, the value is None, which means download the file with multiple coroutines
    /// which depends on the number of cpu cores.
    ///
    /// If the concurrency is more than 0, will download the file with specified coroutines.
    ///
    /// Default chunk-size is 1024 * 1024 * 4, means 4MB.
    #[default(None)]
    pub concurrency: Option<u32>,

    /// File profile, which is returned by head file api.
    /// You must specified this field before download the file object.
    /// If you are not sure, please use head file api to get the file profile first.
    pub head: HeadFileResponse,

    /// The expires time of the private url.
    /// Default is 24 * 3600 seconds.
    #[default(24 * 3600)]
    pub expires: u64,

    /// The path to save the file.
    /// If not specified, will use the current directory.
    #[default(None)]
    pub dest: Option<PathBuf>,

    /// If the file already exists, will overwrite it or not.
    #[default(true)]
    pub overwrite: bool,

    /// The iop cmd to download the file which are images.
    #[default(None)]
    pub iop_cmd: Option<String>,

    /// `STS` temporay security token used to authenticate the request.
    #[default(None)]
    pub security_token: Option<String>,
}

pub struct DownloadFileOperation {
    config: DownloadFileConfig,
    client: HttpClient,
    object_config: ObjectConfig,
}

#[allow(unused)]
impl DownloadFileOperation {
    pub fn new(
        config: DownloadFileConfig,
        object_config: ObjectConfig,
        client: HttpClient,
    ) -> Self {
        Self {
            config,
            object_config,
            client,
        }
    }
}

impl Sealed for DownloadFileOperation {}

#[async_trait::async_trait]
impl ApiOperation for DownloadFileOperation {
    type Response = ();
    type Error = Error;

    async fn execute(&self) -> Result<Self::Response, Self::Error> {
        let config = &self.config;
        let total_file_size = config.head.content_length;
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
        let concurrency = if let Some(concurrency) = config.concurrency {
            concurrency as usize
        } else {
            DEFAULT_CONCURRENCY
        };
        let semphore = Arc::new(Semaphore::new(concurrency));
        let mut join_handles = vec![];
        // create handles with chunk count iterator.
        let private_url_operation_config = GenPrivateUrlConfig::new()
            .key_name(config.key_name.clone())
            .bucket_name(config.bucket_name.clone())
            .expires(config.expires)
            .iop_cmd(config.iop_cmd.clone())
            .build();
        let gen_private_url_operation =
            GenPrivateUrlOperation::new(private_url_operation_config, self.object_config.clone());
        let download_url = gen_private_url_operation.execute().await?;

        // Determie destination path
        let dest_path = if let Some(ref dest) = config.dest {
            dest.clone()
        } else {
            PathBuf::from(&config.key_name)
        };

        // Check if file exists and handle overwrite.
        if fs::try_exists(&dest_path).await? && !config.overwrite {
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
            let security_token = config.security_token.clone();
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
