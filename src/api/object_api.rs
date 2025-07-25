//! This module defines object api and re-export put file api etc.

use std::fs::File;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Semaphore;

use anyhow::Error;
use builder_pattern::Builder;
use bytes::Bytes;

use crate::api::object::InitMultipartState;
// export put file api.
pub use crate::api::put_file_api::PutFileApi;
// export file download api.
pub use crate::api::download_file_api::DownloadFileApi;
// export head file api.
pub use crate::api::head_file_api::HeadFileApi;
// export init multipart file api.
pub use crate::api::init_part_api::InitMultipartFileApi;
// export finish multipart file api.
pub use crate::api::finish_multipart_api::FinishMultipartFileApi;
// export upload part file api.
pub use crate::api::part_file_api::MultipartPutFileApi;
// export abort multipart file api.
pub use crate::api::abort_multipart_api::AbortMultipartUploadApi;

use crate::api::{
    AuthorizationService,
    client::ApiClient,
    finish_multipart_api::MetadataDirective,
    object::{FinishUploadResponse, ObjectConfig},
    traits::ApiExecutor,
    validator::{is_bucket_name_not_empty, is_key_name_not_empty, is_mime_type_valid},
};
use crate::util::fs::ChunkFile;

/// Combinated multipart file put api definition.
#[derive(Builder)]
pub struct CombinatedMultipartPutApi {
    /// The Big file.
    pub file: File,

    /// The bucket name.
    #[validator(is_bucket_name_not_empty)]
    pub bucket: String,

    /// The object key name.
    #[validator(is_key_name_not_empty)]
    pub key_name: String,

    /// mime-type.
    #[validator(is_mime_type_valid)]
    pub mime_type: String,

    /// The upload id.
    #[default(None)]
    pub upload_id: Option<String>,

    /// The new object key name.
    #[default(None)]
    pub new_object: Option<String>,

    /// The object metadata.
    #[default(None)]
    pub metadata: Option<HashMap<String, String>>,

    /// The storage type.
    #[default(None)]
    pub storage_type: Option<String>,

    /// The metadata directive.
    #[default(None)]
    pub metadata_directive: Option<MetadataDirective>,

    /// Whether to verify md5.
    #[default(false)]
    pub is_verify_md5: bool,

    /// The security token.
    #[default(None)]
    pub security_token: Option<String>,

    /// The concurrency for multipart upload slices. The value is not equality to
    /// the more the better. but the less the better. You should consider your
    /// network bandwidth and cpu performance.
    ///
    /// Default is 4.
    #[default(4)]
    pub concurrency: u64,
}

impl CombinatedMultipartPutApi {
    pub async fn execute(
        &mut self,
        object_config: ObjectConfig,
        api_client: Arc<ApiClient>,
        auth_service: AuthorizationService,
    ) -> Result<FinishUploadResponse, Error> {
        // We are going to initilize the multipart upload task here.
        let mut init_api = InitMultipartFileApi::new()
            .bucket_name(self.bucket.clone())
            .map_err(Error::msg)?
            .key_name(self.key_name.clone())
            .map_err(Error::msg)?
            .mime_type(self.mime_type.clone())
            .map_err(Error::msg)?
            .metadata(self.metadata.clone())
            .security_token(self.security_token.clone())
            .build();

        // Here, we got the intilization response which can be used to create the next step to part upload slices.
        let init_state = init_api
            .execute(object_config.clone(), Arc::clone(&api_client), auth_service)
            .await?;

        // Now, we should separate the file to slices with indexes before the real uploading.
        let blk_size = init_state.blk_size;
        let file_size = self.file.metadata()?.len() as u64;
        let part_count = (file_size + blk_size - 1) / blk_size;
        // Async helper to read specific file chunk
        fn try_build_part_api(
            state: InitMultipartState,
            index: u64,
            buffer: Bytes,
            blk_size: u64,
            security_token: Option<String>,
            is_verify_md5: bool,
        ) -> Result<MultipartPutFileApi, Error> {
            Ok(MultipartPutFileApi::new()
                .buffer(buffer)
                .map_err(Error::msg)?
                .state(state)
                .part_index(index as usize)
                .security_token(security_token)
                .is_verify_md5(is_verify_md5)
                .buffer_size(blk_size)
                .build())
        }

        // Create async tasks for each part to read chunks concurrently
        // Here, we read all bytes from the file. this is not recommanded because of huge file in memory.
        let semaphore = Arc::new(Semaphore::new(5)); // Limit concurrent uploads to 5
        let mut tasks = vec![];
        let mut remaining_size = file_size;
        for index in 0..part_count {
            // clone the file handle.
            let file = self.file.try_clone().unwrap();
            let semaphore = semaphore.clone();
            let security_token = self.security_token.clone();
            let is_verify_md5 = self.is_verify_md5;
            let object_config = object_config.clone();
            let api_client = api_client.clone();
            let auth_service = auth_service.clone();
            let state = init_state.clone();
            let buffer_size = if blk_size > remaining_size {
                remaining_size
            } else {
                blk_size
            };
            // decrease the remaining size.
            if remaining_size > blk_size {
                remaining_size -= blk_size;
            }
            tasks.push(tokio::spawn(async move {
                let file = file;
                let permit = semaphore.acquire().await.unwrap();
                let offset = index * blk_size;
                let result = match ChunkFile::create_chunk_file(&file, offset, buffer_size) {
                    Ok(chunk) => {
                        let mut part_api = match try_build_part_api(
                            state,
                            index,
                            chunk.get_bytes(),
                            blk_size,
                            security_token,
                            is_verify_md5,
                        ) {
                            Ok(part_api) => part_api,
                            Err(e) => return Err(e),
                        };
                        part_api
                            .execute(object_config.clone(), Arc::clone(&api_client), auth_service)
                            .await
                    }
                    Err(error) => {
                        tracing::error!("Failed to read chunk {}: {}", index, error);
                        Err(error)
                    }
                };
                // drop permit before return the result.
                drop(permit);
                result
            }));
        }
        // we wait here for all tasks to be finished.
        let results = futures::future::join_all(tasks).await;
        let mut parts_state = vec![];
        for result in results {
            let state = result??;
            tracing::debug!("Part {} uploaded successfully", state.part_number);
            parts_state.push(state);
        }
        // // We can create multiple tasks cocurrentlly to do the upload.
        // // Execute concurrent uploads with limited concurrency
        // // We should send finish upload request to ucloud.
        FinishMultipartFileApi::new()
            .new_object(self.new_object.take())
            .state(init_state)
            .part_states(parts_state)
            .metadata_directive(self.metadata_directive.take())
            .metadata(self.metadata.take())
            .build()
            .execute(object_config, api_client, auth_service)
            .await
    }
}
