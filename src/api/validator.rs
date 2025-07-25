//! This module provides validator functions for api field's validation checking.
use std::{borrow::Cow, fs::File};

use bytes::Bytes;
use mediatype::MediaType;

/// Check the given key is not empty.
fn is_not_empty(key: String, error_msg: &'static str) -> Result<String, &'static str> {
    if key.is_empty() {
        return Err(error_msg);
    }
    Ok(key)
}

/// Check the given key name is empty or not.
pub(crate) fn is_key_name_not_empty(key_name: String) -> Result<String, &'static str> {
    is_not_empty(key_name, "key_name can not be empty")
}

/// Check the given bucket name is empty or not.
pub(crate) fn is_bucket_name_not_empty(bucket_name: String) -> Result<String, &'static str> {
    is_not_empty(bucket_name, "bucket name can not be empty")
}

/// Check file is valid.
pub(crate) fn is_file_valid(file: Option<File>) -> Result<Option<File>, &'static str> {
    match file {
        None => return Err("File must not be null."),
        Some(ref file) => match file.metadata() {
            Ok(meta) => {
                if !meta.is_file() {
                    return Err("file must be a regular file");
                }
                if meta.len() > 512 << 20 {
                    return Err("file size must less than 512MB");
                }
            }
            Err(e) => {
                tracing::error!("get file metadata error: {:?}", e);
                return Err("get file metadata error");
            }
        },
    }

    // check file size.
    Ok(file)
}

pub(crate) fn is_mime_type_valid(mime_type: String) -> Result<String, Cow<'static, str>> {
    // First, check is empty or not.
    is_not_empty(mime_type.clone(), "mime_type can not be empty")
        .map_err(|msg| Cow::Owned(msg.to_string()))?;

    // Second, check is valid or not.
    match MediaType::parse(&mime_type) {
        Ok(media_type) => Ok(media_type.to_string()),
        Err(e) => {
            tracing::error!("Media type parse error: {:?}", e);
            Err(Cow::Owned(format!("mime type [{mime_type}] is invalid")))
        }
    }
}

pub(crate) fn is_buffer_not_empty(buffer: Bytes) -> Result<Bytes, &'static str> {
    if buffer.is_empty() {
        return Err("buffer can not be empty");
    }
    Ok(buffer)
}
