//! This module contains some file system utils. But only support unix like system.

use std::{fs::File, os::unix::fs::FileExt};

use anyhow::Error;
use builder_pattern::Builder;
use bytes::Bytes;

/// The file chunk struct.
/// It used to split file into pieces to upload.
///
/// # Example
///
/// ```
/// use ucloud_rcore::util::fs::ChunkFile;
/// use bytes::Bytes;
///
/// let chunk = ChunkFile::new()
///     .bytes(Bytes::from("hello world"))
///     .offset(0)
///     .size(11)
///     .build();
/// assert_eq!(chunk.bytes, Bytes::from("hello world"));
/// assert_eq!(chunk.offset, 0);
/// assert_eq!(chunk.size, 11);
/// ```
#[derive(Builder)]
pub struct ChunkFile {
    /// The file chunk bytes.
    #[public]
    bytes: Bytes,
    /// The file chunk offset.
    #[public]
    offset: u64,
    /// The file chunk size.
    #[public]
    size: u64,
}

impl ChunkFile {
    pub fn get_bytes(self) -> Bytes {
        self.bytes
    }

    /// Get a slice of the file chunk bytes.
    pub fn get_byte(&self) -> &[u8] {
        self.bytes.iter().as_slice()
    }

    /// Get the size of the chunk.
    pub fn chunk_size(&self) -> usize {
        self.size as usize
    }

    /// Get the size of the internal buffer.
    pub fn buffer_size(&self) -> usize {
        self.bytes.len()
    }

    /// Create a new file chunk.
    /// This method read bytes with unix system api `pread` to read file chunk without mutable reference to the file.
    ///
    ///
    /// # Arguments
    ///
    /// * `file` - The file to read.
    /// * `offset` - The file chunk offset.
    /// * `size` - The file chunk size. Must ensure bytes read from file less than or equal to `size`.
    ///
    /// # Returns
    ///
    /// * `Ok(ChunkFile)` - The file chunk.
    /// * `Err(Error)` - The error.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    pub fn create_chunk_file(file: &File, offset: u64, size: u64) -> Result<ChunkFile, Error> {
        let mut buffer = vec![0u8; size as usize];
        let bytes = file.read_at(&mut buffer, offset);
        match bytes {
            Ok(n) => Ok(ChunkFile::new()
                .bytes(Bytes::from(buffer[..n].to_vec()))
                .offset(offset)
                .size(size)
                .build()),
            Err(e) => {
                tracing::error!(
                    "Failed to read file chunk, offset: {}, size: {}, err: {:?}",
                    offset,
                    size,
                    e
                );
                Err(e.into())
            }
        }
    }
}
