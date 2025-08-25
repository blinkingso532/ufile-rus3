#![allow(unused)]
#![allow(unused_variables)]
pub mod byte;
pub mod digest;
pub mod fs;

use std::{
    fmt::{Display, Formatter},
    fs::File,
    io::Read,
    path::Path,
};

use anyhow::anyhow;
use base64::{Engine, engine::general_purpose};
use sha1::{Digest, Sha1};

use byteorder::{LittleEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};

/// Sha1 Length
pub(crate) const SHA1_DIGEST_LENGTH: usize = 20;

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct ETag {
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "PartEtags")]
    pub part_etags: Vec<String>,
}

impl ETag {
    /// Create a new instance of ETag.
    fn new() -> Self {
        Self {
            etag: String::new(),
            part_etags: vec![],
        }
    }

    pub fn from_file(file_path: impl AsRef<Path>, part_size: u32) -> Result<Self, anyhow::Error> {
        if part_size == 0 {
            return Err(anyhow!("part size is 0"));
        }
        let mut file = File::open(file_path)?;
        let size = file.metadata()?.len();
        let block_count = ((size as f64) / (part_size as f64)).ceil() as u32;

        // head from block size.
        // 准备头部和缓冲区
        let mut head = Vec::new();
        head.write_u32::<LittleEndian>(block_count)?;

        let mut buff = Vec::with_capacity(4 + SHA1_DIGEST_LENGTH);
        buff.extend_from_slice(&head);
        buff.resize(4 + SHA1_DIGEST_LENGTH, 0);

        let mut cache = vec![0; part_size as usize];
        let mut e_tag = Self::default();

        if block_count > 1 {
            let mut digest = Sha1::new();

            for _ in 0..block_count {
                let bytes_read = file.read(&mut cache)?;
                if bytes_read == 0 {
                    break;
                }

                // 计算当前块的SHA1
                let mut block_hasher = Sha1::new();
                block_hasher.update(&cache[0..bytes_read]);
                let block_hash = block_hasher.finalize();

                // 保存块哈希并更新主哈希
                let block_hash_b64 = general_purpose::URL_SAFE.encode(block_hash);
                e_tag.part_etags.push(block_hash_b64);
                digest.update(block_hash);
            }

            let sha1_res = digest.finalize();
            buff[4..4 + SHA1_DIGEST_LENGTH].copy_from_slice(&sha1_res);
        } else {
            // 单个块的情况
            let bytes_read = file.read(&mut cache)?;
            if bytes_read > 0 {
                let mut digest = Sha1::new();
                digest.update(&cache[0..bytes_read]);
                let sha1_res = digest.finalize();
                buff[4..4 + SHA1_DIGEST_LENGTH].copy_from_slice(&sha1_res);
            }
        }

        e_tag.etag = general_purpose::URL_SAFE.encode(&buff);
        Ok(e_tag)
    }
}

impl Display for ETag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[test]
fn test_buff() {
    let block_size = 192;
    // head from block size.
    let mut head = Vec::new();
    head.write_u32::<LittleEndian>(block_size)
        .expect("Write block size to head failed");
    let mut buff: Vec<u8> = Vec::with_capacity(head.len() + SHA1_DIGEST_LENGTH);
    buff.extend_from_slice(head.as_slice());
    buff.resize(head.len() + SHA1_DIGEST_LENGTH, 0);
    println!("{:?}", buff);
}

#[test]
fn test_etag() {
    let etag = ETag::from_file("HardOps_v988.zip", super::constant::MULTIPART_SIZE).unwrap();
    let expected_etag = r#"{"PartEtags":["SHJn1NH0cVPKCvHqon457YNuW7A=","pkOIt9dDJitEoK8MPYmf7YdIako="],"ETag":"AgAAADOfVWTkq2axMvoOXgssP8tkSWxn"}"#;
    println!("{}", expected_etag);
    println!("{}", etag.to_string());
}
