use sha1::{Digest, Sha1};

/// A method used to calc hash value of source with sha1 digest alg.
pub fn sha1(source: impl AsRef<[u8]>) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(source);
    hasher.finalize().to_vec()
}
