use thiserror::Error;

pub type UFileResult<T> = ::std::result::Result<T, UFileError>;

#[derive(Debug, Error)]
pub enum UFileError {
    #[error("Api error")]
    ApiError,
    #[error("Network request error for: {0}")]
    NetworkError(String),
    #[error("Serialize error")]
    SerializeError,
    #[error("Deserialize error")]
    DeserializeError,
    #[error("Unknown error")]
    UnknownError,
}
