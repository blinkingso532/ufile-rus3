//! Simple Form Post File. Not implemented now.

use builder_pattern::Builder;

use crate::api::validator::is_bucket_name_not_empty;

#[derive(Builder)]
pub struct PostFileApi {
    #[validator(is_bucket_name_not_empty)]
    pub bucket_name: String,

    #[default(false)]
    pub is_verify_md5: bool,

    /// sts temporary security token
    #[default(None)]
    pub security_token: Option<String>,
}
