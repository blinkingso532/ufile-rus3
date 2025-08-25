use crate::api::traits::sealed::Sealed;

#[async_trait::async_trait]
pub trait ApiOperation: Sealed {
    type Response;
    type Error;

    async fn execute(&self) -> Result<Self::Response, Self::Error>;
}

pub mod sealed {
    pub trait Sealed {}
}
