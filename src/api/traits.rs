use crate::api::traits::sealed::Sealed;

#[async_trait::async_trait]
pub trait ApiRequest: Sealed {
    type Response;
    type Error;

    /// This method consumed the request and return the response.
    /// Create ApiOperation impl and invoke `ApiOperation`'s execute method to consume self.
    ///
    /// # Errors
    ///
    /// This method will return an error if the request is invalid.
    async fn request(self) -> Result<Self::Response, Self::Error>;
}

#[async_trait::async_trait]
pub trait ApiOperation: Sealed {
    type Request;
    type Response;
    type Error;

    /// This method is used to execute the API operation.
    async fn execute(&self, req: Self::Request) -> Result<Self::Response, Self::Error>;
}

pub mod sealed {
    pub trait Sealed {}
}
