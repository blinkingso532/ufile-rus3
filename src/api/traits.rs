use std::sync::Arc;

use anyhow::Error;

use crate::api::{AuthorizationService, client::ApiClient, object::ObjectConfig};

#[async_trait::async_trait]
pub trait ApiExecutor<R> {
    /// Method to execute the api request.
    async fn execute(
        &mut self,
        object_config: ObjectConfig,
        api_client: Arc<ApiClient>,
        auth_service: AuthorizationService,
    ) -> Result<R, Error>;
}
