#[macro_export]
macro_rules! define_operation_struct {
    ($name:ident) => {
        use ::derive_builder::Builder;

        #[derive(Builder)]
        pub struct $name {
            object_config: $crate::api::ObjectConfig,
            // auth_service: $crate::AuthorizationService,
            client: $crate::client::HttpClient,
        }

        impl $crate::api::Sealed for $name {}
    };
}

#[macro_export]
macro_rules! define_api_request {
    (
        $request_name: ident,
        $operation_name: ty,
        $response_type: ty,
        {
            $(
                $(#[$attr:meta])*
                $vis: vis $field: ident: $field_type: ty
            ),* $(,)?
        }
    ) => {
        /// Request configuration
        #[derive(::derive_builder::Builder)]
        #[builder(pattern = "owned")]
        pub struct $request_name {
            $(
                $(#[$attr])*
                $vis $field: $field_type,
            )*


            /// Required Object Config.
            #[builder(setter(strip_option))]
            object_config: ::std::option::Option<$crate::api::ObjectConfig>,

            /// Required Http Client.
            #[builder(setter(strip_option))]
            client: ::std::option::Option<$crate::client::HttpClient>,
        }

        impl $crate::api::Sealed for $request_name {}

        #[async_trait::async_trait]
        impl $crate::api::ApiRequest for $request_name {
            type Response = $response_type;
            type Error = ::anyhow::Error;

            async fn request(mut self) -> Result<Self::Response, Self::Error> {
                use $crate::api::ApiOperation;
                let object_config = self.object_config.take().unwrap();
                let client = self.client.take().unwrap();
                let operation = <$operation_name>::default()
                    .object_config(object_config)
                    .client(client)
                    .build()
                    .map_err(|e| {
                        ::tracing::error!("Failed to build operation, err: {:?}", e);
                        anyhow::Error::from(e)
                    })?;

                operation.execute(self).await
            }
        }
    };
}
