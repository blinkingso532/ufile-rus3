#[macro_export]
macro_rules! define_operation_struct {
    ($name:ident, $config_name:ident) => {
        use ::builder_pattern::Builder;
        use $crate::AuthorizationService;
        use $crate::api::ObjectConfig;
        use $crate::api::Sealed;
        use $crate::client::HttpClient;

        #[derive(Builder)]
        pub struct $name {
            #[public]
            config: $config_name,
            #[public]
            object_config: ObjectConfig,
            #[public]
            auth_service: AuthorizationService,
            #[public]
            client: HttpClient,
        }

        impl Sealed for $name {}
    };
}
