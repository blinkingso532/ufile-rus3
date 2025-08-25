#[macro_export]
macro_rules! define_operation_struct {
    ($name:ident, $config_name:ident) => {
        use paste::paste;
        use $crate::AuthorizationService;
        use $crate::api::ObjectConfig;
        use $crate::api::Sealed;
        use $crate::client::HttpClient;

        pub struct $name {
            config: $config_name,
            object_config: ObjectConfig,
            auth_service: AuthorizationService,
            client: HttpClient,
        }

        impl Sealed for $name {}

        #[allow(unused)]
        impl $name {
            pub fn new(
                config: $config_name,
                object_config: ObjectConfig,
                auth_service: AuthorizationService,
                client: HttpClient,
            ) -> Self {
                Self {
                    config,
                    object_config,
                    auth_service,
                    client,
                }
            }
        }

        paste! {

            #[allow(unused)]
            pub struct [<$name Builder>] {
                config: $config_name,
            }

            #[allow(unused)]
            impl [<$name Builder>] {
                pub fn new(config: $config_name) -> Self {
                    Self { config }
                }

                pub fn build(
                    self,
                    object_config: ObjectConfig,
                    auth_service: AuthorizationService,
                    client: HttpClient,
                ) -> $name {
                    $name::new(self.config, object_config, auth_service, client)
                }
            }
        }
    };
}
