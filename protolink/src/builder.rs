//! Pieces shared by the client and server factories of every driver.

/// Deadline policy of a factory that has not chosen one yet.
///
/// Factories outside [`tokio`](crate::tokio) start in this state and cannot be
/// built until a policy is chosen: async factories with
/// `.with_deadline_timer(..)` or `.without_deadlines()`, blocking factories
/// with `.with_deadline_clock(..)` or `.without_deadlines()`. This makes the
/// choice to run without local `grpc-timeout` enforcement explicit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeadlinesUnspecified;

/// Consuming setters for the fields of a [`ServerConfig`](crate::ServerConfig),
/// on a factory with a `config` field.
#[cfg(any(feature = "async", feature = "blocking"))]
macro_rules! server_config_methods {
    () => {
        /// Replace the whole server configuration.
        pub fn with_config(mut self, config: $crate::ServerConfig) -> Self {
            self.config = config;
            self
        }

        /// HTTP/2 connection limits, see
        /// [`ServerConfig::http2`](crate::ServerConfig::http2).
        pub fn with_http2(mut self, http2: $crate::grpc::http2::Config) -> Self {
            self.config.http2 = http2;
            self
        }

        /// Largest accepted request and produced response message, see
        /// [`ServerConfig::max_message_size`](crate::ServerConfig::max_message_size).
        pub fn with_max_message_size(mut self, bytes: usize) -> Self {
            self.config.max_message_size = bytes;
            self
        }

        /// Message compression, see
        /// [`ServerConfig::compression`](crate::ServerConfig::compression).
        pub fn with_compression(mut self, compression: $crate::Compression) -> Self {
            self.config.compression = compression;
            self
        }
    };
}

/// Consuming setters for the fields of a [`ClientConfig`](crate::ClientConfig),
/// on a factory with a `config` field.
#[cfg(any(feature = "async", feature = "blocking"))]
macro_rules! client_config_methods {
    () => {
        /// Replace the whole client configuration.
        pub fn with_config(mut self, config: $crate::ClientConfig) -> Self {
            self.config = config;
            self
        }

        /// HTTP/2 connection limits, see
        /// [`ClientConfig::http2`](crate::ClientConfig::http2).
        pub fn with_http2(mut self, http2: $crate::grpc::http2::Config) -> Self {
            self.config.http2 = http2;
            self
        }

        /// Largest accepted response and sent request message, see
        /// [`ClientConfig::max_message_size`](crate::ClientConfig::max_message_size).
        pub fn with_max_message_size(mut self, bytes: usize) -> Self {
            self.config.max_message_size = bytes;
            self
        }

        /// Connection-wide retained response budget, see
        /// [`ClientConfig::max_buffered_response_bytes`](crate::ClientConfig::max_buffered_response_bytes).
        pub fn with_max_buffered_response_bytes(mut self, bytes: usize) -> Self {
            self.config.max_buffered_response_bytes = bytes;
            self
        }

        /// Maximum calls retained by the client, see
        /// [`ClientConfig::max_retained_calls`](crate::ClientConfig::max_retained_calls).
        pub fn with_max_retained_calls(mut self, calls: usize) -> Self {
            self.config.max_retained_calls = calls;
            self
        }

        /// `:authority` sent with every request.
        pub fn with_authority(mut self, authority: impl Into<alloc::string::String>) -> Self {
            self.config.authority = authority.into();
            self
        }

        /// `:scheme` sent with every request. It does not enable TLS; see
        /// [`Scheme`](crate::Scheme).
        pub fn with_scheme(mut self, scheme: $crate::Scheme) -> Self {
            self.config.scheme = scheme;
            self
        }

        /// Message compression, see
        /// [`ClientConfig::compression`](crate::ClientConfig::compression).
        pub fn with_compression(mut self, compression: $crate::Compression) -> Self {
            self.config.compression = compression;
            self
        }

        /// Timeout of calls that don't set
        /// [`CallOptions::timeout`](crate::CallOptions::timeout). It is sent as
        /// `grpc-timeout`; local enforcement depends on the deadline policy.
        pub fn with_default_timeout(mut self, timeout: core::time::Duration) -> Self {
            self.config.default_timeout = Some(timeout);
            self
        }
    };
}

#[cfg(any(feature = "async", feature = "blocking"))]
pub(crate) use {client_config_methods, server_config_methods};
