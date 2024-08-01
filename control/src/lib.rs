use std::future::Future;
use std::path::Path;

use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt};

pub use self::client::ControlClient;
pub use self::error::{ClientError, ServerResult};
pub use self::server::impls::*;
pub use self::server::{
    ArchiveInfo, ArchiveInfoRequest, ArchiveInfoResponse, ArchiveSliceRequest,
    ArchiveSliceResponse, BlockProofRequest, BlockProofResponse, BlockRequest, BlockResponse,
    ControlServer,
};

pub type Context = tarpc::context::Context;

mod client;
mod error;
mod server;

// TODO: Change the path to a more general setup.
pub const DEFAULT_SOCKET_PATH: &str = "/var/venom/data/tycho.sock";

pub struct ControlEndpoint {
    inner: BoxFuture<'static, ()>,
}

impl ControlEndpoint {
    pub async fn bind<P, S>(path: P, server: S) -> std::io::Result<Self>
    where
        P: AsRef<Path>,
        S: ControlServerExt,
    {
        use tarpc::tokio_serde::formats::Bincode;

        let mut listener = tarpc::serde_transport::unix::listen(path, Bincode::default).await?;
        listener.config_mut().max_frame_length(usize::MAX);

        let inner = listener
            // Ignore accept errors.
            .filter_map(|r| futures_util::future::ready(r.ok()))
            .map(tarpc::server::BaseChannel::with_defaults)
            .map(move |channel| server.clone().execute_all(channel))
            // Max 1 channel.
            .buffer_unordered(1)
            .for_each(|_| async {})
            .boxed();

        Ok(Self { inner })
    }

    pub async fn serve(self) {
        self.inner.await;
    }
}

// FIXME: Remove when https://github.com/google/tarpc/pull/448 is merged.
#[macro_export]
macro_rules! impl_serve {
    ($ident:ident) => {
        impl $crate::ControlServerExt for $ident {
            fn execute_all<T>(
                self,
                channel: $crate::__internal::RawChannel<T>,
            ) -> impl ::std::future::Future<Output = ()> + Send
            where
                T: tarpc::Transport<
                        $crate::__internal::tarpc::Response<$crate::__internal::RawResponse>,
                        $crate::__internal::tarpc::ClientMessage<$crate::__internal::RawRequest>,
                    > + Send
                    + 'static,
            {
                use $crate::__internal::futures_util::{future, StreamExt};
                use $crate::__internal::tarpc::server::Channel;

                channel.execute(self.serve()).for_each(|t| {
                    $crate::__internal::tokio::spawn(t);
                    future::ready(())
                })
            }
        }
    };
}

pub trait ControlServerExt: ControlServer + Clone + Send + 'static {
    fn execute_all<T>(self, channel: __internal::RawChannel<T>) -> impl Future<Output = ()> + Send
    where
        T: tarpc::Transport<
                tarpc::Response<__internal::RawResponse>,
                tarpc::ClientMessage<__internal::RawRequest>,
            > + Send
            + 'static;
}

#[doc(hidden)]
pub mod __internal {
    pub use {futures_util, tarpc, tokio};

    pub type RawChannel<T> = tarpc::server::BaseChannel<RawRequest, RawResponse, T>;
    pub type RawRequest = crate::server::ControlServerRequest;
    pub type RawResponse = crate::server::ControlServerResponse;
}
