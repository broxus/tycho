use futures_util::StreamExt;
use tokio::net::ToSocketAddrs;

pub use self::client::ControlClient;
pub use self::error::{ClientError, ServerResult};
pub use self::server::ControlServer;

mod client;
mod error;
mod server;

pub async fn serve<A, S>(addr: A, server: S) -> std::io::Result<()>
where
    A: ToSocketAddrs,
    S: ControlServer + Clone + Send + 'static,
{
    use tarpc::server::{self, Channel};
    use tarpc::tokio_serde::formats::Bincode;

    let mut listener = tarpc::serde_transport::tcp::listen(&addr, Bincode::default).await?;
    tracing::info!(listen_addr = %listener.local_addr(), "control server started");

    listener.config_mut().max_frame_length(usize::MAX);
    listener
        // Ignore accept errors.
        .filter_map(|r| futures_util::future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .map(move |channel| {
            channel
                .execute(server.clone().serve())
                .for_each(|x| async move {
                    tokio::spawn(x);
                })
        })
        // Max 1 channel.
        .buffer_unordered(1)
        .for_each(|_| async {})
        .await;

    Ok(())
}
