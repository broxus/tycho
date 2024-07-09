use std::net::SocketAddr;
use anyhow::Result;
use clap::Parser;
use tarpc::client;
use tarpc::tokio_serde::formats::Json;
use crate::ControlServerClient;


#[derive(Parser)]
struct Flags {
    /// Sets the server address to connect to.
    #[clap(long)]
    server_addr: SocketAddr,
    /// Sets the name to say hello to.
    #[clap(long)]
    name: String,
}


pub async fn get_client(server_address: SocketAddr) -> Result<ControlServerClient> {
    let mut transport = tarpc::serde_transport::tcp::connect(server_address, Json::default);
    transport.config_mut().max_frame_length(usize::MAX);

    Ok(ControlServerClient::new(client::Config::default(), transport.await?).spawn())
}