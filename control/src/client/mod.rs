use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use clap::{Parser, Subcommand};
use everscale_types::models::BlockId;
use tarpc::serde::Deserialize;
use tarpc::tokio_serde::formats::Json;
use tarpc::{client, context};

use crate::ControlServerClient;

pub async fn get_client(server_address: SocketAddr) -> anyhow::Result<ControlServerClient> {
    let mut transport = tarpc::serde_transport::tcp::connect(server_address, Json::default);
    transport.config_mut().max_frame_length(usize::MAX);

    Ok(ControlServerClient::new(client::Config::default(), transport.await?).spawn())
}

#[derive(Deserialize, Parser)]
pub struct PingCmd {
    pub i: u32,
    pub node_addr: SocketAddr,
}

impl PingCmd {
    pub async fn run(&self) {
        let client = match get_client(self.node_addr).await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to create cli. {e:?}");
                return;
            }
        };

        let Ok(pong) = client.ping(context::current(), self.i).await else {
            println!("Failed to get pong");
            return;
        };

        println!("Pong {pong} received")
    }
}

#[derive(Deserialize, Parser)]
pub struct TriggerGcCmd {
    pub mc_block_id: BlockId,
    pub last_key_block_id: u32,
    pub node_addr: SocketAddr,
}

impl TriggerGcCmd {
    pub async fn run(&self) {
        let client = match get_client(self.node_addr).await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to create cli. {e:?}");
                return;
            }
        };
    }
}

#[derive(Deserialize, Parser)]
pub struct GetBlockFullCmd {
    pub block_id: BlockId,
    pub node_addr: SocketAddr,
}

impl GetBlockFullCmd {
    pub async fn run(&self) {
        let client = match get_client(self.node_addr).await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to create cli. {e:?}");
                return;
            }
        };

        let Ok(block_opt) = client
            .get_block_full(context::current(), self.block_id)
            .await
        else {
            println!("Failed to get block full");
            return;
        };

        if let Some(block) = block_opt {
            println!("Block full received: \n");
            println!("Block {}", block.is_link);
            println!("Id {}", block.id);
        } else {
            println!("Block not found");
        }
    }
}
