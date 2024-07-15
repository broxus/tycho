use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use clap::Parser;
use everscale_types::models::BlockId;
use tarpc::serde::Deserialize;
use tarpc::tokio_serde::formats::{Bincode, Json};
use tarpc::{client, context};
use tycho_core::block_strider::ManualGcTrigger;

use crate::ControlServerClient;

pub async fn get_client(server_address: SocketAddr) -> anyhow::Result<ControlServerClient> {
    let mut transport = tarpc::serde_transport::tcp::connect(server_address, Json::default);
    transport.config_mut().max_frame_length(usize::MAX);

    Ok(ControlServerClient::new(client::Config::default(), transport.await?).spawn())
}

pub async fn get_bincode_client(server_address: SocketAddr) -> anyhow::Result<ControlServerClient> {
    let mut transport = tarpc::serde_transport::tcp::connect(server_address, Bincode::default);
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
        if let Err(e) = client
            .trigger_gc(context::current(), ManualGcTrigger::Blocks) // TODO: from console
            .await
        {
            println!("Failed to trigger GC: {e:?}")
        }
    }
}

#[derive(Deserialize, Parser)]
pub struct SwitchMemoryProfilerCmd {
    pub target: bool,
    pub node_addr: SocketAddr,
}

impl SwitchMemoryProfilerCmd {
    pub async fn run(&self) {
        let client = match get_client(self.node_addr).await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to create cli. {e:?}");
                return;
            }
        };
        if let Err(e) = client
            .trigger_memory_profiler(context::current(), self.target)
            .await
        {
            println!("Failed to trigger memory profiler: {e:?}")
        }
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
            println!("Block full received: ");
            println!("Id {}", block.id);
            println!("Block {}", BASE64_STANDARD.encode(block.block));
            println!("Proof {}", BASE64_STANDARD.encode(block.proof));
            println!("Proof is link: {}", block.is_link)
        } else {
            println!("Block not found");
        }
    }
}

#[derive(Deserialize, Parser)]
pub struct GetNextKeyBlockIdsCmd {
    pub block_id: BlockId,
    pub limit: usize,
    pub node_addr: SocketAddr,
}

#[derive(Deserialize, Parser)]
pub struct FindArchiveCmd {
    pub mc_seqno: u32,
    pub node_addr: SocketAddr,
}

impl FindArchiveCmd {
    pub async fn run(&self) {
        let client = match get_client(self.node_addr).await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to create cli. {e:?}");
                return;
            }
        };
        match client
            .get_archive_info(context::current(), self.mc_seqno)
            .await
        {
            Ok(Some(id)) => println!("Found archive {id}"),
            Ok(None) => println!("Archive not found"),
            Err(e) => println!("Failed to find archive. {e:?}"),
        }
    }
}

#[derive(Deserialize, Parser)]
pub struct DumpArchiveCmd {
    pub id: u32,
    pub to: PathBuf,
    pub node_addr: SocketAddr,
}

impl DumpArchiveCmd {
    pub async fn run(&self) {
        let client = match get_bincode_client(self.node_addr).await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to create cli. {e:?}");
                return;
            }
        };

        let mut file = match File::create(&self.to) {
            Ok(file) => file,
            Err(e) => {
                println!("Failed to create file. {e:?}");
                return;
            }
        };


        let mut current_offset: u64 = 0;
        let request_size = 1048576; // 1 mb

        loop {
            match client
                .get_archive_slice(context::current(), self.id, request_size, current_offset)
                .await
            {
                Ok(Some(bytes)) => {
                    if let Err(e) = file.write(bytes.as_slice()) {
                        println!("Failed to write archive slice to file. {e:?}")
                    }
                    if bytes.len() > request_size as usize {
                        println!("Got more bytes than requested");
                        return;
                    }

                    if bytes.len() < request_size as usize {
                        break;
                    }

                    current_offset += bytes.len() as u64
                }
                Ok(None) => println!("Archive slice not found."),
                Err(e) => println!("Failed to get archive slice. Transport error: {e:?}"),
            }
        }

        println!("Archive saved to: {:?}", &self.to )
    }
}
