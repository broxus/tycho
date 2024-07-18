use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use clap::{Parser, ValueEnum};
use everscale_types::models::BlockId;
use serde::Serialize;
use tarpc::serde::Deserialize;
use tarpc::tokio_serde::formats::Bincode;
use tarpc::{client, context};

use crate::ControlServerClient;

pub async fn get_client(server_address: SocketAddr) -> anyhow::Result<ControlServerClient> {
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
    pub ty: ManualTriggerValue,
    pub seqno: Option<u32>,
    pub distance: Option<u32>,
    pub node_addr: SocketAddr,
}

#[derive(ValueEnum, Debug, Clone, Deserialize, Serialize)]
#[clap(rename_all = "kebab_case")]
pub enum ManualTriggerValue {
    Blocks,
    Archives,
    States
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
            .trigger_gc(context::current(), self.ty.clone(), self.seqno, self.distance)
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
        let client = match get_client(self.node_addr).await {
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
            println!(
                "Downloading archive request size {request_size}, current offset {current_offset}"
            );
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
                Err(e) => {
                    println!("Failed to get archive slice. Transport error: {e:?}");
                    return;
                }
            }
        }

        println!("Archive saved to: {:?}", &self.to)
    }
}
