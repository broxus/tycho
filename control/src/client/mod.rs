use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;

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
    #[arg(short, long)]
    pub value: u32,
    #[arg(short, long)]
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

        let Ok(pong) = client.ping(context::current(), self.value).await else {
            println!("Failed to get pong");
            return;
        };

        println!("Pong {pong} received")
    }
}

#[derive(Deserialize, Parser)]
pub struct TriggerGcCmd {
    #[arg(short, long)]
    pub ty: ManualTriggerValue,
    #[arg(short, long)]
    pub seqno: Option<u32>,
    #[arg(short, long)]
    pub distance: Option<u32>,
    #[arg(short, long)]
    pub node_addr: SocketAddr,
}

#[derive(ValueEnum, Debug, Clone, Deserialize, Serialize)]
#[clap(rename_all = "kebab_case")]
pub enum ManualTriggerValue {
    Blocks,
    Archives,
    States,
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
            .trigger_gc(
                context::current(),
                self.ty.clone(),
                self.seqno,
                self.distance,
            )
            .await
        {
            println!("Failed to trigger GC: {e:?}")
        }
    }
}

#[derive(Deserialize, Parser)]
pub struct SwitchMemoryProfilerCmd {
    #[arg(short, long)]
    pub target: bool,
    #[arg(short, long)]
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
pub struct DumpBlock {
    #[arg(short, long)]
    pub block_id: BlockId,
    #[arg(short, long)]
    pub to: PathBuf,
    #[arg(short, long)]
    pub node_addr: SocketAddr,
}

impl DumpBlock {
    pub async fn dump_block(&self) {
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
                println!("Failed to create block file. {e:?}");
                return;
            }
        };

        let Ok(block_opt) = client.get_block(context::current(), self.block_id).await else {
            println!("Failed to get block data");
            return;
        };

        if let Some(block) = block_opt {
            if let Err(e) = file.write(block.as_slice()) {
                println!("Failed to write block data to file. {e:?}")
            }

            println!("Block full saved to {:?} ", &self.to);
        } else {
            println!("Block not found");
        }
    }

    pub async fn dump_block_proof(&self) {
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
                println!("Failed to create proof file. {e:?}");
                return;
            }
        };

        let Ok(block_opt) = client
            .get_block_proof(context::current(), self.block_id)
            .await
        else {
            println!("Failed to get block proof");
            return;
        };

        if let Some((proof, is_link)) = block_opt {
            if let Err(e) = file.write(proof.as_slice()) {
                println!("Failed to write block proof to file. {e:?}")
            }

            println!("Block proof saved to {:?} ", &self.to);
            println!("Block proof is link: {is_link}");
        } else {
            println!("Block not found");
        }
    }
}

#[derive(Deserialize, Parser)]
pub struct GetNextKeyBlockIdsCmd {
    #[arg(short, long)]
    pub block_id: BlockId,
    #[arg(short, long)]
    pub limit: usize,
    #[arg(short, long)]
    pub node_addr: SocketAddr,
}

#[derive(Deserialize, Parser)]
pub struct FindArchiveCmd {
    #[arg(short, long)]
    pub mc_seqno: u32,
    #[arg(short, long)]
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
    #[arg(short, long)]
    pub id: u32,
    #[arg(short, long)]
    pub to: PathBuf,
    #[arg(short, long)]
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
