use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use everscale_types::models::BlockId;
use serde::Serialize;
use tycho_control::ControlClient;
use tycho_core::block_strider::ManualGcTrigger;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;

use crate::util::print_json;

#[derive(Subcommand)]
pub enum CmdControl {
    Ping(CmdPing),
    FindArchive(CmdFindArchive),
    ListArchives(CmdListArchives),
    DumpArchive(CmdDumpArchive),
    ListBlocks(CmdListBlocks),
    DumpBlock(CmdDumpBlock),
    DumpProof(CmdDumpProof),
    DumpQueueDiff(CmdDumpQueueDiff),
    GcArchives(CmdGcArchives),
    GcBlocks(CmdGcBlocks),
    GcStates(CmdGcStates),
    #[clap(subcommand)]
    MemProfiler(CmdMemProfiler),
}

impl CmdControl {
    pub fn run(self) -> Result<()> {
        match self {
            Self::Ping(cmd) => cmd.run(),
            Self::FindArchive(cmd) => cmd.run(),
            Self::ListArchives(cmd) => cmd.run(),
            Self::DumpArchive(cmd) => cmd.run(),
            Self::ListBlocks(cmd) => cmd.run(),
            Self::DumpBlock(cmd) => cmd.run(),
            Self::DumpProof(cmd) => cmd.run(),
            Self::DumpQueueDiff(cmd) => cmd.run(),
            Self::GcArchives(cmd) => cmd.run(),
            Self::GcBlocks(cmd) => cmd.run(),
            Self::GcStates(cmd) => cmd.run(),
            Self::MemProfiler(cmd) => cmd.run(),
        }
    }
}

/// Ping the control server.
#[derive(Parser)]
pub struct CmdPing {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,
}

impl CmdPing {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, |client| async move {
            let timestamp = client.ping().await?;
            print_json(serde_json::json!({
                "timestamp": timestamp,
            }))
        })
    }
}

/// Trigger a garbage collection of archives.
#[derive(Parser)]
pub struct CmdGcArchives {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    #[clap(flatten)]
    by: TriggerBy,
}

impl CmdGcArchives {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, |client| async move {
            client.trigger_archives_gc(self.by.into()).await?;
            print_json(Empty {})
        })
    }
}

/// Trigger a garbage collection of blocks.
#[derive(Parser)]
pub struct CmdGcBlocks {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    #[clap(flatten)]
    by: TriggerBy,
}

impl CmdGcBlocks {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, |client| async move {
            client.trigger_blocks_gc(self.by.into()).await?;
            print_json(Empty {})
        })
    }
}

/// Trigger a garbage collection of states.
#[derive(Parser)]
pub struct CmdGcStates {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    #[clap(flatten)]
    by: TriggerBy,
}

impl CmdGcStates {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, |client| async move {
            client.trigger_states_gc(self.by.into()).await?;
            print_json(Empty {})
        })
    }
}

/// Memory profiler commands.
#[derive(Subcommand)]
pub enum CmdMemProfiler {
    Start(CmdProfilerStart),
    Stop(CmdProfilerStop),
    Dump(CmdProfilerDump),
}

impl CmdMemProfiler {
    pub fn run(self) -> Result<()> {
        let sock = match &self {
            Self::Start(cmd) => &cmd.sock,
            Self::Stop(cmd) => &cmd.sock,
            Self::Dump(cmd) => &cmd.sock,
        }
        .clone();

        #[derive(serde::Serialize)]
        struct ToggleResponse {
            updated: bool,
            active: bool,
        }

        control_rt(sock, |client| async move {
            match self {
                Self::Start(_) => {
                    let updated = client.set_memory_profiler_enabled(true).await?;
                    print_json(ToggleResponse {
                        updated,
                        active: true,
                    })
                }
                Self::Stop(_) => {
                    let updated = client.set_memory_profiler_enabled(false).await?;
                    print_json(ToggleResponse {
                        updated,
                        active: false,
                    })
                }
                Self::Dump(dump) => {
                    let data = client.dump_memory_profiler().await?;
                    if let Err(e) = tokio::fs::write(dump.output, data).await {
                        anyhow::bail!("failed to save dump: {e:?}");
                    }
                    print_json(Empty {})
                }
            }
        })
    }
}

/// Start the memory profiler.
#[derive(Parser)]
pub struct CmdProfilerStart {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,
}

/// Stop the memory profiler.
#[derive(Parser)]
pub struct CmdProfilerStop {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,
}

/// Dump the memory profiler data.
#[derive(Parser)]
pub struct CmdProfilerDump {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    /// path to the output file
    #[clap()]
    output: PathBuf,
}

/// Get archive info from the node.
#[derive(Parser)]
pub struct CmdFindArchive {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    /// masterchain block seqno.
    #[clap(long)]
    seqno: u32,
}

impl CmdFindArchive {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, move |client| async move {
            let Some(info) = client.find_archive(self.seqno).await? else {
                anyhow::bail!("archive not found");
            };

            print_json(serde_json::json!({
                "archive_id": info.id,
                "size": info.size,
            }))
        })
    }
}

#[derive(Parser)]
pub struct CmdListArchives {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,
    #[clap(long)]
    human_readable: bool,
}

impl CmdListArchives {
    pub fn run(self) -> Result<()> {
        fn print_human_readable(archives: &[tycho_control::proto::ArchiveInfo]) {
            use bytesize::ByteSize;

            let chunk_size = match archives.first() {
                Some(info) => info.chunk_size.get(),
                None => {
                    println!("No archives found");
                    return;
                }
            };

            // Collect formatted strings
            let formatted = archives
                .iter()
                .map(|arch| (arch.id.to_string(), ByteSize(arch.size.get()).to_string()))
                .collect::<Vec<_>>();

            // Calculate max widths
            let max_id_width = formatted.iter().map(|(id, _)| id.len()).max().unwrap_or(0);
            let max_size_width = formatted
                .iter()
                .map(|(_, size)| size.len())
                .max()
                .unwrap_or(0);

            println!("Archives:");
            println!("Chunks size: {}", ByteSize(chunk_size as _));

            // Print with calculated padding
            for (id, size) in formatted {
                println!(
                    "ID: {:<id_width$}, Size: {:>size_width$}",
                    id,
                    size,
                    id_width = max_id_width,
                    size_width = max_size_width
                );
            }
        }

        control_rt(self.sock, move |client| async move {
            let archives = client.list_archives().await?;
            if self.human_readable {
                print_human_readable(&archives);
            } else {
                print_json(archives)?;
            }

            Ok(())
        })
    }
}

/// Dump the archive from the node.
#[derive(Parser)]
pub struct CmdDumpArchive {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    /// masterchain block seqno.
    #[clap(long)]
    seqno: u32,

    /// decompress the downloaded archive.
    #[clap(short, long)]
    decompress: bool,

    /// path to the output file.
    #[clap()]
    output: PathBuf,
}

impl CmdDumpArchive {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, move |client| async move {
            let Some(info) = client.find_archive(self.seqno).await? else {
                anyhow::bail!("archive not found");
            };

            #[allow(clippy::disallowed_types)]
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&self.output)?;

            let writer = std::io::BufWriter::new(file);
            client
                .download_archive(info, self.decompress, writer)
                .await?;

            print_json(serde_json::json!({
                "archive_id": info.id,
                "compressed": !self.decompress,
                "size": info.size,
            }))
        })
    }
}

#[derive(Parser)]
pub struct CmdListBlocks {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,
    #[clap(long)]
    human_readable: bool,
    #[clap(short, long, allow_hyphen_values(true))]
    continuation: Option<BlockId>,
}

impl CmdListBlocks {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, move |client| async move {
            let blocks = client.list_blocks(self.continuation).await?;
            if self.human_readable {
                if blocks.blocks.is_empty() {
                    println!("No blocks found");
                } else {
                    println!("Blocks:");
                    for block in blocks.blocks {
                        println!("{}", block);
                    }
                    println!(
                        "Continuation: {:?}",
                        blocks.continuation.map(|block| block.to_string())
                    );
                }
            } else {
                print_json(blocks)?;
            }

            Ok(())
        })
    }
}

/// Download a block from the node.
#[derive(Parser)]
pub struct CmdDumpBlock {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    /// full block ID.
    #[clap(short, long, allow_hyphen_values(true))]
    block_id: BlockId,

    /// path to the output file
    #[clap()]
    output: PathBuf,
}

impl CmdDumpBlock {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, move |client| async move {
            let Some(data) = client.get_block(&self.block_id).await? else {
                anyhow::bail!("block not found");
            };

            let size = data.len();
            tokio::fs::write(self.output, data)
                .await
                .context("failed to save block")?;

            print_json(serde_json::json!({
                "size": size,
            }))
        })
    }
}

/// Dump a block proof from the node.
#[derive(Parser)]
pub struct CmdDumpProof {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    /// full block ID.
    #[clap(short, long, allow_hyphen_values(true))]
    block_id: BlockId,

    /// path to the output file
    #[clap()]
    output: PathBuf,
}

impl CmdDumpProof {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, move |client| async move {
            let Some(data) = client.get_block_proof(&self.block_id).await? else {
                anyhow::bail!("block proof not found");
            };

            let size = data.len();
            tokio::fs::write(self.output, data)
                .await
                .context("failed to save block proof")?;

            print_json(serde_json::json!({
                "size": size,
                "is_link": !self.block_id.is_masterchain(),
            }))
        })
    }
}

/// Dump a queue diff from the node.
#[derive(Parser)]
pub struct CmdDumpQueueDiff {
    /// Unix socket path to connect to.
    #[clap(short, long)]
    sock: Option<PathBuf>,

    /// full block ID.
    #[clap(short, long, allow_hyphen_values(true))]
    block_id: BlockId,

    /// path to the output file
    #[clap()]
    output: PathBuf,
}

impl CmdDumpQueueDiff {
    pub fn run(self) -> Result<()> {
        control_rt(self.sock, move |client| async move {
            let Some(data) = client.get_queue_diff(&self.block_id).await? else {
                anyhow::bail!("queue diff not found");
            };

            let size = data.len();
            tokio::fs::write(self.output, data)
                .await
                .context("failed to save queue diff")?;

            print_json(serde_json::json!({
                "size": size,
            }))
        })
    }
}

#[derive(Parser)]
#[group(required = true, multiple = false)]
struct TriggerBy {
    /// Triggers GC for the specified MC block seqno.
    #[clap(long)]
    pub seqno: Option<u32>,
    /// Triggers GC for the MC block seqno relative to the latest MC block.
    #[clap(long)]
    pub distance: Option<u32>,
}

impl From<TriggerBy> for ManualGcTrigger {
    fn from(value: TriggerBy) -> Self {
        match (value.seqno, value.distance) {
            (Some(seqno), None) => ManualGcTrigger::Exact(seqno),
            (None, Some(distance)) => ManualGcTrigger::Distance(distance),
            _ => unreachable!(),
        }
    }
}

#[derive(Serialize)]
struct Empty {}

fn control_rt<P, F, FT>(sock: Option<P>, f: F) -> Result<()>
where
    P: AsRef<Path>,
    F: FnOnce(ControlClient) -> FT + Send + 'static,
    FT: Future<Output = Result<()>> + Send,
{
    tracing_subscriber::fmt::init();

    let sock = match sock {
        Some(sock) => sock.as_ref().to_owned(),
        None => match std::env::var("TYCHO_CONTROL_SOCK") {
            Ok(sock) => PathBuf::from(sock),
            Err(_) => PathBuf::from(tycho_control::DEFAULT_SOCKET_PATH),
        },
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let run_fut = JoinTask::new(async move {
                let client = ControlClient::connect(sock)
                    .await
                    .context("failed to connect to control server")?;
                f(client).await
            });
            let stop_fut = signal::any_signal(signal::TERMINATION_SIGNALS);
            tokio::select! {
                res = run_fut => res,
                signal = stop_fut => match signal {
                    Ok(signal) => {
                        tracing::info!(?signal, "received termination signal");
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                }
            }
        })
}
