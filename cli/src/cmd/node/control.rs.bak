use std::borrow::Cow;
use std::collections::VecDeque;
use std::future::Future;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use tycho_control::ControlClient;
use tycho_types::models::{BlockId, StdAddr};
use tycho_util::cli::logger::init_logger_simple;
use tycho_util::cli::signal;
use tycho_util::futures::JoinTask;

use crate::BaseArgs;
use crate::util::print_json;

#[derive(Subcommand)]
pub enum CmdControl {
    Status(CmdStatus),
    Ping(CmdPing),
    GetAccount(CmdGetAccount),
    GetNeighbours(CmdGetNeighbours),
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
    Compact(CmdCompact),
    WaitSync(CmdWaitSync),
    #[clap(subcommand)]
    MemProfiler(CmdMemProfiler),
}

impl CmdControl {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        match self {
            Self::Status(cmd) => cmd.run(args),
            Self::Ping(cmd) => cmd.run(args),
            Self::GetAccount(cmd) => cmd.run(args),
            Self::GetNeighbours(cmd) => cmd.run(args),
            Self::FindArchive(cmd) => cmd.run(args),
            Self::ListArchives(cmd) => cmd.run(args),
            Self::DumpArchive(cmd) => cmd.run(args),
            Self::ListBlocks(cmd) => cmd.run(args),
            Self::DumpBlock(cmd) => cmd.run(args),
            Self::DumpProof(cmd) => cmd.run(args),
            Self::DumpQueueDiff(cmd) => cmd.run(args),
            Self::GcArchives(cmd) => cmd.run(args),
            Self::GcBlocks(cmd) => cmd.run(args),
            Self::GcStates(cmd) => cmd.run(args),
            Self::Compact(cmd) => cmd.run(args),
            Self::WaitSync(cmd) => cmd.run(args),
            Self::MemProfiler(cmd) => cmd.run(args),
        }
    }
}

/// Get node status.
#[derive(Parser)]
pub struct CmdStatus {
    #[clap(flatten)]
    args: ControlArgs,
}

impl CmdStatus {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, |client| async move {
            const SYNC_THRESHOLD: u32 = 300; // 5m

            let status = client.get_status().await?;

            let (mc_seqno, mc_block_id, time_diff, is_synced) = match status.last_applied_block {
                None => (None, None, None, false),
                Some(b) => {
                    let diff = status.status_at.saturating_sub(b.gen_utime);
                    (
                        Some(b.block_id.seqno),
                        Some(b.block_id),
                        Some(diff),
                        diff <= SYNC_THRESHOLD,
                    )
                }
            };

            let (in_current_vset, in_next_vset, is_elected) = match status.validator_status {
                None => (false, false, false),
                Some(s) => (s.in_current_vset, s.in_next_vset, s.is_elected),
            };

            print_json(serde_json::json!({
                "tycho_version": status.node_info.version,
                "tycho_build": status.node_info.build,
                "public_addr": status.node_info.public_addr,
                "local_addr": status.node_info.local_addr.to_string(),
                "adnl_id": status.node_info.adnl_id,
                "collator": status.node_info.collator.map(|c| {
                    serde_json::json!({
                        "supported_block_version": c.global_version.version,
                        "supported_capabilities": c.global_version.capabilities,
                    })
                }),
                "init_mc_seqno": status.init_block_id.map(|id| id.seqno),
                "init_mc_block_id": status.init_block_id.map(|id| id.to_string()),
                "latest_mc_seqno": mc_seqno,
                "latest_mc_block_id": mc_block_id.map(|id| id.to_string()),
                "time_diff": time_diff,
                "is_synced": is_synced,
                "in_current_vset": in_current_vset,
                "in_next_vset": in_next_vset,
                "is_elected": is_elected,
            }))
        })
    }
}

/// Ping the control server.
#[derive(Parser)]
pub struct CmdPing {
    #[clap(flatten)]
    args: ControlArgs,
}

impl CmdPing {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, |client| async move {
            let timestamp = client.ping().await?;
            print_json(serde_json::json!({
                "timestamp": timestamp,
            }))
        })
    }
}

/// Get account state from the node.
#[derive(Parser)]
pub struct CmdGetAccount {
    #[clap(flatten)]
    args: ControlArgs,

    /// Account address.
    #[clap(long, short, allow_hyphen_values(true))]
    addr: StdAddr,

    /// Parse the account state.
    #[clap(short, long)]
    parse: bool,
}

impl CmdGetAccount {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, move |client| async move {
            let state = client.get_account_state(&self.addr).await?;
            if self.parse {
                print_json(state.parse()?)
            } else {
                print_json(serde_json::json!({
                    "mc_seqno": state.mc_seqno,
                    "gen_utime": state.gen_utime,
                    "state": BASE64_STANDARD.encode(state.state),
                }))
            }
        })
    }
}

/// Get list of all known public overlay neighbours.
#[derive(Parser)]
#[clap(disable_help_flag = true)]
pub struct CmdGetNeighbours {
    #[clap(flatten)]
    args: ControlArgs,

    #[clap(short, long)]
    human_readable: bool,

    #[clap(long, action = clap::ArgAction::HelpLong)]
    help: Option<bool>,
}

impl CmdGetNeighbours {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        struct TableRow(tycho_control::proto::NeighbourInfo);

        impl tabled::Tabled for TableRow {
            const LENGTH: usize = 5;

            fn fields(&self) -> Vec<Cow<'_, str>> {
                vec![
                    Cow::from(self.0.id.to_string()),
                    Cow::from(self.0.score.to_string()),
                    Cow::from(self.0.failed_requests.to_string()),
                    Cow::from(self.0.total_requests.to_string()),
                    Cow::from(self.0.roundtrip_ms.to_string()),
                ]
            }

            fn headers() -> Vec<Cow<'static, str>> {
                vec![
                    Cow::from("peer_id"),
                    Cow::from("score"),
                    Cow::from("failed_requests"),
                    Cow::from("total_requests"),
                    Cow::from("roundtrip_ms"),
                ]
            }
        }

        self.args.rt(args, move |client| async move {
            let res = client.get_neighbours_info().await?;

            if self.human_readable {
                let mut table = tabled::Table::new(res.neighbours.into_iter().map(TableRow));
                table.with(tabled::settings::Style::psql());
                println!("{table}");
                Ok(())
            } else {
                print_json(res)
            }
        })
    }
}

/// Trigger a garbage collection of archives.
#[derive(Parser)]
pub struct CmdGcArchives {
    #[clap(flatten)]
    args: ControlArgs,

    #[clap(flatten)]
    by: TriggerBy,
}

impl CmdGcArchives {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, |client| async move {
            client.trigger_archives_gc(self.by.into()).await?;
            print_json(Empty {})
        })
    }
}

/// Trigger a garbage collection of blocks.
#[derive(Parser)]
pub struct CmdGcBlocks {
    #[clap(flatten)]
    args: ControlArgs,

    #[clap(flatten)]
    by: TriggerBy,
}

impl CmdGcBlocks {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, |client| async move {
            client.trigger_blocks_gc(self.by.into()).await?;
            print_json(Empty {})
        })
    }
}

/// Trigger a garbage collection of states.
#[derive(Parser)]
pub struct CmdGcStates {
    #[clap(flatten)]
    args: ControlArgs,

    #[clap(flatten)]
    by: TriggerBy,
}

impl CmdGcStates {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, |client| async move {
            client.trigger_states_gc(self.by.into()).await?;
            print_json(Empty {})
        })
    }
}

/// Trigger a compaction in database.
#[derive(Parser)]
pub struct CmdCompact {
    #[clap(flatten)]
    args: ControlArgs,

    /// DB name. Possible values are: `base`, `mempool`, `rpc` or other.
    #[clap(short, long, value_parser = ["base", "mempool", "rpc"])]
    database: String,
}

impl CmdCompact {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, |client| async move {
            client
                .trigger_compaction(tycho_control::proto::TriggerCompactionRequest {
                    database: self.database,
                })
                .await?;
            print_json(Empty {})
        })
    }
}

/// Wait until node synced.
#[derive(Parser)]
pub struct CmdWaitSync {
    #[clap(flatten)]
    args: ControlArgs,

    /// Threshold node time diff.
    #[clap(short, long, value_parser = humantime::parse_duration, default_value = "10s")]
    timediff: Duration,

    /// Size of the sliding window used to track recent sync statuses.
    #[clap(long, default_value_t = 10)]
    sample_window_size: usize,

    /// Minimum number of successful samples required to consider
    /// the system as totally synced.
    #[clap(long, default_value_t = 7)]
    min_required_samples: usize,
}

impl CmdWaitSync {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        let timediff = self.timediff.as_secs() as u32;
        let window_size = self.sample_window_size;
        let required_samples = self.min_required_samples;

        if required_samples > window_size {
            anyhow::bail!(
                "invalid argument: min_required_samples ({required_samples}) cannot be greater than sample_window_size ({window_size})",
            );
        }

        self.args.rt(args, move |client| async move {
            let mut history = VecDeque::with_capacity(window_size);

            loop {
                let status = client.get_status().await?;

                let is_synced = match status.last_applied_block {
                    None => false,
                    Some(b) => {
                        let diff = status.status_at.saturating_sub(b.gen_utime);
                        diff <= timediff
                    }
                };

                // Update the sliding window
                if history.len() == window_size {
                    history.pop_front();
                }
                history.push_back(is_synced);

                let synced_count = history.iter().filter(|&&s| s).count();
                if synced_count >= required_samples {
                    // Node totally synced
                    break;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            print_json(Empty {})
        })
    }
}

/// Manage memory profiler.
#[derive(Subcommand)]
pub enum CmdMemProfiler {
    Start(CmdProfilerStart),
    Stop(CmdProfilerStop),
    Dump(CmdProfilerDump),
}

impl CmdMemProfiler {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        let control = match &self {
            Self::Start(cmd) => &cmd.args,
            Self::Stop(cmd) => &cmd.args,
            Self::Dump(cmd) => &cmd.args,
        }
        .clone();

        #[derive(serde::Serialize)]
        struct ToggleResponse {
            updated: bool,
            active: bool,
        }

        control.rt(args, |client| async move {
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
    #[clap(flatten)]
    args: ControlArgs,
}

/// Stop the memory profiler.
#[derive(Parser)]
pub struct CmdProfilerStop {
    #[clap(flatten)]
    args: ControlArgs,
}

/// Dump the memory profiler data.
#[derive(Parser)]
pub struct CmdProfilerDump {
    #[clap(flatten)]
    args: ControlArgs,

    /// path to the output file
    #[clap()]
    output: PathBuf,
}

/// Get archive info from the node.
#[derive(Parser)]
pub struct CmdFindArchive {
    #[clap(flatten)]
    args: ControlArgs,

    /// masterchain block seqno.
    #[clap(long)]
    seqno: u32,
}

impl CmdFindArchive {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, move |client| async move {
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

/// Fetch the list of all stored archive ids.
#[derive(Parser)]
#[clap(disable_help_flag = true)]
pub struct CmdListArchives {
    #[clap(flatten)]
    args: ControlArgs,

    #[clap(short, long)]
    human_readable: bool,

    #[clap(long, action = clap::ArgAction::HelpLong)]
    help: Option<bool>,
}

impl CmdListArchives {
    pub fn run(self, args: BaseArgs) -> Result<()> {
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
                println!("ID: {id:<max_id_width$}, Size: {size:>max_size_width$}");
            }
        }

        self.args.rt(args, move |client| async move {
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
    #[clap(flatten)]
    args: ControlArgs,

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
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, move |client| async move {
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

/// Fetch the list of all stored block ids.
#[derive(Parser)]
#[clap(disable_help_flag = true)]
pub struct CmdListBlocks {
    #[clap(flatten)]
    args: ControlArgs,

    #[clap(short, long)]
    human_readable: bool,

    #[clap(long, action = clap::ArgAction::HelpLong)]
    help: Option<bool>,
}

impl CmdListBlocks {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, move |client| async move {
            let (json_start, json_pfx, json_end) = if std::io::stdin().is_terminal() {
                ("{\n  \"blocks\": [", "\n    ", "\n  ]\n}")
            } else {
                ("{\"blocks\":[", "", "]}")
            };

            if !self.human_readable {
                print!("{json_start}");
            }

            let mut is_first = true;
            let mut print_response = |blocks: Vec<BlockId>| -> std::io::Result<()> {
                let mut stdout = std::io::stdout().lock();

                for block_id in blocks {
                    let is_first = std::mem::take(&mut is_first);

                    if self.human_readable {
                        let pfx = if is_first { "Blocks:\n" } else { "" };
                        writeln!(stdout, "{pfx}{block_id}")?;
                    } else {
                        let sep = if is_first { "" } else { "," };
                        write!(stdout, "{sep}{json_pfx}\"{block_id}\"")?;
                    }
                }

                stdout.flush()
            };

            let mut continuation = None;
            loop {
                let res = client.list_blocks(continuation).await?;
                print_response(res.blocks)?;

                continuation = res.continuation;
                if continuation.is_none() {
                    break;
                }
            }

            if self.human_readable && is_first {
                println!("No blocks found");
            } else if !self.human_readable {
                println!("{json_end}");
            }
            Ok(())
        })
    }
}

/// Download a block from the node.
#[derive(Parser)]
pub struct CmdDumpBlock {
    #[clap(flatten)]
    args: ControlArgs,

    /// full block ID.
    #[clap(short, long, allow_hyphen_values(true))]
    block_id: BlockId,

    /// path to the output file
    #[clap()]
    output: PathBuf,
}

impl CmdDumpBlock {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, move |client| async move {
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
    #[clap(flatten)]
    args: ControlArgs,

    /// full block ID.
    #[clap(short, long, allow_hyphen_values(true))]
    block_id: BlockId,

    /// path to the output file
    #[clap()]
    output: PathBuf,
}

impl CmdDumpProof {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, move |client| async move {
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
    #[clap(flatten)]
    args: ControlArgs,

    /// full block ID.
    #[clap(short, long, allow_hyphen_values(true))]
    block_id: BlockId,

    /// path to the output file
    #[clap()]
    output: PathBuf,
}

impl CmdDumpQueueDiff {
    pub fn run(self, args: BaseArgs) -> Result<()> {
        self.args.rt(args, move |client| async move {
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

impl From<TriggerBy> for tycho_control::proto::TriggerGcRequest {
    fn from(value: TriggerBy) -> Self {
        match (value.seqno, value.distance) {
            (Some(seqno), None) => tycho_control::proto::TriggerGcRequest::Exact(seqno),
            (None, Some(distance)) => tycho_control::proto::TriggerGcRequest::Distance(distance),
            _ => unreachable!(),
        }
    }
}

#[derive(Serialize)]
struct Empty {}

#[derive(Clone, Args)]
struct ControlArgs {
    /// Path to the control socket. Default: `$TYCHO_HOME/control.sock`
    #[clap(long)]
    control_socket: Option<PathBuf>,
}

impl ControlArgs {
    fn rt<F, FT>(&self, args: BaseArgs, f: F) -> Result<()>
    where
        F: FnOnce(ControlClient) -> FT + Send + 'static,
        FT: Future<Output = Result<()>> + Send,
    {
        init_logger_simple("info,tarpc=error");

        let sock = args.control_socket_path(self.control_socket.as_ref());

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
}
