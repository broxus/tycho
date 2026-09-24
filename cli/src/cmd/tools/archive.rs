use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use tl_proto::TlWrite;
use tycho_block_util::archive::{
    ARCHIVE_PREFIX, ArchiveEntryHeader, ArchiveEntryType, ArchiveReader,
};
use tycho_types::models::BlockId;
use tycho_util::compression::ZstdDecompressStream;

use crate::util::print_json;

/// Manipulate archive files.
#[derive(Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl Cmd {
    pub fn run(self) -> Result<()> {
        match self.cmd {
            SubCmd::List(cmd) => cmd.run(),
            SubCmd::Get(cmd) => cmd.run(),
            SubCmd::Set(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    /// List archive entries.
    List(CmdList),
    /// Extract an archive entry.
    Get(CmdGet),
    /// Replace or append an archive entry.
    Set(CmdSet),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum EntryTypeArg {
    Block,
    Proof,
    QueueDiff,
}

impl From<EntryTypeArg> for ArchiveEntryType {
    fn from(value: EntryTypeArg) -> Self {
        match value {
            EntryTypeArg::Block => Self::Block,
            EntryTypeArg::Proof => Self::Proof,
            EntryTypeArg::QueueDiff => Self::QueueDiff,
        }
    }
}

#[derive(Parser)]
struct CmdList {
    /// Archive file path.
    path: PathBuf,
}

impl CmdList {
    fn run(self) -> Result<()> {
        let data = std::fs::read(&self.path)
            .with_context(|| format!("failed to read archive {}", self.path.display()))?;
        let mut decoder =
            ZstdDecompressStream::new(1024 * 1024).context("Failed to construct zstd decoder")?;

        let mut decompressed = Vec::new();
        decoder
            .write(&data, &mut decompressed)
            .context("Failed to decompress archive data")?;

        let reader = ArchiveReader::new(&decompressed).context("invalid archive")?;

        let entries = reader
            .map(|item| {
                let entry = item.context("failed to read archive entry")?;
                Ok(ListEntry {
                    block_id: entry.block_id.to_string(),
                    ty: format_entry_type(entry.ty),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        print_json(entries)
    }
}

#[derive(Serialize)]
struct ListEntry {
    block_id: String,
    ty: &'static str,
}

#[derive(Parser)]
struct CmdGet {
    /// Archive file path.
    path: PathBuf,
    /// Entry block id.
    #[clap(long, allow_hyphen_values(true))]
    block_id: BlockId,
    /// Entry type.
    #[clap(long, value_enum)]
    ty: EntryTypeArg,
    /// Output file path. Prints to stdout if omitted.
    #[clap(short, long)]
    output: Option<PathBuf>,
}

impl CmdGet {
    fn run(self) -> Result<()> {
        let data = std::fs::read(&self.path)
            .with_context(|| format!("failed to read archive {}", self.path.display()))?;

        let mut decoder =
            ZstdDecompressStream::new(1024 * 1024).context("Failed to construct zstd decoder")?;

        let mut decompressed = Vec::new();
        decoder
            .write(&data, &mut decompressed)
            .context("Failed to decompress archive data")?;

        let mut reader = ArchiveReader::new(&decompressed).context("invalid archive")?;
        let ty = ArchiveEntryType::from(self.ty);

        let entry = reader
            .find_map(|item| match item {
                Ok(entry) if entry.block_id == self.block_id && entry.ty == ty => {
                    Some(Ok::<Vec<u8>, anyhow::Error>(entry.data.to_vec()))
                }
                Ok(_) => None,
                Err(e) => Some(Err(e.into())),
            })
            .transpose()?
            .context("archive entry not found")?;

        match self.output {
            Some(path) => std::fs::write(&path, entry)
                .with_context(|| format!("failed to write output {}", path.display())),
            None => {
                println!("{}", hex::encode(entry));
                Ok(())
            }
        }
    }
}

#[derive(Parser)]
struct CmdSet {
    /// Archive file path.
    path: PathBuf,
    /// Entry block id.
    #[clap(long, allow_hyphen_values(true))]
    block_id: BlockId,
    /// Entry type.
    #[clap(long, value_enum)]
    ty: EntryTypeArg,
    /// Input file path.
    input: PathBuf,
}

impl CmdSet {
    fn run(self) -> Result<()> {
        let data = std::fs::read(&self.path)
            .with_context(|| format!("failed to read archive {}", self.path.display()))?;

        let mut decoder =
            ZstdDecompressStream::new(1024 * 1024).context("Failed to construct zstd decoder")?;

        let mut decompressed = Vec::new();
        decoder
            .write(&data, &mut decompressed)
            .context("Failed to decompress archive data")?;

        let reader = ArchiveReader::new(&decompressed).context("invalid archive")?;

        let replacement = std::fs::read(&self.input)
            .with_context(|| format!("failed to read input {}", self.input.display()))?;

        let mut output = Vec::with_capacity(data.len() + replacement.len());
        output.extend_from_slice(&ARCHIVE_PREFIX);

        let mut replaced = false;
        let ty = ArchiveEntryType::from(self.ty);

        for item in reader {
            let entry = item.context("failed to read archive entry")?;
            let entry_data = if entry.block_id == self.block_id && entry.ty == ty {
                replaced = true;
                replacement.as_slice()
            } else {
                entry.data
            };

            write_entry(&mut output, entry.block_id, entry.ty, entry_data);
        }

        if !replaced {
            write_entry(&mut output, self.block_id, ty, &replacement);
        }

        std::fs::write(&self.path, output)
            .with_context(|| format!("failed to write archive {}", self.path.display()))
    }
}

fn write_entry(dst: &mut Vec<u8>, block_id: BlockId, ty: ArchiveEntryType, data: &[u8]) {
    let header = ArchiveEntryHeader {
        block_id,
        ty,
        data_len: data.len() as u32,
    };
    header.write_to(dst);
    dst.extend_from_slice(data);
}

fn format_entry_type(ty: ArchiveEntryType) -> &'static str {
    match ty {
        ArchiveEntryType::Block => "block",
        ArchiveEntryType::Proof => "proof",
        ArchiveEntryType::QueueDiff => "queue_diff",
    }
}
