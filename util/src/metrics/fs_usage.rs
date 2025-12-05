use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tycho_util::metrics::spawn_metrics_loop;
use tycho_util::sync::CancellationFlag;
use walkdir::WalkDir;

const BYTES_METRIC: &str = "tycho_fs_used_bytes";
const FILES_METRIC: &str = "tycho_fs_used_files";
const BLOCKS_METRIC: &str = "tycho_fs_used_blocks";
const TOTAL_LABEL: &str = "__total__";

#[derive(Debug, Clone)]
pub struct Stats {
    pub entries: Vec<StatsEntry>,
}

#[derive(Debug, Clone)]
pub struct StatsEntry {
    pub path: PathBuf,
    pub usage: Usage,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Usage {
    pub bytes: u64,
    pub files: u64,
    pub blocks: u64,
}

impl Stats {
    pub fn total(&self) -> Usage {
        total_counts(&self.entries)
    }
}

pub struct FsUsageBuilder {
    paths: Vec<PathBuf>,
}

impl FsUsageBuilder {
    pub fn new() -> Self {
        Self { paths: Vec::new() }
    }

    pub fn add_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.paths.push(path.into());
        self
    }

    pub fn build(self) -> FsUsageMonitor {
        let entries = self.paths.into_iter().map(Entry::new).collect::<Vec<_>>();

        FsUsageMonitor {
            state: Arc::new(FsUsageState {
                entries: Mutex::new(entries),
            }),
            stop: CancellationFlag::new(),
            export_handle: None,
        }
    }
}

impl Default for FsUsageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FsUsageMonitor {
    state: Arc<FsUsageState>,
    stop: CancellationFlag,
    export_handle: Option<tokio::task::AbortHandle>,
}

impl FsUsageMonitor {
    pub fn add_path<P: Into<PathBuf>>(&self, path: P) -> bool {
        let path = path.into();
        let mut entries = self.state.entries.lock().unwrap();

        if entries.iter().any(|e| e.path == path) {
            return false;
        }

        entries.push(Entry::new(path));
        true
    }

    pub fn iter_sizes(&self) -> impl Iterator<Item = StatsEntry> {
        self.snapshot().entries.into_iter()
    }

    pub fn walk(&self) -> Stats {
        walk_state(self.state.as_ref(), &self.stop)
    }

    pub fn snapshot(&self) -> Stats {
        let entries = self.state.entries.lock().unwrap();
        Stats {
            entries: entries
                .iter()
                .map(|e| StatsEntry {
                    path: e.path.clone(),
                    usage: e.usage,
                })
                .collect(),
        }
    }

    /// Starts the metrics loop that periodically collects and exports filesystem usage metrics.
    /// Can only be called once; subsequent calls will return an error.
    pub fn spawn_metrics_loop(&mut self, interval: Duration) -> Result<(), MetricsLoopStartError> {
        if self.stop.check() {
            return Err(MetricsLoopStartError::ShuttingDown);
        }

        if self.export_handle.is_some() {
            return Err(MetricsLoopStartError::AlreadyRunning);
        }

        let stop = self.stop.clone();

        let handle = spawn_metrics_loop(&self.state, interval, move |state| {
            let stop = stop.clone();
            async move {
                let stats = tokio::task::spawn_blocking(move || walk_state(state.as_ref(), &stop))
                    .await
                    .expect("spawn blocking failed");

                export_metrics(&stats);
            }
        });

        self.export_handle = Some(handle);

        Ok(())
    }

    fn shutdown(&mut self) {
        self.stop.cancel();

        if let Some(handle) = self.export_handle.take() {
            handle.abort();
        }
    }
}

impl Drop for FsUsageMonitor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug)]
struct FsUsageState {
    entries: Mutex<Vec<Entry>>,
}

#[derive(Debug, Clone)]
struct Entry {
    path: PathBuf,
    usage: Usage,
}

impl Entry {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            usage: Usage::default(),
        }
    }
}

fn walk_state(state: &FsUsageState, stop: &CancellationFlag) -> Stats {
    let paths: Vec<_> = state.entries.lock().unwrap().clone();

    if paths.is_empty() {
        return Stats { entries: vec![] };
    }

    let mut results = Vec::with_capacity(paths.len());
    for e in paths {
        if stop.check() {
            break;
        }

        let usage = collect_path_usage(&e.path, stop);
        results.push((e.path, usage));
    }

    let mut entries = state.entries.lock().unwrap();

    let stats_out = entries
        .iter_mut()
        .zip(results)
        .map(|(entry, (path, usage))| {
            entry.usage = usage;
            StatsEntry { path, usage }
        })
        .collect();

    Stats { entries: stats_out }
}

fn collect_path_usage(path: &Path, stop: &CancellationFlag) -> Usage {
    let mut usage = Usage::default();

    let walker = WalkDir::new(path)
        .follow_links(false)
        .follow_root_links(true);

    for item in walker {
        if stop.check() {
            break;
        }

        let entry = match item {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("fs usage: walk e: {e:?}");
                continue;
            }
        };

        if entry.file_type().is_symlink() {
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(
                    path = %entry.path().display(),
                    "fs usage: failed to read metadata: {e:?}",
                );
                continue;
            }
        };

        if metadata.is_dir() {
            usage.files = usage.files.saturating_add(1);
            usage.blocks = usage.blocks.saturating_add(blocks_from_metadata(&metadata));
        } else if metadata.is_file() {
            usage.files = usage.files.saturating_add(1);
            usage.bytes = usage.bytes.saturating_add(metadata.len());
            usage.blocks = usage.blocks.saturating_add(blocks_from_metadata(&metadata));
        }
    }

    usage
}

fn total_counts(entries: &[StatsEntry]) -> Usage {
    let mut sorted = entries.iter().collect::<Vec<_>>();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));

    let mut total_bytes: u64 = 0;
    let mut total_files: u64 = 0;
    let mut total_blocks: u64 = 0;

    let mut last_parent: Option<&Path> = None;

    for entry in sorted {
        if let Some(parent) = last_parent
            && entry.path.starts_with(parent)
        {
            continue;
        }

        total_bytes = total_bytes.saturating_add(entry.usage.bytes);
        total_files = total_files.saturating_add(entry.usage.files);
        total_blocks = total_blocks.saturating_add(entry.usage.blocks);

        last_parent = Some(entry.path.as_path());
    }

    Usage {
        bytes: total_bytes,
        files: total_files,
        blocks: total_blocks,
    }
}

fn export_metrics(stats: &Stats) {
    for entry in &stats.entries {
        let path = entry.path.to_string_lossy().into_owned();

        for (metric, value) in [
            (BYTES_METRIC, entry.usage.bytes),
            (FILES_METRIC, entry.usage.files),
            (BLOCKS_METRIC, entry.usage.blocks),
        ] {
            metrics::gauge!(metric, "path" => path.clone()).set(value as f64);
        }
    }

    let Usage {
        bytes,
        files,
        blocks,
    } = stats.total();
    for (metric, value) in [
        (BYTES_METRIC, bytes),
        (FILES_METRIC, files),
        (BLOCKS_METRIC, blocks),
    ] {
        metrics::gauge!(metric, "path" => TOTAL_LABEL).set(value as f64);
    }
}

fn blocks_from_metadata(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks()
    }
    #[cfg(not(unix))]
    {
        let len = metadata.len();
        if len == 0 {
            0
        } else {
            len.saturating_add(511) / 512
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MetricsLoopStartError {
    #[error("metrics loop is already running")]
    AlreadyRunning,
    #[error("fs monitor is shutting down")]
    ShuttingDown,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn walk_counts_files_and_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let file_a = root.join("a.txt");
        fs::write(&file_a, b"abcd").unwrap();

        let nested_dir = root.join("nested");
        fs::create_dir(&nested_dir).unwrap();
        fs::write(nested_dir.join("b.bin"), b"123456").unwrap();

        let monitor = FsUsageBuilder::new().add_path(root).build();

        let stats = monitor.walk();
        let entry = stats.entries.first().unwrap();

        assert_eq!(entry.usage.bytes, 10);
        assert_eq!(entry.usage.files, 4);
        assert!(entry.usage.blocks.saturating_mul(512) >= entry.usage.bytes);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn walk_skips_symlinks_and_missing_paths() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();
        let file_a = root.join("a");
        fs::write(&file_a, b"1").unwrap();

        let link = root.join("link");
        std::os::unix::fs::symlink(&file_a, &link).unwrap();

        let missing = root.join("missing");

        let monitor = FsUsageBuilder::new()
            .add_path(&link)
            .add_path(&missing)
            .add_path(&file_a)
            .build();

        let stats = monitor.walk();
        let totals = stats
            .entries
            .iter()
            .map(|entry| {
                (
                    entry
                        .path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                    (entry.usage.bytes, entry.usage.files, entry.usage.blocks),
                )
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(totals.get("link"), Some(&(0, 0, 0)));
        assert_eq!(totals.get("missing"), Some(&(0, 0, 0)));
        let &(bytes, files, blocks) = totals.get("a").unwrap();
        assert_eq!(bytes, 1);
        assert_eq!(files, 1);
        assert!(blocks >= 1);
    }

    #[test]
    fn total_counts_skips_children() {
        let entries = vec![
            StatsEntry {
                path: PathBuf::from("/var/log"),
                usage: Usage {
                    bytes: 3,
                    files: 1,
                    blocks: 2,
                },
            },
            StatsEntry {
                path: PathBuf::from("/var"),
                usage: Usage {
                    bytes: 10,
                    files: 4,
                    blocks: 7,
                },
            },
            StatsEntry {
                path: PathBuf::from("/opt"),
                usage: Usage {
                    bytes: 5,
                    files: 2,
                    blocks: 3,
                },
            },
        ];

        let Usage {
            bytes,
            files,
            blocks,
        } = total_counts(&entries);

        assert_eq!(bytes, 15);
        assert_eq!(files, 6);
        assert_eq!(blocks, 10);
    }
}
