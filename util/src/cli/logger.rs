use std::borrow::Cow;
use std::io::IsTerminal;
use std::num::NonZeroUsize;
use std::ops::Not;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::Subscriber;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

pub struct LoggerTargets {
    directives: Vec<Directive>,
}

impl LoggerTargets {
    pub fn load_from<P: AsRef<Path>>(path: P) -> Result<Self> {
        crate::serde_helpers::load_json_from_file(path)
    }

    pub fn build_subscriber(&self) -> EnvFilter {
        let mut builder = EnvFilter::default();
        for item in &self.directives {
            builder = builder.add_directive(item.clone());
        }
        builder
    }
}

impl<'de> Deserialize<'de> for LoggerTargets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LoggerVisitor;

        impl<'de> Visitor<'de> for LoggerVisitor {
            type Value = LoggerTargets;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a list of targets")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut directives = Vec::new();

                while let Some((target, level)) = map.next_entry::<String, String>()? {
                    let directive = format!("{target}={level}")
                        .parse::<Directive>()
                        .map_err(serde::de::Error::custom)?;

                    directives.push(directive);
                }

                Ok(LoggerTargets { directives })
            }
        }

        deserializer.deserialize_map(LoggerVisitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggerConfig {
    pub outputs: Vec<LoggerOutput>,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            outputs: vec![LoggerOutput::Stderr(STDERR)],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Auto,
    Human,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LoggerOutput {
    Stderr(LoggerStderrOutput),
    File(LoggerFileOutput),
}

impl LoggerOutput {
    pub fn as_layer<S>(&self) -> Result<Box<dyn Layer<S> + Send + Sync + 'static>>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        match self {
            Self::Stderr(stderr) => Ok(stderr.as_layer()),
            Self::File(file) => file.as_layer::<S>(),
        }
    }
}

pub const STDERR: LoggerStderrOutput = LoggerStderrOutput {
    format: LogFormat::Auto,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct LoggerStderrOutput {
    #[serde(default)]
    pub format: LogFormat,
}

impl LoggerStderrOutput {
    pub fn as_layer<S>(&self) -> Box<dyn Layer<S> + Send + Sync + 'static>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        match self.format {
            LogFormat::Human | LogFormat::Auto => human_layer(),
            LogFormat::Json => tracing_stackdriver::layer()
                .with_writer(std::io::stderr)
                .boxed(),
        }
    }
}

fn human_layer<S>() -> Box<dyn Layer<S> + Send + Sync + 'static>
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    if is_systemd_child() {
        fmt::layer().without_time().with_ansi(false).boxed()
    } else if !std::io::stdout().is_terminal() {
        fmt::layer().with_ansi(false).boxed()
    } else {
        fmt::layer().boxed()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggerFileOutput {
    pub dir: PathBuf,
    #[serde(default, skip_serializing_if = "<&bool>::not")]
    pub human_readable: bool,
    #[serde(default)]
    pub format: Option<LogFormat>,
    #[serde(default = "log_file_prefix")]
    pub file_prefix: String,
    #[serde(default = "max_log_files")]
    pub max_files: NonZeroUsize,
}

impl LoggerFileOutput {
    fn resolved_format(&self) -> LogFormat {
        let format = self.format.unwrap_or_default();

        match format {
            LogFormat::Human => LogFormat::Human,
            LogFormat::Auto if self.human_readable => LogFormat::Human,
            LogFormat::Json | LogFormat::Auto => LogFormat::Json,
        }
    }

    pub fn as_layer<S>(&self) -> Result<Box<dyn Layer<S> + Send + Sync + 'static>>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        let writer = tracing_appender::rolling::Builder::new()
            .rotation(Rotation::HOURLY)
            .filename_prefix(&self.file_prefix)
            .max_log_files(self.max_files.get())
            .build(&self.dir)?;

        Ok(match self.resolved_format() {
            LogFormat::Human => fmt::layer()
                .without_time()
                .with_ansi(false)
                .with_writer(writer)
                .boxed(),
            LogFormat::Json | LogFormat::Auto => {
                tracing_stackdriver::layer().with_writer(writer).boxed()
            }
        })
    }
}

fn log_file_prefix() -> String {
    "tycho.log".to_owned()
}

fn max_log_files() -> NonZeroUsize {
    NonZeroUsize::new(25).unwrap()
}

pub fn is_systemd_child() -> bool {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::getppid() == 1
            || std::env::var("SYSTEMD_EXEC_PID")
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .is_some()
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

pub fn init_logger_simple(default_filter: &str) {
    use tracing_subscriber::layer::SubscriberExt;

    let mut filter = Cow::Borrowed(default_filter);
    if let Ok(env) = std::env::var(EnvFilter::DEFAULT_ENV) {
        filter = Cow::Owned(env);
    }

    tracing_subscriber::registry()
        .with(EnvFilter::try_new(filter).expect("tracing directives"))
        .with(STDERR.as_layer())
        .init();
}

/// Initializes logger once.
///
/// All invocations after the successfull initialization will fail.
///
/// If `logger_targets` file path is provided, spawns a new thread which
/// will poll the file metadata and reload logger when file changes.
pub fn init_logger(config: &LoggerConfig, logger_targets: Option<PathBuf>) -> Result<()> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::reload;

    let try_make_filter = {
        let logger_targets = logger_targets.clone();
        move || {
            Ok::<_, anyhow::Error>(match &logger_targets {
                None => EnvFilter::builder()
                    .with_default_directive(tracing::Level::INFO.into())
                    .from_env_lossy(),
                Some(path) => LoggerTargets::load_from(path)
                    .context("failed to load logger config")?
                    .build_subscriber(),
            })
        }
    };

    static ONCE: Once = Once::new();

    let mut result = None;
    ONCE.call_once(|| {
        result = Some((|| {
            let (layer, handle) = reload::Layer::new(try_make_filter()?);

            let subscriber = tracing_subscriber::registry().with(layer).with(
                config
                    .outputs
                    .iter()
                    .map(|o| o.as_layer())
                    .collect::<Result<Vec<_>>>()?,
            );
            tracing::subscriber::set_global_default(subscriber)?;

            Ok::<_, anyhow::Error>(handle)
        })());
    });

    let handle = match result {
        Some(res) => res?,
        None => anyhow::bail!("logger was already initialized"),
    };

    if let Some(logger_config) = logger_targets {
        const INTERVAL: Duration = Duration::from_secs(10);

        // NOTE: We are using a simple thread there instead of tokio
        // so that there is no surprise when using it during the app start.
        std::thread::Builder::new()
            .name("watch_logger_config".to_owned())
            .spawn(move || {
                tracing::info!(
                    logger_config = %logger_config.display(),
                    "started watching for changes in logger config"
                );

                let get_metadata = move || {
                    std::fs::metadata(&logger_config)
                        .ok()
                        .and_then(|m| m.modified().ok())
                };

                let mut last_modified = get_metadata();

                loop {
                    std::thread::sleep(INTERVAL);

                    let modified = get_metadata();
                    if last_modified == modified {
                        continue;
                    }
                    last_modified = modified;

                    match try_make_filter() {
                        Ok(filter) => {
                            if handle.reload(filter).is_err() {
                                break;
                            }
                            tracing::info!("reloaded logger config");
                        }
                        Err(e) => tracing::error!(%e, "failed to reload logger config"),
                    }
                }

                tracing::info!("stopped watching for changes in logger config");
            })?;
    }

    Ok(())
}

pub fn set_abort_with_tracing() {
    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;

        let backtrace = std::backtrace::Backtrace::force_capture();
        tracing::error!("panic: {info}\n{backtrace}");

        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();

        #[allow(clippy::exit)]
        std::process::exit(1);
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_stderr_default_format() {
        let output: LoggerOutput = serde_json::from_str(r#"{ "type": "Stderr" }"#).unwrap();
        match output {
            LoggerOutput::Stderr(stderr) => assert_eq!(stderr.format, LogFormat::Auto),
            LoggerOutput::File(_) => panic!("expected stderr output"),
        }
    }

    #[test]
    fn resolves_file_format_from_legacy() {
        let output: LoggerOutput = serde_json::from_str(
            r#"{
                "type": "File",
                "dir": "/var/log/tycho",
                "human_readable": true
            }"#,
        )
        .unwrap();
        match output {
            LoggerOutput::File(file) => assert_eq!(file.resolved_format(), LogFormat::Human),
            LoggerOutput::Stderr(_) => panic!("expected file"),
        }
    }

    #[test]
    fn format_has_more_prio_than_human_readable() {
        let output: LoggerOutput = serde_json::from_str(
            r#"{
                "type": "File",
                "dir": "/var/log/tycho",
                "human_readable": true,
                "format": "json"
            }"#,
        )
        .unwrap();
        match output {
            LoggerOutput::File(file) => assert_eq!(file.resolved_format(), LogFormat::Json),
            LoggerOutput::Stderr(_) => panic!("expected file"),
        }
    }

    #[test]
    fn resolves_auto_to_boolean_default() {
        let output: LoggerOutput = serde_json::from_str(
            r#"{
                "type": "File",
                "dir": "/var/log/tycho",
                "format": "auto"
            }"#,
        )
        .unwrap();
        match output {
            LoggerOutput::File(file) => assert_eq!(file.resolved_format(), LogFormat::Json),
            LoggerOutput::Stderr(_) => panic!("expected file"),
        }
    }
}
