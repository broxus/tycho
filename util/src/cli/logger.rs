use std::borrow::Cow;
use std::io::IsTerminal;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::Subscriber;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};

pub struct LoggerTargets {
    directives: Vec<Directive>,
}

impl LoggerTargets {
    pub fn load_from<P: AsRef<Path>>(path: P) -> Result<Self> {
        crate::serde_helpers::load_json_from_file(path)
    }

    pub fn build_subscriber(&self) -> tracing_subscriber::filter::EnvFilter {
        let mut builder = tracing_subscriber::filter::EnvFilter::default();
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
            outputs: vec![LoggerOutput::Stderr(LoggerStderrOutput)],
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggerStderrOutput;

impl LoggerStderrOutput {
    #[allow(clippy::unused_self)]
    pub fn as_layer<S>(&self) -> Box<dyn Layer<S> + Send + Sync + 'static>
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggerFileOutput {
    pub dir: PathBuf,
    #[serde(default)]
    pub human_readable: bool,
    #[serde(default = "log_file_prefix")]
    pub file_prefix: String,
    #[serde(default = "max_log_files")]
    pub max_files: NonZeroUsize,
}

impl LoggerFileOutput {
    pub fn as_layer<S>(&self) -> Result<Box<dyn Layer<S> + Send + Sync + 'static>>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        let writer = tracing_appender::rolling::Builder::new()
            .rotation(Rotation::HOURLY)
            .filename_prefix(&self.file_prefix)
            .max_log_files(self.max_files.get())
            .build(&self.dir)?;

        Ok(if self.human_readable {
            fmt::layer()
                .without_time()
                .with_ansi(false)
                .with_writer(writer)
                .boxed()
        } else {
            tracing_stackdriver::layer().with_writer(writer).boxed()
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
        .with(LoggerStderrOutput.as_layer())
        .init();
}

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

    let (layer, handle) = reload::Layer::new(try_make_filter()?);

    let subscriber = tracing_subscriber::registry().with(layer).with(
        config
            .outputs
            .iter()
            .map(|o| o.as_layer())
            .collect::<anyhow::Result<Vec<_>>>()?,
    );
    tracing::subscriber::set_global_default(subscriber).unwrap();

    if let Some(logger_config) = logger_targets {
        tokio::spawn(async move {
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

            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

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
        });
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
