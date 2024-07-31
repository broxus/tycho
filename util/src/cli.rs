use std::io::IsTerminal;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::Subscriber;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::{fmt, Layer};

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
                    let directive = format!("{}={}", target, level)
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
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}
