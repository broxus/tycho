use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::Subscriber;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::{fmt, Layer};

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

pub struct LoggerTargets {
    directives: Vec<Directive>,
}

impl LoggerTargets {
    pub fn load_from<P: AsRef<Path>>(path: P) -> Result<Self> {
        tycho_util::serde_helpers::load_json_from_file(path)
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
pub struct LoggingConfig {
    pub path: PathBuf,
    #[serde(default)]
    pub human_readable: bool,
}

pub fn file_logging_layer<S>(
    config: Option<LoggingConfig>,
) -> Result<Box<dyn Layer<S> + Send + Sync + 'static>>
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    let Some(config) = config else {
        return Ok(NoopLayer.boxed());
    };
    let writer = tracing_appender::rolling::Builder::new()
        .rotation(Rotation::HOURLY)
        .filename_prefix("tycho.log")
        .max_log_files(24)
        .build(config.path)?;

    let layer = if config.human_readable {
        fmt::layer()
            .without_time()
            .with_ansi(false)
            .with_writer(writer)
            .boxed()
    } else {
        tracing_stackdriver::layer().with_writer(writer).boxed()
    };

    Ok(layer)
}

struct NoopLayer;

impl<S> Layer<S> for NoopLayer where S: Subscriber {}
