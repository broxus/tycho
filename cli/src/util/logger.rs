use std::path::Path;

use anyhow::Result;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use tracing_subscriber::filter::Directive;

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

pub struct LoggerConfig {
    directives: Vec<Directive>,
}

impl LoggerConfig {
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

impl<'de> Deserialize<'de> for LoggerConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LoggerVisitor;

        impl<'de> Visitor<'de> for LoggerVisitor {
            type Value = LoggerConfig;

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

                Ok(LoggerConfig { directives })
            }
        }

        deserializer.deserialize_map(LoggerVisitor)
    }
}
