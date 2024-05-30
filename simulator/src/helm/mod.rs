use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod runner;
pub use runner::*;

use crate::config::ServiceConfig;

pub struct HelmConfig {
    values: Values,
    helm_path: PathBuf,
    template_path: PathBuf,
}

impl HelmConfig {
    pub fn new(service_config: &ServiceConfig) -> Self {
        let helm_path = service_config.helm_template_output();
        let template_path = service_config.helm_template_path();

        std::fs::create_dir_all(&helm_path).unwrap();
        Self {
            values: Values {
                image: Image {
                    repository: "localhost/tycho-network".to_string(),
                    pull_policy: "IfNotPresent".to_string(),
                    tag: "latest".to_string(),
                },
                fullname_override: "tycho".to_string(),
                config: Config {
                    rust_log: "info,tycho_network=trace".to_string(),
                    global_config: String::new(),
                },
                service: Service {
                    type_field: "ClusterIP".to_string(),
                    port: 30310,
                    cluster_ip: "None".to_string(),
                },
                volumes: vec![
                    Volume {
                        name: "keys".to_string(),
                        config_map: None,
                        secret: Some(Secret {
                            secret_name: "tycho-keys".to_string(),
                        }),
                    },
                    Volume {
                        name: "global-config".to_string(),
                        config_map: Some(ConfigMap {
                            name: "global-config".to_string(),
                        }),
                        secret: None,
                    },
                ],
                volume_mounts: vec![
                    VolumeMount {
                        name: "keys".to_string(),
                        mount_path: "/keys".to_string(),
                        sub_path: None,
                        read_only: true,
                    },
                    VolumeMount {
                        name: "global-config".to_string(),
                        mount_path: "/app/global-config.json".to_string(),
                        sub_path: Some("global-config.json".to_string()),
                        read_only: true,
                    },
                ],
                service_monitor: ServiceMonitor {
                    enabled: true,
                    port: 9090,
                },
                ..Default::default()
            },
            helm_path,
            template_path,
        }
    }

    pub fn add_node(&mut self, secret_key: &str) {
        self.values.replica_count += 1;
        self.values.keys.push(secret_key.to_owned());
    }

    pub fn finalize(&mut self, global_config: String) -> Result<()> {
        self.values.config.global_config = global_config;

        // copy the helm chart to the scratch directory
        let dst = self.helm_path.join("tycho");
        std::fs::create_dir_all(&dst).unwrap();
        fs_extra::dir::copy(
            &self.template_path,
            &dst,
            &fs_extra::dir::CopyOptions::new()
                .overwrite(true)
                .content_only(true),
        )?;

        // write the values file
        let values_path = dst.join("values.yaml");
        // yaml is a superset of json, so we can use serde_json to write yaml
        let values_data = serde_json::to_string_pretty(&self.values)?;
        std::fs::write(values_path, values_data)?;

        Ok(())
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Values {
    pub replica_count: u64,
    pub image: Image,
    pub image_pull_secrets: Vec<Value>,
    pub name_override: String,
    pub fullname_override: String,
    pub config: Config,
    pub keys: Vec<String>,
    pub service_account: ServiceAccount,
    pub pod_annotations: PodAnnotations,
    pub pod_labels: PodLabels,
    pub pod_security_context: PodSecurityContext,
    pub security_context: SecurityContext,
    pub service: Service,
    pub resources: Resources,
    pub volumes: Vec<Volume>,
    pub volume_mounts: Vec<VolumeMount>,
    pub node_selector: NodeSelector,
    pub tolerations: Vec<Value>,
    pub affinity: Affinity,
    pub service_monitor: ServiceMonitor,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Image {
    pub repository: String,
    pub pull_policy: String,
    pub tag: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(rename = "rustLog")]
    pub rust_log: String,
    pub global_config: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccount {
    pub create: bool,
    pub automount: bool,
    pub annotations: Annotations,
    pub name: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Annotations {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodAnnotations {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodLabels {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodSecurityContext {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityContext {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    #[serde(rename = "type")]
    pub type_field: String,
    pub port: i64,
    #[serde(rename = "clusterIP")]
    pub cluster_ip: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resources {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub name: String,
    pub config_map: Option<ConfigMap>,
    pub secret: Option<Secret>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMap {
    pub name: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Secret {
    pub secret_name: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
    pub sub_path: Option<String>,
    pub read_only: bool,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelector {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Affinity {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceMonitor {
    enabled: bool,
    port: u16,
}
