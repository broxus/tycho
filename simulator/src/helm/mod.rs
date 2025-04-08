use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod runner;
pub use runner::*;

use crate::config::{PodConfig, ProjectRoot};

pub struct HelmConfig {
    values: Values,
}

impl HelmConfig {
    pub fn new(
        pod_config: &PodConfig,
        shared_conf: SharedConfigs,
        node_secrets: Vec<String>,
    ) -> Self {
        let fullname_override = "tycho".to_string();

        let keys = "keys".to_string();
        let shared_configs = "shared-configs".to_string();
        let data_volume = "data-volume".to_string();

        // 1250 is a max dag length by default mempool config; 6/5 is +20% provisioning
        let mempool_data_gb = node_secrets.len() * 1250 * 6 / 5000;

        Self {
            values: Values {
                image: Image {
                    repository: format!("localhost/{}", pod_config.image_name),
                    pull_policy: "IfNotPresent".to_string(),
                    tag: "latest".to_string(),
                },
                fullname_override: fullname_override.clone(),
                replica_count: node_secrets.len() as u64,
                keys: node_secrets,
                shared_configs: shared_conf,
                service: Service {
                    type_field: "ClusterIP".to_string(),
                    port: pod_config.node_port,
                    cluster_ip: "None".to_string(),
                },
                persistence: Some(Persistence {
                    storage_class: "local-storage".to_string(),
                    is_local: true,
                    local_path: "/var/tmp".to_string(),
                    access_mode: PersistenceAccessMode::ReadWriteOnce,
                    size_per_pod: mempool_data_gb,
                    size_type: PersistenceSizeType::Gi,
                }),
                volumes: vec![
                    Volume {
                        name: keys.clone(),
                        volume_type: VolumeType::Secret(Secret {
                            secret_name: format!("{fullname_override}-keys"),
                        }),
                    },
                    Volume {
                        name: shared_configs.clone(),
                        volume_type: VolumeType::ConfigMap(ConfigMap {
                            name: format!("{fullname_override}-shared-configs"),
                        }),
                    },
                    Volume {
                        name: data_volume.clone(),
                        volume_type: VolumeType::PersistentVolumeClaim(PersistentVolumeClaim {
                            claim_name: format!("{fullname_override}-data-local-claim"),
                        }),
                        // volume_type: VolumeType::EmptyDir(EmptyDir {
                        //     size_limit: format!("{mempool_data_gb}{:?}", PersistenceSizeType::Gi),
                        //     medium: None,
                        // }),
                    },
                ],
                volume_mounts: vec![
                    VolumeMount {
                        name: keys.clone(),
                        mount_path: "/keys".to_string(),
                        sub_path_type: None,
                        read_only: true,
                    },
                    VolumeMount {
                        name: shared_configs.clone(),
                        mount_path: "/app/global-config.json".to_string(),
                        sub_path_type: Some(SubPathType::SubPath("global-config.json".to_string())),
                        read_only: true,
                    },
                    VolumeMount {
                        name: shared_configs.clone(),
                        mount_path: "/app/config.json".to_string(),
                        sub_path_type: Some(SubPathType::SubPath("config.json".to_string())),
                        read_only: true,
                    },
                    VolumeMount {
                        name: shared_configs.clone(),
                        mount_path: "/app/zerostate.boc".to_string(),
                        sub_path_type: Some(SubPathType::SubPath("zerostate.boc".to_string())),
                        read_only: true,
                    },
                    VolumeMount {
                        name: shared_configs.clone(),
                        mount_path: "/app/logger.json".to_string(),
                        sub_path_type: Some(SubPathType::SubPath("logger.json".to_string())),
                        read_only: true,
                    },
                    VolumeMount {
                        name: data_volume.clone(),
                        mount_path: pod_config.db_path.clone(),
                        sub_path_type: Some(SubPathType::SubPathExpr(
                            "$(POD_HOST_NAME)".to_string(),
                        )),
                        read_only: false,
                    },
                ],
                service_monitor: ServiceMonitor {
                    enabled: true,
                    port: pod_config.metrics_port,
                    namespace: "monitoring".to_string(), // must be where prometheus is installed
                    path: "/".to_string(),               // empty defaults to /metrics, will also do
                },
                ..Default::default()
            },
        }
    }

    pub fn write(self, project_root: &ProjectRoot) -> Result<()> {
        let dst = &project_root.scratch.helm.tycho.dir;

        std::fs::create_dir_all(dst)?;

        // copy the helm chart to the scratch directory
        fs_extra::dir::copy(
            &project_root.simulator.helm.tycho.dir,
            dst,
            &fs_extra::dir::CopyOptions::new()
                .overwrite(true)
                .content_only(true),
        )?;

        // write the values file
        // yaml is a superset of json, so we can use serde_json to write yaml
        let values_data = serde_json::to_string_pretty(&self.values)?;
        std::fs::write(&project_root.scratch.helm.tycho.values, values_data)?;

        Ok(())
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Values {
    image: Image,
    image_pull_secrets: Vec<Value>,
    name_override: String,
    fullname_override: String,
    replica_count: u64,
    keys: Vec<String>,
    shared_configs: SharedConfigs,
    service_account: ServiceAccount,
    pod_annotations: PodAnnotations,
    pod_labels: PodLabels,
    pod_security_context: PodSecurityContext,
    security_context: SecurityContext,
    service: Service,
    resources: Resources,
    persistence: Option<Persistence>,
    volumes: Vec<Volume>,
    volume_mounts: Vec<VolumeMount>,
    node_selector: NodeSelector,
    tolerations: Vec<Value>,
    affinity: Affinity,
    service_monitor: ServiceMonitor,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Image {
    repository: String,
    pull_policy: String,
    tag: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharedConfigs {
    /// Used only by network example
    #[serde(rename = "rustLog")]
    pub rust_log: String,
    pub global_config: String,
    /// tycho node config is conventionally named just `config`
    pub config: String,
    pub zerostate: String,
    pub logger: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceAccount {
    create: bool,
    automount: bool,
    annotations: Annotations,
    name: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Annotations {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PodAnnotations {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PodLabels {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PodSecurityContext {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SecurityContext {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Service {
    #[serde(rename = "type")]
    type_field: String,
    port: u16,
    #[serde(rename = "clusterIP")]
    cluster_ip: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Resources {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Volume {
    name: String,
    #[serde(flatten)]
    volume_type: VolumeType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum VolumeType {
    PersistentVolumeClaim(PersistentVolumeClaim),
    ConfigMap(ConfigMap),
    Secret(Secret),
    EmptyDir(EmptyDir),
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigMap {
    name: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Secret {
    secret_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmptyDir {
    size_limit: String,
    /// leave empty to use file fs, not tmpfs
    medium: Option<EmptyDirMedium>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum EmptyDirMedium {
    Memory,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistentVolumeClaim {
    claim_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VolumeMount {
    name: String,
    mount_path: String,
    #[serde(flatten)]
    sub_path_type: Option<SubPathType>,
    read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum SubPathType {
    SubPath(String),
    SubPathExpr(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Persistence {
    storage_class: String,
    is_local: bool,
    local_path: String,
    access_mode: PersistenceAccessMode,
    size_per_pod: usize,
    size_type: PersistenceSizeType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum PersistenceAccessMode {
    ReadWriteOnce,
    ReadWriteMany,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum PersistenceSizeType {
    Mi,
    Gi,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeSelector {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Affinity {}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceMonitor {
    enabled: bool,
    port: u16,
    namespace: String,
    path: String,
}
