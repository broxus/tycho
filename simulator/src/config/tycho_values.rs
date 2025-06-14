use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{BuilderValues, ClusterType, PodConfig, TychoImageConfig};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelmValues {
    only_network: bool,
    tycho_image: TychoImageConfig,
    image_pull_policy: ImagePullPolicy,
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharedConfigs {
    /// Used only by network example
    pub rust_log: String,
    pub global_config: String,
    /// tycho node config is conventionally named just `config`
    pub config: String,
    pub zerostate: String,
    pub logger: String,
}

impl HelmValues {
    pub fn new(
        cluster_type: ClusterType,
        pod_config: &PodConfig,
        builder_values: &BuilderValues,
        shared_conf: SharedConfigs,
        node_secrets: Vec<String>,
        pod_disk_gb: u8,
        pod_milli_cpu: u16,
    ) -> Self {
        let fullname_override = "tycho".to_string();

        let keys = "keys".to_string();
        let shared_configs = "shared-configs".to_string();
        let data_volume = "data-volume".to_string();

        let resources = ResourcesValue {
            cpu: format!("{pod_milli_cpu}m"),
        };

        let mut volumes = vec![
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
        ];
        match cluster_type {
            ClusterType::Gke => {} // PersistentVolumeClaimTemplate is used
            ClusterType::K3S => volumes.push(Volume {
                name: data_volume.clone(),
                volume_type: VolumeType::PersistentVolumeClaim(PersistentVolumeClaim {
                    claim_name: format!("{fullname_override}-data-local-claim"),
                }),
                // volume_type: VolumeType::EmptyDir(EmptyDir {
                //     size_limit: format!("{mempool_data_gb}{:?}", PersistenceSizeType::Gi),
                //     medium: None,
                // }),
            }),
        }

        Self {
            only_network: false,
            tycho_image: builder_values.tycho_image.clone(),
            image_pull_policy: ImagePullPolicy::IfNotPresent,
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
                storage_class: match cluster_type {
                    ClusterType::Gke => "premium-rwo".to_string(),
                    ClusterType::K3S => "local-storage".to_string(),
                },
                host_path: match cluster_type {
                    ClusterType::Gke => "".to_string(),
                    ClusterType::K3S => "/var/tmp".to_string(),
                },
                access_mode: PersistenceAccessMode::ReadWriteOnce,
                size_per_pod: pod_disk_gb as _,
                size_type: PersistenceSizeType::Gi,
            }),
            volumes,
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
                    sub_path_type: Some(SubPathType::SubPathExpr("$(POD_HOST_NAME)".to_string())),
                    read_only: false,
                },
            ],
            service_monitor: ServiceMonitor {
                enabled: true,
                port: pod_config.metrics_port,
                namespace: "monitoring".to_string(), // must be where prometheus is installed
                path: "/".to_string(),               // empty defaults to /metrics, will also do
            },
            resources: Resources {
                requests: resources.clone(),
                limits: resources,
            },
            // defaults
            image_pull_secrets: vec![],
            name_override: "".to_string(),
            service_account: ServiceAccount::default(),
            pod_annotations: PodAnnotations {},
            pod_labels: PodLabels {},
            pod_security_context: PodSecurityContext {},
            node_selector: NodeSelector {},
            tolerations: vec![],
            affinity: Affinity {},
            security_context: SecurityContext {},
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum ImagePullPolicy {
    IfNotPresent,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceAccount {
    create: bool,
    automount: bool,
    annotations: Annotations,
    name: String,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Annotations {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PodAnnotations {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PodLabels {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PodSecurityContext {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SecurityContext {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Service {
    #[serde(rename = "type")]
    type_field: String,
    port: u16,
    #[serde(rename = "clusterIP")]
    cluster_ip: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Resources {
    requests: ResourcesValue,
    limits: ResourcesValue,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourcesValue {
    cpu: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Volume {
    name: String,
    #[serde(flatten)]
    volume_type: VolumeType,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum VolumeType {
    PersistentVolumeClaim(PersistentVolumeClaim),
    ConfigMap(ConfigMap),
    Secret(Secret),
    EmptyDir(EmptyDir),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigMap {
    name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Secret {
    secret_name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EmptyDir {
    size_limit: String,
    /// leave empty to use file fs, not tmpfs
    medium: Option<EmptyDirMedium>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum EmptyDirMedium {
    Memory,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistentVolumeClaim {
    claim_name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VolumeMount {
    name: String,
    mount_path: String,
    #[serde(flatten)]
    sub_path_type: Option<SubPathType>,
    read_only: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum SubPathType {
    SubPath(String),
    SubPathExpr(String),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Persistence {
    storage_class: String,
    host_path: String,
    access_mode: PersistenceAccessMode,
    size_per_pod: usize,
    size_type: PersistenceSizeType,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum PersistenceAccessMode {
    ReadWriteOnce,
    ReadWriteMany,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum PersistenceSizeType {
    Gi, // 2^30 bytes
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeSelector {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Affinity {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceMonitor {
    enabled: bool,
    port: u16,
    namespace: String,
    path: String,
}
