use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuilderValues {
    git: GitConfig,
    registry: RegistryConfig,
    pub base_image: BaseImageConfig,
    pub tycho_image: TychoImageConfig,
}

impl Default for BuilderValues {
    fn default() -> Self {
        Self {
            git: GitConfig {
                url: "https://github.com/broxus/tycho.git".to_string(),
                revision: "".to_string(),
            },
            registry: RegistryConfig {
                name: "docker.io".to_string(),
                secret_name: "tycho-docker-config".to_string(),
            },
            base_image: BaseImageConfig {
                repository: "localhost/tycho-rocksdb".to_string(),
                tag: "latest".to_string(),
                build: false,
            },
            tycho_image: TychoImageConfig {
                repository: "localhost/tycho-simulated".to_string(),
                tag: "latest".to_string(),
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GitConfig {
    url: String,
    revision: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegistryConfig {
    name: String,
    secret_name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseImageConfig {
    pub repository: String,
    pub tag: String,
    pub build: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TychoImageConfig {
    pub repository: String,
    pub tag: String,
}
