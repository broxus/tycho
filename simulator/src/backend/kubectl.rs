use std::io;
use std::io::Write;
use std::process::{Command, Output, Stdio};

use anyhow::anyhow;

use crate::config::ClusterType;

pub struct KubeCtl;

impl KubeCtl {
    pub fn get_cluster_type() -> anyhow::Result<ClusterType> {
        let output = Command::new("kubectl")
            .arg("version")
            .arg("-o")
            .arg("json")
            .output()?;

        let json: serde_json::Value = if output.status.success() {
            serde_json::from_slice(&output.stdout)?
        } else {
            anyhow::bail!(io::Error::other(
                String::from_utf8_lossy(&output.stderr).to_string()
            ));
        };

        let version = json
            .get("serverVersion")
            .ok_or_else(|| anyhow!("no `serverVersion` field in {json}"))?
            .get("gitVersion")
            .ok_or_else(|| anyhow!("no `gitVersion` field in {json}"))?
            .to_string()
            .to_lowercase();

        if version.contains("k3s") {
            Ok(ClusterType::K3S)
        } else if version.contains("gke") {
            Ok(ClusterType::Gke)
        } else {
            Err(anyhow!(
                "unsupported cluster type in server version {version};\n
                use 'export KUBECONFIG=/path/to/kube/config' and 'kubectl config use-context <ctx>'"
            ))
        }
    }

    pub fn shell(pod_name: &str, ctrl_cmd: Option<String>) -> Result<(), io::Error> {
        let mut cmd = Command::new("kubectl");
        cmd.arg("exec")
            .arg(pod_name)
            .arg("-it")
            .arg("--")
            .arg("/bin/bash");
        if let Some(ctrl_cmd) = ctrl_cmd {
            cmd.arg("-c")
                .arg(format!("/app/tycho node mempool {ctrl_cmd}"));
        }
        cmd.spawn()?.wait()?;
        Ok(())
    }

    pub fn logs(pod_name: &str, follow: bool) -> Result<(), io::Error> {
        let mut command = Command::new("kubectl");
        command.arg("logs").arg(pod_name);
        if follow {
            command.arg("-f");
        }

        let mut child = command.stdout(Stdio::piped()).spawn()?;

        if follow {
            let mut stdout = child.stdout.take().expect("Failed to capture stdout");
            io::copy(&mut stdout, &mut io::stdout())?;
            child.wait()?;
            Ok(())
        } else {
            Self::match_output(child.wait_with_output()?)
        }
    }

    fn match_output(output: Output) -> Result<(), io::Error> {
        if output.status.success() {
            io::stdout().write_all(&output.stdout)
        } else {
            Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }
}
