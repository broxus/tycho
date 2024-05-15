use std::ffi::OsString;
use std::io;
use std::io::Write;
use std::process::{Command, Output, Stdio};

use clap::ValueEnum;

use crate::config::ServiceConfig;

pub struct HelmRunner {
    service_config: ServiceConfig,
    pub cluster_type: ClusterType,
}

#[derive(ValueEnum, Clone, Debug, Copy)]
pub enum ClusterType {
    K3S,
    GKE,
}

impl HelmRunner {
    pub fn new(service_config: ServiceConfig, cluster_type: ClusterType) -> Self {
        Self {
            service_config,
            cluster_type,
        }
    }

    pub fn upload_image(&self, image: &str) -> Result<(), io::Error> {
        match self.cluster_type {
            ClusterType::K3S => {
                println!("Uploading image {} to K3S cluster", image);
                //  docker save tycho-network | sudo k3s ctr images import -
                let pipe = Command::new("docker")
                    .arg("save")
                    .arg(image)
                    .stdout(Stdio::piped())
                    .spawn()?;
                let mut output = Command::new("sudo")
                    .arg("k3s")
                    .arg("ctr")
                    .arg("images")
                    .arg("import")
                    .arg("-")
                    .stdin(pipe.stdout.expect("Failed to capture stdout"))
                    .stdout(Stdio::piped())
                    .spawn()?;
                output.wait()?;
                Ok(())
            }
            ClusterType::GKE => {
                println!("Uploading image {} to GKE cluster", image);
                todo!()
            }
        }
    }

    pub fn exec_command(&self, pod_name: &str, command: &[String]) -> Result<String, io::Error> {
        let output = Command::new("kubectl")
            .arg("exec")
            .arg(pod_name)
            .arg("--")
            .args(command)
            .output()?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&output.stderr).to_string(),
            ))
        }
    }

    pub fn ps(&self) -> Result<(), io::Error> {
        Command::new("kubectl")
            .arg("get")
            .arg("pods")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;

        Ok(())
    }

    pub fn stop_node(&self, release_name: &str) -> Result<(), io::Error> {
        let res = self.helm(vec!["uninstall", release_name].into_iter())?;

        if res.status.success() {
            io::stdout().write_all(&res.stdout)?;

            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&res.stderr).to_string(),
            ))
        }
    }

    pub fn start_node(
        &self,
        release_name: &str,
        chart: &str,
        values: Option<&str>,
    ) -> Result<Output, io::Error> {
        let mut args = vec!["upgrade", "--install", release_name, chart];
        if let Some(values_file) = values {
            args.push("-f");
            args.push(values_file);
        }
        let res = self.helm(args.into_iter())?;

        if res.status.success() {
            io::stdout().write_all(&res.stdout)?;
            Ok(res)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                String::from_utf8_lossy(&res.stderr).to_string(),
            ))
        }
    }

    pub fn logs(&self, pod_name: &str, follow: bool) -> Result<(), io::Error> {
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
        } else {
            let output = child.wait_with_output()?;
            if output.status.success() {
                io::stdout().write_all(&output.stdout)?;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    String::from_utf8_lossy(&output.stderr).to_string(),
                ));
            }
        }

        Ok(())
    }
    pub fn kubectl<A, T>(&self, args: A) -> Result<Output, io::Error>
    where
        A: Iterator<Item = T>,
        T: Into<OsString>,
    {
        let mut command = Command::new("kubectl");

        for arg in args {
            command.arg(arg.into());
        }

        // Execute the command and return the output
        command.output()
    }

    pub fn helm<A, T>(&self, args: A) -> Result<Output, io::Error>
    where
        A: Iterator<Item = T>,
        T: Into<OsString>,
    {
        let mut command = Command::new("helm");
        let helm_dir = self.service_config.helm_template_output();

        for arg in args {
            command.arg(arg.into());
        }

        command.current_dir(helm_dir);

        command.output()
    }
}
