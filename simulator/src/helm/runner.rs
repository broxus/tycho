use std::io;
use std::io::Write;
use std::process::{Command, Output, Stdio};

use clap::ValueEnum;

use crate::config::SimulatorConfig;

const HELM_CHART: &str = "tycho"; // name in `Chart.yaml`

pub struct HelmRunner;

#[derive(ValueEnum, Clone, Copy, Debug, Default)]
pub enum ClusterType {
    #[default]
    K3S,
    Gke,
}

impl HelmRunner {
    pub fn upload_image(
        config: &SimulatorConfig,
        cluster_type: ClusterType,
    ) -> Result<(), io::Error> {
        match cluster_type {
            ClusterType::K3S => {
                println!("Uploading image {} to K3S cluster", config.pod.image_name);
                //  docker save tycho-simulated | sudo k3s ctr images import -
                let pipe = Command::new("docker")
                    .arg("save")
                    .arg(&config.pod.image_name)
                    .stdout(Stdio::piped())
                    .spawn()?;
                Command::new("sudo")
                    .arg("k3s")
                    .arg("ctr")
                    .arg("images")
                    .arg("import")
                    .arg("-")
                    .stdin(pipe.stdout.expect("Failed to capture stdout"))
                    .stdout(Stdio::piped())
                    .spawn()?
                    .wait()?;
                Ok(())
            }
            ClusterType::Gke => {
                println!("Uploading image {} to GKE cluster", config.pod.image_name);
                panic!("unimplemented: upload image to GKE");
            }
        }
    }

    pub fn exec_command(
        pod_name: &str,
        stdin: bool,
        tty: bool,
        cmd: &str,
        args: &[String],
    ) -> Result<(), io::Error> {
        Command::new("kubectl")
            .arg("exec")
            .arg(pod_name)
            .args(stdin.then_some("-i"))
            .args(tty.then_some("-t"))
            .arg("--")
            .arg(cmd)
            .args(args)
            .spawn()?
            .wait()?;
        Ok(())
    }

    pub fn ps() -> Result<(), io::Error> {
        Self::match_output(Command::new("kubectl").arg("get").arg("pods").output()?)
    }

    pub fn lint(config: &SimulatorConfig) -> Result<(), io::Error> {
        Self::helm(config, vec![
            &"lint".into(),
            &config.project_root.scratch.helm.tycho.dir,
            &"--strict".into(),
            &"--with-subcharts".into(),
            &"--debug".into(),
        ])
    }

    pub fn uninstall(config: &SimulatorConfig) -> Result<(), io::Error> {
        Self::helm(config, vec![
            "uninstall",
            &config.helm_release,
            "--cascade",
            "foreground",
            "--ignore-not-found",
            "--wait",
            "--debug",
        ])
    }

    pub fn upgrade_install(
        config: &SimulatorConfig,
        debug: bool,
        values: Option<&str>,
    ) -> Result<(), io::Error> {
        let mut args = vec![
            "upgrade",
            "--install",
            &config.helm_release,
            HELM_CHART,
            "--force",
            "--atomic",
            "--cleanup-on-fail",
            "--reset-values",
            "--wait",
            "--wait-for-jobs",
        ];
        if debug {
            args.push("--debug");
        }
        if let Some(values_file) = values {
            args.push("-f");
            args.push(values_file);
        }
        Self::helm(config, args)
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

    // TODO use or remove
    pub fn _kubectl<I, S>(args: I) -> Result<Output, io::Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        // Execute the command and return the output
        Command::new("kubectl").args(args).output()
    }

    pub fn helm<I, S>(config: &SimulatorConfig, args: I) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        Command::new("helm")
            .args(args)
            .current_dir(&config.project_root.scratch.helm.dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .stdin(Stdio::null())
            .spawn()?
            .wait()?;
        Ok(())
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
