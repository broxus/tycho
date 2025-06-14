use std::io;
use std::process::{Command, Stdio};

use crate::config::SimulatorConfig;

pub struct Helm;
pub struct HelmChart(pub &'static str);

impl Helm {
    pub const TYCHO_CHART: HelmChart = HelmChart("tycho");
    pub const BUILDER_CHART: HelmChart = HelmChart("builder");

    pub fn lint(config: &SimulatorConfig) -> Result<(), io::Error> {
        let helm = &config.project_root.simulator.helm;
        for dir in [&helm.builder.dir, &helm.tycho.dir] {
            Self::helm(config, vec![
                &"lint".into(),
                dir,
                &"--strict".into(),
                &"--debug".into(),
            ])?;
        }
        Ok(())
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
        chart: HelmChart,
        quiet: bool,
    ) -> Result<(), io::Error> {
        let mut args = vec![
            "upgrade",
            "--install",
            &config.helm_release,
            chart.0,
            "--force",
            "--atomic",
            "--cleanup-on-fail",
            "--reset-values",
            "--wait",
            "--debug",
        ];
        if !quiet {
            args.push("--debug");
        }
        Self::helm(config, args)
    }

    fn helm<I, S>(config: &SimulatorConfig, args: I) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        Command::new("helm")
            .args(args)
            .current_dir(&config.project_root.simulator.helm.dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .stdin(Stdio::null())
            .spawn()?
            .wait()?;
        Ok(())
    }
}
