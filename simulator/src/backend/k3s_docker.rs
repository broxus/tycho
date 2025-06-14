use std::process::{Command, Stdio};

use anyhow::Context;

use crate::config::{BuilderValues, SimulatorConfig};

pub struct K3sDocker;

impl K3sDocker {
    pub fn build_upload(
        config: &SimulatorConfig,
        builder_values: &BuilderValues,
    ) -> Result<(), anyhow::Error> {
        if let Err(err) = Self::test_local_prerequisites() {
            println!("try build in cluster or install prerequisites");
            return Err(err);
        }

        if builder_values.base_image.build {
            let base_image = &builder_values.base_image;
            let image_name = format!("{}:{}", base_image.repository, base_image.tag);

            println!("Building docker image {image_name}");
            Command::new("docker")
                .arg("build")
                .arg("-t")
                .arg(image_name)
                .arg("-f")
                .arg(&config.project_root.base_dockerfile)
                .arg(&config.project_root.dir)
                .env("DOCKER_BUILDKIT", "1")
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()?
                .wait()?;
        }

        let tycho_image = &builder_values.tycho_image;
        let image_name = &format!("{}:{}", tycho_image.repository, tycho_image.tag);

        println!("Building docker image {image_name}");
        Command::new("docker")
            .arg("build")
            .arg("-t")
            .arg(image_name)
            .arg("-f")
            .arg(&config.project_root.tycho_dockerfile)
            .arg(&config.project_root.dir)
            .env("DOCKER_BUILDKIT", "1")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?
            .wait()?;

        println!("Saving image to docker: {image_name}");
        //  docker save tycho-simulated | sudo k3s ctr images import -
        let pipe = Command::new("docker")
            .arg("save")
            .arg(image_name)
            .stdout(Stdio::piped())
            .spawn()?;

        println!("Uploading image to K3S cluster: {image_name}");
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

    fn test_local_prerequisites() -> anyhow::Result<()> {
        Command::new("docker")
            .arg("--version")
            .output()
            .context("docker not found")?;
        Command::new("docker")
            .arg("buildx")
            .arg("--version")
            .output()
            .context("docker-buildx not found")?;
        Command::new("k3s")
            .arg("--version")
            .output()
            .context("k3s not found")?;
        Ok(())
    }
}
