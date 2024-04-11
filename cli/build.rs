use std::os::unix::ffi::OsStringExt;

use anyhow::Result;

fn main() -> Result<()> {
    let app_version = env("CARGO_PKG_VERSION")?;
    let app_version = match app_version.to_string_lossy() {
        std::borrow::Cow::Borrowed(version) => version,
        std::borrow::Cow::Owned(version) => {
            anyhow::bail!("invalid CARGO_PKG_VERSION: {version}")
        }
    };
    let git_version = get_git_version()?;
    let rustc_version = rustc_version::version()?;

    println!("cargo:rustc-env=TYCHO_VERSION={app_version}");
    println!("cargo:rustc-env=TYCHO_BUILD={git_version}");
    println!("cargo:rustc-env=TYCHO_RUSTC_VERSION={rustc_version}");
    Ok(())
}

fn get_git_version() -> Result<String> {
    let pkg_dir = std::path::PathBuf::from(env("CARGO_MANIFEST_DIR")?);
    let git_dir = command("git", &["rev-parse", "--git-dir"], Some(pkg_dir));
    let git_dir = match git_dir {
        Ok(git_dir) => std::path::PathBuf::from(std::ffi::OsString::from_vec(git_dir)),
        Err(msg) => {
            println!("cargo:warning=unable to determine git version (not in git repository?)");
            println!("cargo:warning={msg}");
            return Ok("unknown".to_owned());
        }
    };

    for subpath in ["HEAD", "logs/HEAD", "index"] {
        let path = git_dir.join(subpath).canonicalize()?;
        println!("cargo:rerun-if-changed={}", path.display());
    }

    // * --always -> if there is no matching tag, use commit hash
    // * --dirty=-modified -> append '-modified' if there are local changes
    // * --tags -> consider tags even if they are unnanotated
    // * --match=v[0-9]* -> only consider tags starting with a v+digit
    let args = &[
        "describe",
        "--always",
        "--dirty=-modified",
        "--tags",
        "--match=v[0-9]*",
    ];
    let out = command("git", args, None)?;
    match String::from_utf8_lossy(&out) {
        std::borrow::Cow::Borrowed(version) => Ok(version.trim().to_string()),
        std::borrow::Cow::Owned(version) => {
            anyhow::bail!("git: invalid output: {version}")
        }
    }
}

fn command(prog: &str, args: &[&str], cwd: Option<std::path::PathBuf>) -> Result<Vec<u8>> {
    println!("cargo:rerun-if-env-changed=PATH");
    let mut cmd = std::process::Command::new(prog);
    cmd.args(args);
    cmd.stderr(std::process::Stdio::inherit());
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    let out = cmd.output()?;
    if out.status.success() {
        let mut stdout = out.stdout;
        if let Some(b'\n') = stdout.last() {
            stdout.pop();
            if let Some(b'\r') = stdout.last() {
                stdout.pop();
            }
        }
        Ok(stdout)
    } else if let Some(code) = out.status.code() {
        anyhow::bail!("{prog}: terminated with {code}");
    } else {
        anyhow::bail!("{prog}: killed by signal")
    }
}

fn env(key: &str) -> Result<std::ffi::OsString> {
    println!("cargo:rerun-if-env-changed={}", key);
    std::env::var_os(key).ok_or_else(|| anyhow::anyhow!("missing '{}' environment variable", key))
}
