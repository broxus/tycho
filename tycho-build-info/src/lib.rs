use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow};

/// Controls how [`git_describe`] formats the reported git revision.
pub enum DescribeFormat {
    /// Uses `git describe --dirty --tags`
    Reach,
    /// Only emit the abbreviated commit hash, matching `git rev-parse HEAD`
    CommitOnly,
}

/// Returns the current git version formatted according to [`DescribeFormat`].
///
/// * [`DescribeFormat::Reach`] produces output identical to `git describe --always --dirty --tags`.
///   `[tag]-[number of commits since tag]-g[short hash][-modified if dirty]` Eg v0.3.2-20-gedb15c6f7-modified
/// * [`DescribeFormat::CommitOnly`] emits full commit hash from `git rev-parse`.
pub fn git_describe(describe_type: DescribeFormat) -> Result<String> {
    let manifest_dir = manifest_dir()?;
    let git_dir = git_dir(&manifest_dir)?;
    register_git_paths(&git_dir);

    match describe_type {
        DescribeFormat::Reach => run_git(&manifest_dir, [
            "describe",
            "--always",
            "--dirty=-modified",
            "--tags",
            "--match=v[0-9]*",
        ]),
        DescribeFormat::CommitOnly => run_git(&manifest_dir, ["rev-parse", "HEAD"]),
    }
}

fn manifest_dir() -> Result<PathBuf> {
    std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("missing 'CARGO_MANIFEST_DIR' environment variable"))
}

fn git_dir(manifest_dir: &Path) -> Result<PathBuf> {
    let raw = run_git(manifest_dir, ["rev-parse", "--git-dir"])?;
    let path = PathBuf::from(raw.trim());
    Ok(if path.is_absolute() {
        path
    } else {
        manifest_dir.join(path)
    })
}

fn register_git_paths(git_dir: &Path) {
    for subpath in ["HEAD", "logs/HEAD", "index"] {
        if let Ok(path) = git_dir.join(subpath).canonicalize() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}

fn run_git(cwd: &Path, args: impl IntoIterator<Item = &'static str>) -> Result<String> {
    let args: Vec<&'static str> = args.into_iter().collect();
    run_command(cwd, "git", &args)
}

pub fn run_command(cwd: &Path, program: &str, args: &[&str]) -> Result<String> {
    println!("cargo:rerun-if-env-changed=PATH");
    let output = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .stderr(Stdio::inherit())
        .output()
        .with_context(|| format!("{program} {:?} failed", args))?;

    if output.status.success() {
        let mut text = String::from_utf8(output.stdout)
            .with_context(|| format!("{program} {:?} printed invalid utf-8", args))?;
        while matches!(text.as_bytes().last(), Some(b'\n' | b'\r')) {
            text.pop();
        }
        Ok(text)
    } else if let Some(code) = output.status.code() {
        Err(anyhow!("{program} {:?}: exited with {code}", args))
    } else {
        Err(anyhow!("{program} {:?}: terminated by signal", args))
    }
}
