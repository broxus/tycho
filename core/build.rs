fn main() {
    match tycho_build_info::git_describe(tycho_build_info::DescribeFormat::CommitOnly) {
        Ok(build) => println!("cargo:rustc-env=TYCHO_BUILD={build}"),
        Err(err) => {
            println!("cargo:warning=unable to determine git version (not in git repository?)");
            println!("cargo:warning={err}");
            println!("cargo:rustc-env=TYCHO_BUILD=unknown");
        }
    }
}
