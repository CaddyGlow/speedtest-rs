use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=GITHUB_REF");
    println!("cargo:rerun-if-env-changed=GITHUB_REF_NAME");
    println!("cargo:rerun-if-env-changed=GITHUB_REF_TYPE");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/tags");

    let version = github_tag_version()
        .or_else(git_tag_version)
        .unwrap_or_else(|| env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string()));

    println!("cargo:rustc-env=TUNMUX_SPEEDTEST_VERSION={version}");
}

fn github_tag_version() -> Option<String> {
    let ref_type = env::var("GITHUB_REF_TYPE").ok()?;
    if ref_type != "tag" {
        return None;
    }

    if let Ok(tag) = env::var("GITHUB_REF_NAME") {
        let tag = tag.trim().to_string();
        if !tag.is_empty() {
            return Some(tag);
        }
    }

    let full_ref = env::var("GITHUB_REF").ok()?;
    full_ref
        .strip_prefix("refs/tags/")
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(ToOwned::to_owned)
}

fn git_tag_version() -> Option<String> {
    let output = Command::new("git")
        .args(["describe", "--tags", "--exact-match"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let tag = String::from_utf8(output.stdout).ok()?;
    let tag = tag.trim();
    if tag.is_empty() {
        None
    } else {
        Some(tag.to_string())
    }
}
