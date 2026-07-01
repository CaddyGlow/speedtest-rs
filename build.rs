use std::process::Command;
use std::{env, fs};

fn main() {
    println!("cargo:rerun-if-env-changed=GITHUB_REF");
    println!("cargo:rerun-if-env-changed=GITHUB_REF_NAME");
    println!("cargo:rerun-if-env-changed=GITHUB_REF_TYPE");
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-env-changed=TUNMUX_SPEEDTEST_VERSION_LABEL");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/logs/HEAD");
    print_current_git_ref_rerun_directive();
    println!("cargo:rerun-if-changed=.git/refs/tags");

    let version_label = env_version_label()
        .or_else(github_tag_version)
        .or_else(git_tag_version)
        .unwrap_or_else(|| env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string()));
    let git_hash = github_short_sha()
        .or_else(git_short_hash)
        .unwrap_or_else(|| "unknown".to_string());
    let version = format!("{version_label} ({git_hash})");

    println!("cargo:rustc-env=TUNMUX_SPEEDTEST_VERSION={version}");
}

fn env_version_label() -> Option<String> {
    env::var("TUNMUX_SPEEDTEST_VERSION_LABEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn print_current_git_ref_rerun_directive() {
    let Ok(head) = fs::read_to_string(".git/HEAD") else {
        return;
    };
    let Some(ref_name) = head.trim().strip_prefix("ref: ") else {
        return;
    };
    println!("cargo:rerun-if-changed=.git/{ref_name}");
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

fn github_short_sha() -> Option<String> {
    let sha = env::var("GITHUB_SHA").ok()?;
    short_hash(&sha)
}

fn git_short_hash() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let hash = String::from_utf8(output.stdout).ok()?;
    short_hash(hash.trim())
}

fn short_hash(hash: &str) -> Option<String> {
    let hash = hash.trim();
    if hash.is_empty() {
        return None;
    }
    Some(hash.chars().take(12).collect())
}
