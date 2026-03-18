use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const CACHE_SUBDIR: &str = "speedtest-rs";
const SESSION_FILE_NAME: &str = "modern-session.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModernSession {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_auth_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_isp_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_hash: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cookies: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saved_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl ModernSession {
    #[must_use]
    pub fn cookie_header_value(&self) -> Option<String> {
        if self.cookies.is_empty() {
            return None;
        }

        let value = self
            .cookies
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        Some(value)
    }

    pub fn apply_set_cookie_header_line(&mut self, line: &str) {
        let mut parts = line.split(';');
        let Some(name_value) = parts.next() else {
            return;
        };

        let Some((name, value)) = name_value.split_once('=') else {
            return;
        };

        let cookie_name = name.trim();
        if cookie_name.is_empty() {
            return;
        }

        let mut expired = false;
        let mut expires_at = None;
        for attribute in parts {
            let trimmed = attribute.trim();
            if trimmed.eq_ignore_ascii_case("max-age=0") {
                expired = true;
                continue;
            }

            if let Some((attr_name, attr_value)) = trimmed.split_once('=') {
                if attr_name.eq_ignore_ascii_case("max-age")
                    && attr_value.trim().parse::<i64>().unwrap_or(1) <= 0
                {
                    expired = true;
                }

                if attr_name.eq_ignore_ascii_case("expires") {
                    expires_at = Some(attr_value.trim().to_string());
                }
            }
        }

        if expired {
            self.cookies.remove(cookie_name);
        } else {
            self.cookies
                .insert(cookie_name.to_string(), value.trim().to_string());
        }

        if expires_at.is_some() {
            self.expires_at = expires_at;
        }
    }

    pub fn touch_saved_at(&mut self) {
        self.saved_at = Some(current_timestamp().to_string());
    }
}

pub fn load_modern_session() -> Result<Option<ModernSession>> {
    let path = session_file_path()?;
    if !path.exists() {
        return Ok(None);
    }

    let body = fs::read_to_string(&path)
        .with_context(|| format!("failed reading session file {}", path.display()))?;
    let parsed = serde_json::from_str::<ModernSession>(&body)
        .with_context(|| format!("failed parsing session file {}", path.display()))?;
    Ok(Some(parsed))
}

pub fn save_modern_session(session: &ModernSession) -> Result<()> {
    let path = session_file_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating cache directory {}", parent.display()))?;
    }

    let body = serde_json::to_string_pretty(session)?;
    fs::write(&path, body)
        .with_context(|| format!("failed writing session file {}", path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Ok(metadata) = fs::metadata(&path) {
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o600);
            let _ = fs::set_permissions(&path, permissions);
        }
    }

    Ok(())
}

#[must_use]
pub fn generate_session_guid() -> String {
    let now = current_timestamp();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos())
        .unwrap_or(0);
    format!("speedtest-rs-{now:x}-{nanos:08x}-{}", std::process::id())
}

pub fn session_file_path() -> Result<PathBuf> {
    if let Ok(cache_home) = std::env::var("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(cache_home)
            .join(CACHE_SUBDIR)
            .join(SESSION_FILE_NAME));
    }

    let home = std::env::var("HOME").context("HOME is not set; cannot resolve session path")?;
    Ok(PathBuf::from(home)
        .join(".cache")
        .join(CACHE_SUBDIR)
        .join(SESSION_FILE_NAME))
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::ModernSession;

    #[test]
    fn builds_cookie_header() {
        let mut session = ModernSession::default();
        session.cookies.insert("a".to_string(), "1".to_string());
        session.cookies.insert("b".to_string(), "2".to_string());

        let header = session
            .cookie_header_value()
            .expect("header should exist for non-empty cookie jar");
        assert!(header.contains("a=1"));
        assert!(header.contains("b=2"));
    }

    #[test]
    fn applies_and_removes_cookie_from_set_cookie() {
        let mut session = ModernSession::default();
        session.apply_set_cookie_header_line("foo=bar; Path=/; HttpOnly");
        assert_eq!(session.cookies.get("foo").map(String::as_str), Some("bar"));

        session.apply_set_cookie_header_line("foo=gone; Max-Age=0; Path=/");
        assert!(!session.cookies.contains_key("foo"));
    }
}
