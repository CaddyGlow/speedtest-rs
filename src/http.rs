use anyhow::Result;
use reqwest::{Client, Proxy};
use std::time::Duration;

use crate::util::validate_proxy_scheme;

pub fn build_client(proxy_url: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder()
        .user_agent("tunmux-speedtest/0.1.0")
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(60));

    if let Some(proxy_url) = proxy_url {
        validate_proxy_scheme(proxy_url)?;
        builder = builder.proxy(Proxy::all(proxy_url)?);
    }

    Ok(builder.build()?)
}
