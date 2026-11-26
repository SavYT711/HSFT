//! The network adapters on this PC, for choosing one to carry an extra connection.

use std::net::Ipv4Addr;

use serde::Serialize;

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct Adapter {
    pub name: String,
    pub ip: String,
}

/// Adapters with a usable IPv4 address.
pub fn adapters() -> Vec<Adapter> {
    let mut found: Vec<Adapter> = if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|interface| match interface.ip() {
            std::net::IpAddr::V4(ip) if usable(ip) => Some(Adapter {
                name: interface.name,
                ip: ip.to_string(),
            }),
            _ => None,
        })
        .collect();
    found.sort_by(|a, b| a.name.cmp(&b.name));
    found
}

fn usable(ip: Ipv4Addr) -> bool {
    !ip.is_loopback() && !ip.is_link_local() && !ip.is_unspecified()
}

/// Finds the current address of an adapter, given its name or an address.
pub fn resolve(adapter: &str) -> Option<Ipv4Addr> {
    if let Ok(ip) = adapter.parse() {
        return Some(ip);
    }
    adapters()
        .into_iter()
        .find(|a| a.name == adapter)
        .and_then(|a| a.ip.parse().ok())
}
