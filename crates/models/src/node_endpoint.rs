//! The network endpoints a cluster node advertises.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The host and port one node advertises, and the URL form of one advertised service.
//! - **Depends on.** URL parsing and standard socket-address primitives.
//! - **Must not know.** Gossip, consensus membership, name resolution or any transport.

use std::{fmt, net::SocketAddr, str::FromStr};

use error_stack::Report;
use thiserror::Error;
use url::Url;

/// One host and port a cluster node advertises itself on.
///
/// The host is kept as advertised rather than resolved, so a node behind a name that moves between
/// addresses keeps one stable endpoint. Resolving it belongs to whoever dials it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeEndpoint {
    host: String,
    port: u16,
}

impl NodeEndpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub const fn port(&self) -> u16 {
        self.port
    }

    pub fn with_port(&self, port: u16) -> Self {
        Self::new(self.host.clone(), port)
    }

    /// The `host:port` authority, bracketing a literal IPv6 host.
    pub fn authority(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

impl fmt::Display for NodeEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.authority())
    }
}

impl From<SocketAddr> for NodeEndpoint {
    fn from(addr: SocketAddr) -> Self {
        Self::new(addr.ip().to_string(), addr.port())
    }
}

impl FromStr for NodeEndpoint {
    type Err = NodeEndpointParseError;

    fn from_str(advertised: &str) -> Result<Self, Self::Err> {
        if let Ok(addr) = advertised.parse::<SocketAddr>() {
            return Ok(Self::from(addr));
        }

        let Some((host, port)) = advertised.rsplit_once(':') else {
            return Err(NodeEndpointParseError::MissingPort {
                advertised: advertised.to_string(),
            });
        };
        if host.is_empty() {
            return Err(NodeEndpointParseError::MissingHost {
                advertised: advertised.to_string(),
            });
        }
        if host.contains(':') {
            return Err(NodeEndpointParseError::UnbracketedIpv6Host {
                advertised: advertised.to_string(),
            });
        }
        let Ok(port) = port.parse::<u16>() else {
            return Err(NodeEndpointParseError::InvalidPort {
                advertised: advertised.to_string(),
                port: port.to_string(),
            });
        };
        Ok(Self::new(host.to_string(), port))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NodeEndpointParseError {
    #[error("advertised endpoint '{advertised}' has no ':port' suffix")]
    MissingPort { advertised: String },
    #[error("advertised endpoint '{advertised}' has no host")]
    MissingHost { advertised: String },
    #[error("advertised endpoint '{advertised}' must write an IPv6 host as '[addr]:port'")]
    UnbracketedIpv6Host { advertised: String },
    #[error("advertised endpoint '{advertised}' has an invalid port '{port}'")]
    InvalidPort { advertised: String, port: String },
}

/// The URL a node advertises one of its own services on.
///
/// Every value carries a scheme and a host, so a holder reaches the service by rendering the URL
/// rather than by rebuilding one from parts that may be missing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeServiceUrl(String);

impl NodeServiceUrl {
    /// The URL of the service reached at `endpoint` over `scheme`.
    pub fn new(
        scheme: &str,
        endpoint: &NodeEndpoint,
    ) -> error_stack::Result<Self, NodeServiceUrlParseError> {
        let advertised = format!("{scheme}://{}", endpoint.authority());
        advertised.parse().map_err(Report::new)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeServiceUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for NodeServiceUrl {
    type Err = NodeServiceUrlParseError;

    fn from_str(advertised: &str) -> Result<Self, Self::Err> {
        let Ok(url) = Url::parse(advertised) else {
            return Err(NodeServiceUrlParseError::Malformed {
                advertised: advertised.to_string(),
            });
        };
        if url.host().is_none() {
            return Err(NodeServiceUrlParseError::MissingHost {
                advertised: advertised.to_string(),
            });
        }
        Ok(Self(advertised.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NodeServiceUrlParseError {
    #[error("advertised service url '{advertised}' is not a url")]
    Malformed { advertised: String },
    #[error("advertised service url '{advertised}' has no host")]
    MissingHost { advertised: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_parses_a_hostname_and_port() {
        let parsed = "nervix-0.nervix-headless:47392"
            .parse::<NodeEndpoint>()
            .expect("host:port should parse");
        assert_eq!(parsed.host(), "nervix-0.nervix-headless");
        assert_eq!(parsed.port(), 47392);
        assert_eq!(parsed.to_string(), "nervix-0.nervix-headless:47392");
    }

    #[test]
    fn endpoint_round_trips_an_ipv6_socket_address() {
        let parsed = "[::1]:47392"
            .parse::<NodeEndpoint>()
            .expect("IPv6 socket address should parse");
        assert_eq!(parsed.host(), "::1");
        assert_eq!(parsed.to_string(), "[::1]:47392");
        assert_eq!(parsed.authority(), "[::1]:47392");
    }

    #[test]
    fn endpoint_rejects_malformed_advertisements() {
        assert_eq!(
            "nervix-0".parse::<NodeEndpoint>(),
            Err(NodeEndpointParseError::MissingPort {
                advertised: "nervix-0".to_string(),
            })
        );
        assert_eq!(
            ":47392".parse::<NodeEndpoint>(),
            Err(NodeEndpointParseError::MissingHost {
                advertised: ":47392".to_string(),
            })
        );
        assert_eq!(
            "::1:47392".parse::<NodeEndpoint>(),
            Err(NodeEndpointParseError::UnbracketedIpv6Host {
                advertised: "::1:47392".to_string(),
            })
        );
        assert_eq!(
            "nervix-0:http".parse::<NodeEndpoint>(),
            Err(NodeEndpointParseError::InvalidPort {
                advertised: "nervix-0:http".to_string(),
                port: "http".to_string(),
            })
        );
        assert_eq!(
            "https://nervix-0.test:7443".parse::<NodeEndpoint>(),
            Err(NodeEndpointParseError::UnbracketedIpv6Host {
                advertised: "https://nervix-0.test:7443".to_string(),
            })
        );
    }

    #[test]
    fn service_url_renders_its_endpoint_under_the_given_scheme() {
        let endpoint = NodeEndpoint::new("nervix-0.test", 7443);
        let url = NodeServiceUrl::new("https", &endpoint).expect("a host and port form a url");
        assert_eq!(url.to_string(), "https://nervix-0.test:7443");

        let ipv6 = NodeEndpoint::new("::1", 7443);
        let url = NodeServiceUrl::new("http", &ipv6).expect("an IPv6 host and port form a url");
        assert_eq!(url.to_string(), "http://[::1]:7443");
    }

    #[test]
    fn service_url_rejects_advertisements_without_a_host() {
        assert_eq!(
            "nervix-0.test:7443".parse::<NodeServiceUrl>(),
            Err(NodeServiceUrlParseError::MissingHost {
                advertised: "nervix-0.test:7443".to_string(),
            })
        );
        assert_eq!(
            "".parse::<NodeServiceUrl>(),
            Err(NodeServiceUrlParseError::Malformed {
                advertised: String::new(),
            })
        );
    }
}
