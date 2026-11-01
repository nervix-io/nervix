//! Qualifies the `host:port` client address.
//!
//! Layer: test harness.
//! - **Owns.** The address shapes a client may write, literal hosts and refusals.
//! - **Depends on.** [`HostPort`].
//! - **Must not know.** Which connector reads an address.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the client address shape")
)]

use std::net::SocketAddr;

use rstest::rstest;

use super::*;

#[rstest]
#[case("syslog.example.test:5514", "syslog.example.test", 5514)]
#[case("127.0.0.1:514", "127.0.0.1", 514)]
#[case("[2001:db8::1]:6514", "2001:db8::1", 6514)]
#[case("0.0.0.0:0", "0.0.0.0", 0)]
fn an_address_reads_as_its_host_and_port(
    #[case] address: &str,
    #[case] host: &str,
    #[case] port: u16,
) {
    let parsed = HostPort::parse(address).expect("the address names a host and a port");
    assert_eq!(parsed.host(), host);
    assert_eq!(parsed.port(), port);
}

#[rstest]
#[case("127.0.0.1:514")]
#[case("[2001:db8::1]:6514")]
fn a_literal_host_is_a_socket_address(#[case] address: &str) {
    let expected: SocketAddr = address.parse().expect("the case is a socket address");
    let parsed = HostPort::parse(address).expect("the address names a host and a port");
    assert_eq!(parsed.socket_addr(), Some(expected));
}

#[test]
fn a_host_name_is_no_socket_address() {
    let parsed = HostPort::parse("localhost:514").expect("the address names a host and a port");
    assert_eq!(parsed.socket_addr(), None);
}

#[rstest]
#[case("missing-port", HostPortError::MissingPort)]
#[case("user:secret@syslog.example.test:5514", HostPortError::Shape)]
#[case("syslog.example.test:5514/path", HostPortError::Shape)]
#[case("syslog.example.test:5514?query=1", HostPortError::Shape)]
#[case("syslog.example.test:5514#fragment", HostPortError::Shape)]
#[case("syslog.example.test:port", HostPortError::Parse)]
#[case(":5514", HostPortError::Parse)]
fn an_address_holding_anything_else_is_refused(
    #[case] address: &str,
    #[case] refusal: HostPortError,
) {
    let error = HostPort::parse(address).expect_err("the address is not a host and a port");
    assert_eq!(error.current_context(), &refusal);
}
