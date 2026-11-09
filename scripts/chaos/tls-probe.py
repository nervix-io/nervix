"""The TLS probe of the leader-crash scenario's outward TLS policy check.

Runs inside the pinned Python tool image and uses only the standard library. For every target it
opens one TCP connection and one TLS handshake that offers only TLS 1.3, the
TLS_CHACHA20_POLY1305_SHA256 cipher suite and the X25519 group: the members the `fips` policy alias
excludes. It verifies the node's certificate against the run's authority and the target's host
name, and prints one JSON document on standard output.

The ssl module has no call that selects TLS 1.3 cipher suites, so OpenSSL takes them from the
configuration that OPENSSL_CONF names (tls-probe-openssl.cnf). The probe selects the group itself,
reads back what the client will offer and refuses to judge when that is anything else, so a
configuration that did not load can never pass for a policy verdict.

Each target's outcome is one of:

accepted  The handshake completed over TLS 1.3 with TLS_CHACHA20_POLY1305_SHA256.
refused   The node answered the offer with a handshake_failure alert: it shares no cipher suite or
          no group with this client.
failed    Anything else: no connection, a timeout, a certificate that did not verify, another alert
          or a negotiation the client did not offer. A failed target is never a policy verdict.

Exit status 0 means every target was accepted or refused, 1 that at least one failed, and 2 a usage
error or a client that would offer something other than the restricted set.
"""

from __future__ import annotations

import argparse
import json
import socket
import ssl
import sys
import time

OFFERED_CIPHER_SUITE = "TLS_CHACHA20_POLY1305_SHA256"
OFFERED_GROUP = "X25519"
OFFERED_VERSION = "TLSv1.3"
# OpenSSL's reason for the handshake_failure alert a TLS server sends when it shares no cipher
# suite or no key-exchange group with the client.
HANDSHAKE_FAILURE_ALERT = "SSLV3_ALERT_HANDSHAKE_FAILURE"


class RestrictionNotInEffect(Exception):
    """The client would offer something other than the restricted set."""


def restricted_context(ca_path: str) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cafile=ca_path)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.set_ecdh_curve(OFFERED_GROUP)
    offered = [cipher["name"] for cipher in context.get_ciphers() if cipher["protocol"] == OFFERED_VERSION]
    if offered != [OFFERED_CIPHER_SUITE]:
        raise RestrictionNotInEffect(
            f"the client would offer the TLS 1.3 cipher suites {offered}, not only {OFFERED_CIPHER_SUITE}; "
            "OPENSSL_CONF must name tls-probe-openssl.cnf"
        )
    return context


def probe(context: ssl.SSLContext, host: str, port: int, timeout_seconds: float) -> dict:
    result: dict = {"host": host, "port": port}
    started = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds) as connection:
            with context.wrap_socket(connection, server_hostname=host) as tls:
                negotiated_cipher, negotiated_version, _bits = tls.cipher()
        result["version"] = negotiated_version
        result["cipher"] = negotiated_cipher
        if negotiated_version == OFFERED_VERSION and negotiated_cipher == OFFERED_CIPHER_SUITE:
            result["outcome"] = "accepted"
        else:
            result["outcome"] = "failed"
            result["error"] = "the handshake negotiated parameters the client did not offer"
    except ssl.SSLCertVerificationError as error:
        result["outcome"] = "failed"
        result["error"] = f"certificate verification: {error.verify_message}"
    except ssl.SSLError as error:
        result["reason"] = error.reason
        if error.reason == HANDSHAKE_FAILURE_ALERT:
            result["outcome"] = "refused"
        else:
            result["outcome"] = "failed"
            result["error"] = str(error)
    except OSError as error:
        result["outcome"] = "failed"
        result["error"] = f"{type(error).__name__}: {error}"
    result["elapsed_ms"] = round((time.monotonic() - started) * 1000)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ca", required=True, help="PEM certificate authority that signed every target")
    parser.add_argument("--port", required=True, type=int, help="TLS port every target listens on")
    parser.add_argument("--timeout", default=5.0, type=float, help="connection and handshake bound in seconds")
    parser.add_argument("hosts", nargs="+", help="host names, each also the expected certificate name")
    arguments = parser.parse_args()
    if not 0 < arguments.port < 65536 or arguments.timeout <= 0:
        parser.error("the port must be a TCP port and the timeout positive")
    try:
        context = restricted_context(arguments.ca)
    except RestrictionNotInEffect as error:
        print(f"tls-probe: {error}", file=sys.stderr)
        return 2
    targets = [probe(context, host, arguments.port, arguments.timeout) for host in arguments.hosts]
    document = {
        "client": {
            "openssl": ssl.OPENSSL_VERSION,
            "version": OFFERED_VERSION,
            "cipher_suites": [OFFERED_CIPHER_SUITE],
            "groups": [OFFERED_GROUP],
        },
        "targets": targets,
    }
    sys.stdout.write(json.dumps(document, separators=(",", ":"), sort_keys=True) + "\n")
    if all(target["outcome"] in ("accepted", "refused") for target in targets):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
