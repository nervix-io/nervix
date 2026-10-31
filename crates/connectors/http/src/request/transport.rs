//! The transport an HTTP client's configuration declares for every entity that sends through it:
//! the one deadline of a request and the redirects it follows, and the one rule by which a
//! redirect is followed.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading a client's required `timeout_ms` and its `follow_redirects` opt-in, and
//!   deciding from one redirect answer whether its request is repeated and where: the answers that
//!   move a request without changing it, the client's own origin as the only place a request is
//!   repeated, and the declared limit.
//! - **Depends on.** The vocabulary's HTTP transport contract, the shared deadline reader, a
//!   client's configuration entries and URL resolution.
//! - **Must not know.** How a request is sent or through which library, or what an answer means for
//!   a poll or a record.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::time::Duration;

use error_stack::Report;
use nervix_connector::{
    ClientConfigError, ClientConfigResult, ConfiguredDeadline, optional_bool_client_config_value,
};
use nervix_models::{ClientConfigEntry, CreateClientHttp};
use url::Url;

/// The configuration key by which a client opts into following redirects.
const FOLLOW_REDIRECTS: &str = "follow_redirects";

/// What an HTTP client's configuration declares about how each of its requests travels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClientTransport {
    /// The one physical deadline of a request, shared with every redirect it follows: name
    /// resolution, connection, TLS, the request and its response included.
    pub(crate) timeout: Duration,
    pub(crate) redirects: Redirects,
}

impl ClientTransport {
    /// What `config` declares. `timeout_ms` is required, as the client's declaration states.
    pub(crate) fn declared(
        config: &[ClientConfigEntry],
        connector: &'static str,
    ) -> ClientConfigResult<Self> {
        let contract = CreateClientHttp::TRANSPORT;
        let Some(timeout) = ConfiguredDeadline::timeout(config)? else {
            return Err(Report::new(ClientConfigError::MissingRequired {
                connector,
                key: contract.timeout.key().to_string(),
            }));
        };
        Ok(Self {
            timeout: timeout.bound(),
            redirects: Redirects::declared(config)?,
        })
    }
}

/// Whether a client follows redirects, as its `follow_redirects` setting declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Redirects {
    /// A redirect answer is the response to the request it answers.
    Refused,
    /// A request is repeated where a redirect answer moves it, at most `limit` times.
    Followed { limit: usize },
}

/// One answer to a request, and the request it answers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RedirectAnswer<'a> {
    /// Where the answered request was sent.
    pub(crate) url: &'a Url,
    /// The answered request's method, exactly as it was sent.
    pub(crate) method: &'a str,
    /// How many redirects the request had already followed when this answer arrived.
    pub(crate) followed: usize,
    pub(crate) status: u16,
    /// The answer's `Location`, when it carries exactly one.
    pub(crate) location: Option<&'a str>,
}

impl Redirects {
    /// What `config` declares: redirects are followed only when `follow_redirects` is `true`.
    pub(crate) fn declared(config: &[ClientConfigEntry]) -> ClientConfigResult<Self> {
        let contract = CreateClientHttp::TRANSPORT;
        let follows = optional_bool_client_config_value(config, FOLLOW_REDIRECTS)?;
        match follows {
            Some(true) => Ok(Self::Followed {
                limit: contract.redirect_limit_with_opt_in,
            }),
            Some(false) | None => Ok(Self::Refused),
        }
    }

    /// Where the request `answer` answers is sent next, when this client follows the answer: the
    /// same request, unchanged, at the URL its `Location` names. `None` makes the answer the
    /// response.
    pub(crate) fn next(&self, answer: &RedirectAnswer<'_>) -> Option<Url> {
        let Self::Followed { limit } = self else {
            return None;
        };
        if answer.followed >= *limit || !answer.repeats_request() {
            return None;
        }
        answer.location_on_origin()
    }
}

impl RedirectAnswer<'_> {
    /// Whether the answer asks for the request itself again, so repeating it unchanged is what the
    /// answer means. `301`, `302`, `307` and `308` move the request to their `Location`; HTTP lets a
    /// client turn a `301` or `302` to a `POST` into a `GET`, which this client never does. A `303`
    /// asks for a retrieval of its `Location`, which only a `GET` or `HEAD` already is; every other
    /// request it answered was already handled where it was sent.
    fn repeats_request(&self) -> bool {
        match self.status {
            301 | 302 | 307 | 308 => true,
            303 => {
                self.method.eq_ignore_ascii_case("GET") || self.method.eq_ignore_ascii_case("HEAD")
            }
            _ => false,
        }
    }

    /// The URL the answer's `Location` names, resolved against the answered request's URL, when it
    /// stays on that request's origin: the same scheme, host and port, with no user information
    /// and a path that cannot be read as an authority. A `Location`'s fragment never reaches a
    /// request, so it is dropped.
    fn location_on_origin(&self) -> Option<Url> {
        let location = self.location?;
        let Ok(mut next) = self.url.join(location) else {
            return None;
        };
        let on_origin = next.origin() == self.url.origin()
            && next.username().is_empty()
            && next.password().is_none()
            && !next.path().starts_with("//");
        if !on_origin {
            return None;
        }
        next.set_fragment(None);
        Some(next)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    const FOLLOWED: Redirects = Redirects::Followed { limit: 10 };

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn url(text: &str) -> Url {
        Url::parse(text).assured("the test URLs are valid")
    }

    fn answer<'a>(
        url: &'a Url,
        method: &'a str,
        status: u16,
        location: Option<&'a str>,
    ) -> RedirectAnswer<'a> {
        RedirectAnswer {
            url,
            method,
            followed: 0,
            status,
            location,
        }
    }

    #[test]
    fn a_client_declares_its_required_timeout_and_its_redirects() {
        let declared = ClientTransport::declared(
            &[
                entry("timeout_ms", "1500"),
                entry("follow_redirects", "true"),
            ],
            "HTTP",
        )
        .assured("a timeout and a boolean are valid");
        assert_eq!(
            declared,
            ClientTransport {
                timeout: Duration::from_millis(1500),
                redirects: Redirects::Followed { limit: 10 },
            }
        );
        let missing = ClientTransport::declared(&[entry("follow_redirects", "true")], "HTTP")
            .expect_err("timeout_ms is required");
        assert!(matches!(
            missing.current_context(),
            ClientConfigError::MissingRequired { connector: "HTTP", key } if key == "timeout_ms"
        ));
        let invalid = ClientTransport::declared(&[entry("timeout_ms", "soon")], "HTTP")
            .expect_err("a timeout is whole milliseconds");
        assert!(matches!(
            invalid.current_context(),
            ClientConfigError::InvalidDeadline { key: "timeout_ms" }
        ));
    }

    #[test]
    fn only_a_true_follow_redirects_follows_with_the_declared_limit() {
        assert_eq!(
            Redirects::declared(&[]).assured("an absent key is valid"),
            Redirects::Refused
        );
        assert_eq!(
            Redirects::declared(&[entry("follow_redirects", "false")])
                .assured("false is a boolean"),
            Redirects::Refused
        );
        assert_eq!(
            Redirects::declared(&[entry("follow_redirects", "TRUE")]).assured("TRUE is a boolean"),
            Redirects::Followed {
                limit: CreateClientHttp::TRANSPORT.redirect_limit_with_opt_in
            }
        );
        assert!(Redirects::declared(&[entry("follow_redirects", "yes")]).is_err());
    }

    #[test]
    fn a_refusing_client_follows_nothing() {
        let current = url("http://api.example.com/events/1");
        let moved = answer(&current, "POST", 307, Some("/moved"));
        assert_eq!(Redirects::Refused.next(&moved), None);
    }

    #[test]
    fn moving_answers_repeat_every_method_and_see_other_repeats_only_retrievals() {
        let current = url("https://api.example.com:8443/events/1?tenant=north");
        let moved = url("https://api.example.com:8443/moved?attempt=2");
        for status in [301, 302, 307, 308] {
            for method in ["POST", "PUT", "GET", "HEAD", "DELETE", "purge"] {
                assert_eq!(
                    FOLLOWED.next(&answer(&current, method, status, Some("/moved?attempt=2"))),
                    Some(moved.clone()),
                    "{status} moves a {method} request"
                );
            }
        }
        for method in ["GET", "HEAD", "get", "Head"] {
            assert_eq!(
                FOLLOWED.next(&answer(&current, method, 303, Some("/moved?attempt=2"))),
                Some(moved.clone()),
                "303 asks for a retrieval, which a {method} request is"
            );
        }
        for method in ["POST", "PUT", "PATCH", "DELETE", "purge"] {
            assert_eq!(
                FOLLOWED.next(&answer(&current, method, 303, Some("/moved"))),
                None,
                "a {method} request a 303 answers was already handled"
            );
        }
        for status in [300, 304, 305, 306, 200, 404, 503] {
            assert_eq!(
                FOLLOWED.next(&answer(&current, "GET", status, Some("/moved"))),
                None,
                "{status} moves no request"
            );
        }
    }

    #[test]
    fn a_location_is_resolved_against_the_answered_request_and_must_keep_its_origin() {
        let current = url("http://api.example.com/v1/events/1?tenant=north");
        let cases = [
            ("/moved", Some("http://api.example.com/moved")),
            ("retry", Some("http://api.example.com/v1/events/retry")),
            (
                "?tenant=south",
                Some("http://api.example.com/v1/events/1?tenant=south"),
            ),
            (
                "http://api.example.com:80/moved",
                Some("http://api.example.com/moved"),
            ),
            ("/moved#section", Some("http://api.example.com/moved")),
            ("https://api.example.com/moved", None),
            ("http://api.example.com:8080/moved", None),
            ("http://other.example.com/moved", None),
            ("//other.example.com/moved", None),
            ("http://user:secret@api.example.com/moved", None),
            ("/.//other.example.com/moved", None),
            ("http://[::1", None),
        ];
        for (location, expected) in cases {
            let expected = expected.map(url);
            assert_eq!(
                FOLLOWED.next(&answer(&current, "GET", 302, Some(location))),
                expected,
                "Location {location:?}"
            );
        }
        assert_eq!(FOLLOWED.next(&answer(&current, "GET", 302, None)), None);
    }

    #[test]
    fn one_request_follows_at_most_the_declared_limit() {
        let current = url("http://api.example.com/events/1");
        let mut redirected = answer(&current, "POST", 307, Some("/events/1"));
        redirected.followed = 9;
        assert_eq!(FOLLOWED.next(&redirected), Some(current.clone()));
        redirected.followed = 10;
        assert_eq!(FOLLOWED.next(&redirected), None);
    }
}
