use url::Url;

pub(in crate::runtime) struct ServiceUrl<'a> {
    raw: &'a str,
    label: &'static str,
}

impl<'a> ServiceUrl<'a> {
    pub(in crate::runtime) fn new(raw: &'a str, label: &'static str) -> Self {
        Self { raw, label }
    }

    pub(in crate::runtime) fn scheme(&self) -> Result<String, String> {
        let url = Url::parse(self.raw)
            .map_err(|source| format!("invalid {} '{}': {source}", self.label, self.raw))?;
        Ok(url.scheme().to_string())
    }

    pub(in crate::runtime) fn has_scheme(&self, expected_scheme: &str) -> Result<bool, String> {
        Ok(self.scheme()? == expected_scheme)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_scheme_detection_uses_url_parser() {
        assert!(
            ServiceUrl::new(
                "amqps://guest:guest@[2001:db8::1]:5671/%2f?heartbeat=30",
                "RabbitMQ addr"
            )
            .has_scheme("amqps")
            .expect("must parse")
        );
        assert!(
            ServiceUrl::new("rediss://127.0.0.1:6380/?protocol=resp3", "Redis addr")
                .has_scheme("rediss")
                .expect("must parse")
        );
        assert!(
            ServiceUrl::new("tls://127.0.0.1:4223?name=nervix", "NATS addr")
                .has_scheme("tls")
                .expect("must parse")
        );
        assert!(
            !ServiceUrl::new("amqp://guest:guest@127.0.0.1:5672/%2f", "RabbitMQ addr")
                .has_scheme("amqps")
                .expect("must parse")
        );
        assert_eq!(
            ServiceUrl::new("wss://example.com/socket?token=abc", "WebSockets endpoint")
                .scheme()
                .expect("must parse"),
            "wss"
        );
        assert!(
            ServiceUrl::new("not a url", "RabbitMQ addr")
                .has_scheme("amqps")
                .is_err()
        );
    }
}
