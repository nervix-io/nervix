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
