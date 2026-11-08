//! The actual AWS-LC backend status, independent of TLS algorithm selection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Reporting whether the linked AWS-LC module runs in FIPS mode.
//! - **Depends on.** AWS-LC's runtime module-status query.
//! - **Must not know.** Service policy, Models, graph state or deployment qualification.

/// Backend mode reported by AWS-LC, not a claim about a deployment's validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModuleMode {
    Ordinary,
    Fips,
}

/// Query the linked backend, including when an embedding service enables AWS-LC's FIPS feature.
#[must_use]
pub fn module_mode() -> ModuleMode {
    match aws_lc_rs::try_fips_mode() {
        Ok(()) => ModuleMode::Fips,
        Err(_) => ModuleMode::Ordinary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_matches_the_linked_module() {
        assert_eq!(
            module_mode() == ModuleMode::Fips,
            aws_lc_rs::try_fips_mode().is_ok()
        );
    }
}
