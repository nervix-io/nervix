//! Admission policy for typed session statements.
//!
//! Layer: decisions.
//! - **Owns.** Domain, leader and persistence requirements for a semantic statement.
//! - **Depends on.** The statement vocabulary and its mutation classification.
//! - **Must not know.** Sessions, registry state, consensus, transports or execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns pure statement admission policy")
)]

use nervix_models::Statement;

pub(super) fn requires_request_domain(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::CreateDomain(_)
            | Statement::CreateUser(_)
            | Statement::Backup(_)
            | Statement::Restore(_)
            | Statement::StopDomain(_)
            | Statement::ShowClusterStatus(_)
            | Statement::DescribeCluster(_)
            | Statement::ShowTransactions(_)
            | Statement::DescribeTransaction(_)
            | Statement::DropNode(_)
            | Statement::CordonNode(_)
            | Statement::UncordonNode(_)
            | Statement::DrainNode(_)
    )
}

pub(super) fn requires_existing_domain(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::CreateDomain(_)
            | Statement::CreateUser(_)
            | Statement::Backup(_)
            | Statement::Restore(_)
            | Statement::StopDomain(_)
            | Statement::ShowClusterStatus(_)
            | Statement::DescribeCluster(_)
            | Statement::ShowTransactions(_)
            | Statement::DescribeTransaction(_)
            | Statement::DropNode(_)
            | Statement::CordonNode(_)
            | Statement::UncordonNode(_)
            | Statement::DrainNode(_)
    )
}

pub(super) fn requires_leader(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::ShowClusterStatus(_)
            | Statement::DescribeCluster(_)
            | Statement::ShowTransactions(_)
            | Statement::DescribeResource(_)
            | Statement::DescribeDomain(_)
            | Statement::DescribeEndpoint(_)
            | Statement::DescribeClient(_)
            | Statement::DescribeIngestor(_)
            | Statement::DescribeRelay(_)
            | Statement::DescribeLookup(_)
            | Statement::DescribeJunction(_)
            | Statement::DescribeDeduplicator(_)
            | Statement::DescribeReingestor(_)
            | Statement::DescribeCorrelator(_)
            | Statement::DescribeReorderer(_)
            | Statement::DescribeEmitter(_)
            | Statement::DescribeUdf(_)
            | Statement::DescribeWasmProcessor(_)
            | Statement::DescribeWindowProcessor(_)
            | Statement::DescribePlacement(_)
            | Statement::DescribeRelocation(_)
            | Statement::LookupQuery(_)
            | Statement::ShowCreate(_)
            | Statement::ShowUdfs(_)
            | Statement::ShowIngestors(_)
            | Statement::ShowEmitters(_)
            | Statement::ShowPlacements(_)
            | Statement::ShowRelayMaterializedState(_)
    )
}

pub(super) fn is_persistent_statement(statement: &Statement) -> bool {
    statement.is_model_mutation()
        || matches!(
            statement,
            Statement::CreateDomain(_)
                | Statement::AlterDomain(_)
                | Statement::CreateUser(_)
                | Statement::CreateResource(_)
                | Statement::StartDomain(_)
                | Statement::StopDomain(_)
                | Statement::DropNode(_)
                | Statement::CordonNode(_)
                | Statement::UncordonNode(_)
                | Statement::DrainNode(_)
                | Statement::Relocate(_)
                | Statement::ResetWasmState(_)
                | Statement::Backup(_)
        )
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns direct statement admission policy checks")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Typed admission contracts for domain and cluster inspection.
    //! - **Depends on.** The routing decisions and semantic statement constructors.
    //! - **Must not know.** Sessions, storage or runtime execution.

    use meticulous::ResultExt as _;
    use nervix_models::{ClientName, DescribeClient, DescribeCluster};

    use super::*;

    #[test]
    fn configured_client_inspection_is_a_domain_read() {
        let statement = Statement::DescribeClient(DescribeClient {
            name: ClientName::parse("warehouse").assured("warehouse is a valid client name"),
        });
        assert!(requires_request_domain(&statement));
        assert!(requires_existing_domain(&statement));
        assert!(!requires_leader(&statement));
        assert!(!is_persistent_statement(&statement));
    }

    #[test]
    fn cluster_inspection_needs_no_domain_or_mutation() {
        let statement = Statement::DescribeCluster(DescribeCluster);
        assert!(!requires_request_domain(&statement));
        assert!(!requires_existing_domain(&statement));
        assert!(!requires_leader(&statement));
        assert!(!is_persistent_statement(&statement));
    }
}
