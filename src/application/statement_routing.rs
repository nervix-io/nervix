//! Pure routing requirements of semantic server statements.
//!
//! Layer: decisions.
//!
//! - **Owns.** Whether a statement needs a request domain, an existing domain, leadership or
//!   durable command admission.
//! - **Depends on.** Semantic statement Models from the vocabulary.
//! - **Must not know.** Parsing, sessions, consensus, runtime state or transports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        decisions,
        reason = "classifies statement routing from semantic Models only"
    )
)]

use nervix_models::Statement;

pub(in crate::application) fn requires_request_domain(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::AlterCluster(_)
            | Statement::CreateDomain(_)
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

pub(in crate::application) fn requires_existing_domain(statement: &Statement) -> bool {
    !matches!(
        statement,
        Statement::AlterCluster(_)
            | Statement::CreateDomain(_)
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

pub(in crate::application) fn requires_leader(statement: &Statement) -> bool {
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

pub(in crate::application) fn is_persistent_statement(statement: &Statement) -> bool {
    statement.is_model_mutation()
        || matches!(
            statement,
            Statement::AlterCluster(_)
                | Statement::CreateDomain(_)
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

    use std::time::Duration;

    use meticulous::ResultExt as _;
    use nervix_models::{
        AlterCluster, ClientName, ClusterParameterChange, ClusterParameterUpdates, DescribeClient,
        DescribeCluster,
    };

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

    #[test]
    fn parameter_changes_use_durable_cluster_admission() {
        let parameters = ClusterParameterUpdates::try_from(vec![
            ClusterParameterChange::NodeUnavailabilityTimeout(Duration::from_secs(15)),
        ])
        .assured("a positive timeout is a valid cluster parameter change");
        let statement = Statement::AlterCluster(AlterCluster { parameters });
        assert!(!requires_request_domain(&statement));
        assert!(!requires_existing_domain(&statement));
        assert!(requires_leader(&statement));
        assert!(is_persistent_statement(&statement));
    }
}
