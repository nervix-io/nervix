//! The catalog table an Iceberg emitter opens, and what a catalog that does not hold it lacks.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Loading the table an emitter's statement names from its catalog, and classifying a
//!   catalog that holds no namespace of that name, or no table of that name in a namespace it
//!   holds, as the missing external entity an operator provisions. Any other catalog failure leaves
//!   the emitter uninitialized, with the catalog's error beneath.
//! - **Depends on.** The `iceberg` crate's catalog interface and errors, the connector contract's
//!   open failures, the vocabulary's catalog names, and `error-stack`.
//! - **Must not know.** Staging, commits, acknowledgements, the host, or when the host opens the
//!   emitter again.
//!
//! Nervix creates nothing in a catalog. A REST catalog answers the load of a table whose namespace
//! does not exist exactly as it answers the load of a table that does not exist, with
//! `404 Not Found`, so a table the catalog does not hold is told apart by asking the catalog for its
//! namespace.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the emitter opens the table its validated plan names")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use ::iceberg::{Catalog, ErrorKind as IcebergErrorKind, NamespaceIdent, TableIdent, table::Table};
use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_connector::{EmitterOpenError, EmitterStartResult};
use nervix_models::{ConnectorKind, IcebergNamespace, IcebergTableName};

use super::ICEBERG;

/// The table an Iceberg emitter writes, as its statement names it.
pub(super) struct IcebergCatalogTable<'a> {
    pub(super) namespace: &'a IcebergNamespace,
    pub(super) table: &'a IcebergTableName,
}

impl IcebergCatalogTable<'_> {
    /// The catalog's identifier of the table.
    pub(super) fn ident(&self) -> TableIdent {
        let namespace = NamespaceIdent::from_strs(self.namespace.levels())
            .assured("a validated Iceberg namespace has at least one nonempty level");
        TableIdent::new(namespace, self.table.as_str().to_string())
    }

    /// Loads the table from `catalog`.
    ///
    /// A namespace or table the catalog does not hold is the missing external entity an operator
    /// provisions, and the host opens the emitter again until it exists. Any other catalog failure
    /// leaves the emitter uninitialized, with the catalog's error beneath.
    pub(super) async fn load(&self, catalog: &dyn Catalog) -> EmitterStartResult<Table> {
        let ident = self.ident();
        let error = match catalog.load_table(&ident).await {
            Ok(table) => return Ok(table),
            Err(error) => error,
        };
        let failure = match error.kind() {
            IcebergErrorKind::TableNotFound | IcebergErrorKind::NamespaceNotFound => {
                self.missing_entity(catalog, ident.namespace()).await
            }
            _ => Report::new(error).change_context(IcebergTableOpenError::Load {
                namespace: self.namespace.clone(),
                table: self.table.clone(),
            }),
        };
        Err(IcebergTableOpenError::open_failure(failure))
    }

    /// What a catalog that answered it does not hold the table lacks: the table's namespace, or the
    /// table in a namespace it holds.
    async fn missing_entity(
        &self,
        catalog: &dyn Catalog,
        namespace: &NamespaceIdent,
    ) -> Report<IcebergTableOpenError> {
        match catalog.get_namespace(namespace).await {
            Ok(_) => Report::new(IcebergTableOpenError::MissingTable {
                namespace: self.namespace.clone(),
                table: self.table.clone(),
            }),
            Err(error) if error.kind() == IcebergErrorKind::NamespaceNotFound => {
                Report::new(IcebergTableOpenError::MissingNamespace {
                    namespace: self.namespace.clone(),
                })
            }
            Err(error) => {
                Report::new(error).change_context(IcebergTableOpenError::NamespaceLookup {
                    namespace: self.namespace.clone(),
                })
            }
        }
    }
}

/// Why an Iceberg emitter could not open the catalog table it writes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum IcebergTableOpenError {
    /// The catalog holds no namespace of this name, so it holds no table in it either.
    #[error("{connector} namespace '{namespace}' does not exist", connector = ConnectorKind::Iceberg)]
    MissingNamespace { namespace: IcebergNamespace },
    /// The catalog holds the namespace, and no table of this name in it.
    #[error(
        "{connector} table '{table}' does not exist in namespace '{namespace}'",
        connector = ConnectorKind::Iceberg
    )]
    MissingTable {
        namespace: IcebergNamespace,
        table: IcebergTableName,
    },
    /// The catalog failed to load the table. Its error is beneath.
    #[error(
        "failed to load {connector} table '{table}' in namespace '{namespace}'",
        connector = ConnectorKind::Iceberg
    )]
    Load {
        namespace: IcebergNamespace,
        table: IcebergTableName,
    },
    /// The catalog answered that it does not hold the table, and then failed to answer whether it
    /// holds the table's namespace. Its error is beneath.
    #[error(
        "failed to look up {connector} namespace '{namespace}'",
        connector = ConnectorKind::Iceberg
    )]
    NamespaceLookup { namespace: IcebergNamespace },
}

impl IcebergTableOpenError {
    /// The connector contract's open failure this is. An entity the catalog does not hold is
    /// missing until an operator provisions it; a catalog that failed to answer leaves the emitter
    /// uninitialized.
    fn contract_failure(&self) -> EmitterOpenError {
        match self {
            Self::MissingNamespace { namespace } => EmitterOpenError::MissingExternalEntity {
                connector: ICEBERG,
                kind: "namespace",
                name: namespace.to_string(),
            },
            Self::MissingTable { namespace, table } => EmitterOpenError::MissingExternalEntity {
                connector: ICEBERG,
                kind: "table",
                name: format!("{namespace}.{table}"),
            },
            Self::Load { .. } | Self::NamespaceLookup { .. } => {
                EmitterOpenError::Initialize { connector: ICEBERG }
            }
        }
    }

    /// `report` as the emitter's open failure: the contract's failure above it, and its own chain
    /// as the description the emitter shows as its transient error.
    fn open_failure(report: Report<Self>) -> Report<EmitterOpenError> {
        let failure = report.current_context().contract_failure();
        let description = format!("{report:#}");
        report.change_context(failure).attach_printable(description)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "catalog_table_tests.rs"]
mod tests;
