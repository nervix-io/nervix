//! What opening an Iceberg emitter reports of the catalog table its statement names, driven
//! against Iceberg's in-memory catalog and against the answers a REST catalog gives the same
//! requests.

use ::iceberg::{
    CatalogBuilder as _, Error as IcebergError, MemoryCatalog, Namespace, Result as IcebergResult,
    TableCommit, TableCreation,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use error_stack::{AttachmentKind, FrameKind};
use meticulous::OptionExt as _;

use super::*;

const NAMESPACE: &str = "Analytics.Events";
const TABLE: &str = "orders";

/// The catalog names an emitter's statement declares.
struct DeclaredTable {
    namespace: IcebergNamespace,
    table: IcebergTableName,
}

impl DeclaredTable {
    fn new() -> Self {
        Self {
            namespace: IcebergNamespace::parse(NAMESPACE)
                .assured("the fixture namespace is a valid catalog namespace"),
            table: IcebergTableName::parse(TABLE)
                .assured("the fixture table is a valid catalog table name"),
        }
    }

    fn catalog_table(&self) -> IcebergCatalogTable<'_> {
        IcebergCatalogTable {
            namespace: &self.namespace,
            table: &self.table,
        }
    }

    /// Opens the declared table from `catalog`, which the fixture does not let succeed.
    async fn refused_by(&self, catalog: &AnsweringCatalog) -> Report<EmitterOpenError> {
        let opened = self.catalog_table().load(catalog).await;
        let Err(report) = opened else {
            panic!(
                "the {:?} catalog does not hold the declared table",
                catalog.answers
            );
        };
        report
    }
}

/// How the fixture catalog answers the two requests an open makes.
#[derive(Debug, Clone, Copy)]
enum CatalogAnswers {
    /// The in-memory catalog's own answers, whose load names a missing namespace.
    InMemory,
    /// A REST catalog's answers: its load reports every table it does not hold as
    /// `TableNotFound`, whether or not the table's namespace exists.
    Rest,
    /// A catalog that fails the load.
    LoadOutage,
    /// A REST catalog that does not hold the table, and then fails the namespace lookup.
    NamespaceLookupOutage,
}

/// Iceberg's in-memory catalog behind the answers the fixture gives an open.
#[derive(Debug)]
struct AnsweringCatalog {
    inner: MemoryCatalog,
    answers: CatalogAnswers,
}

impl AnsweringCatalog {
    async fn new(answers: CatalogAnswers) -> Self {
        let inner = MemoryCatalogBuilder::default()
            .load(
                "memory",
                [(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://warehouse".to_string(),
                )]
                .into_iter()
                .collect(),
            )
            .await
            .assured("the in-memory catalog takes a warehouse location");
        Self { inner, answers }
    }

    /// Provisions the declared namespace level by level, as an operator does: the in-memory
    /// catalog creates a namespace only inside a parent it already holds.
    async fn provision_namespace(&self, declared: &DeclaredTable) {
        let levels: Vec<&str> = declared.namespace.levels().collect();
        for depth in 1..=levels.len() {
            let namespace = NamespaceIdent::from_strs(&levels[..depth])
                .assured("every prefix of a validated namespace has a nonempty level");
            self.inner
                .create_namespace(&namespace, Default::default())
                .await
                .assured("the in-memory catalog creates a namespace inside a parent it holds");
        }
    }

    /// Provisions the declared namespace and table, as an operator does.
    async fn provision_table(&self, declared: &DeclaredTable) {
        self.provision_namespace(declared).await;
        let ident = declared.catalog_table().ident();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "user_id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .assured("the fixture schema is valid");
        let creation = TableCreation::builder()
            .name(TABLE.to_string())
            .schema(schema)
            .build();
        self.inner
            .create_table(ident.namespace(), creation)
            .await
            .assured("the in-memory catalog creates a table in a namespace it holds");
    }

    /// The error a REST catalog's client reports for a table load the catalog answered with
    /// `404 Not Found`, which it gives whether or not the table's namespace exists.
    fn rest_not_found(error: IcebergError) -> IcebergError {
        match error.kind() {
            IcebergErrorKind::NamespaceNotFound | IcebergErrorKind::TableNotFound => {
                IcebergError::new(
                    IcebergErrorKind::TableNotFound,
                    "Tried to load a table that does not exist",
                )
            }
            _ => error,
        }
    }
}

#[async_trait::async_trait]
impl Catalog for AnsweringCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    #[expect(
        clippy::disallowed_types,
        reason = "iceberg's Catalog trait fixes namespace properties to the standard library's map"
    )]
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        match self.answers {
            CatalogAnswers::NamespaceLookupOutage => Err(IcebergError::new(
                IcebergErrorKind::Unexpected,
                "the fixture catalog fails its namespace lookup",
            )),
            CatalogAnswers::InMemory | CatalogAnswers::Rest | CatalogAnswers::LoadOutage => {
                self.inner.get_namespace(namespace).await
            }
        }
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }

    #[expect(
        clippy::disallowed_types,
        reason = "iceberg's Catalog trait fixes namespace properties to the standard library's map"
    )]
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        match self.answers {
            CatalogAnswers::InMemory => self.inner.load_table(table).await,
            CatalogAnswers::Rest | CatalogAnswers::NamespaceLookupOutage => self
                .inner
                .load_table(table)
                .await
                .map_err(Self::rest_not_found),
            CatalogAnswers::LoadOutage => Err(IcebergError::new(
                IcebergErrorKind::Unexpected,
                "the fixture catalog fails its table load",
            )),
        }
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await
    }
}

/// The description the emitter shows as its transient error: the first printable attachment of
/// its open failure.
fn description(report: &Report<EmitterOpenError>) -> String {
    for frame in report.frames() {
        if let FrameKind::Attachment(AttachmentKind::Printable(attachment)) = frame.kind() {
            return attachment.to_string();
        }
    }
    panic!("an Iceberg open failure describes itself in a printable attachment: {report:?}");
}

#[nervix_primitives::test]
async fn a_catalog_without_the_namespace_reports_the_namespace_as_the_missing_entity() {
    let declared = DeclaredTable::new();
    for answers in [CatalogAnswers::InMemory, CatalogAnswers::Rest] {
        let catalog = AnsweringCatalog::new(answers).await;

        let report = declared.refused_by(&catalog).await;

        assert_eq!(
            report.current_context(),
            &EmitterOpenError::MissingExternalEntity {
                connector: ICEBERG,
                kind: "namespace",
                name: NAMESPACE.to_string(),
            },
            "{answers:?}"
        );
        assert_eq!(
            report.downcast_ref::<IcebergTableOpenError>(),
            Some(&IcebergTableOpenError::MissingNamespace {
                namespace: declared.namespace.clone(),
            }),
            "{answers:?}"
        );
        assert_eq!(
            description(&report),
            "ICEBERG namespace 'Analytics.Events' does not exist",
            "{answers:?}"
        );
    }
}

#[nervix_primitives::test]
async fn a_catalog_holding_the_namespace_without_the_table_reports_the_table_as_the_missing_entity()
{
    let declared = DeclaredTable::new();
    for answers in [CatalogAnswers::InMemory, CatalogAnswers::Rest] {
        let catalog = AnsweringCatalog::new(answers).await;
        catalog.provision_namespace(&declared).await;

        let report = declared.refused_by(&catalog).await;

        assert_eq!(
            report.current_context(),
            &EmitterOpenError::MissingExternalEntity {
                connector: ICEBERG,
                kind: "table",
                name: "Analytics.Events.orders".to_string(),
            },
            "{answers:?}"
        );
        assert_eq!(
            report.downcast_ref::<IcebergTableOpenError>(),
            Some(&IcebergTableOpenError::MissingTable {
                namespace: declared.namespace.clone(),
                table: declared.table.clone(),
            }),
            "{answers:?}"
        );
        assert_eq!(
            description(&report),
            "ICEBERG table 'orders' does not exist in namespace 'Analytics.Events'",
            "{answers:?}"
        );
    }
}

#[nervix_primitives::test]
async fn a_provisioned_table_loads() {
    let declared = DeclaredTable::new();
    let catalog = AnsweringCatalog::new(CatalogAnswers::Rest).await;
    catalog.provision_table(&declared).await;

    let loaded = declared
        .catalog_table()
        .load(&catalog)
        .await
        .assured("the catalog holds the provisioned table");

    assert_eq!(loaded.identifier(), &declared.catalog_table().ident());
}

#[nervix_primitives::test]
async fn a_catalog_that_fails_the_load_leaves_the_emitter_uninitialized_with_its_error_beneath() {
    let declared = DeclaredTable::new();
    let catalog = AnsweringCatalog::new(CatalogAnswers::LoadOutage).await;

    let report = declared.refused_by(&catalog).await;

    assert_eq!(
        report.current_context(),
        &EmitterOpenError::Initialize { connector: ICEBERG }
    );
    assert_eq!(
        report.downcast_ref::<IcebergTableOpenError>(),
        Some(&IcebergTableOpenError::Load {
            namespace: declared.namespace.clone(),
            table: declared.table.clone(),
        })
    );
    let cause = report
        .downcast_ref::<IcebergError>()
        .assured("the catalog's error stays beneath the failed load");
    assert_eq!(cause.kind(), IcebergErrorKind::Unexpected);
    let description = description(&report);
    assert!(
        description
            .starts_with("failed to load ICEBERG table 'orders' in namespace 'Analytics.Events': "),
        "{description}"
    );
    assert!(
        description.contains("the fixture catalog fails its table load"),
        "{description}"
    );
}

#[nervix_primitives::test]
async fn a_catalog_that_fails_the_namespace_lookup_leaves_the_emitter_uninitialized_with_its_error_beneath()
 {
    let declared = DeclaredTable::new();
    let catalog = AnsweringCatalog::new(CatalogAnswers::NamespaceLookupOutage).await;

    let report = declared.refused_by(&catalog).await;

    assert_eq!(
        report.current_context(),
        &EmitterOpenError::Initialize { connector: ICEBERG }
    );
    assert_eq!(
        report.downcast_ref::<IcebergTableOpenError>(),
        Some(&IcebergTableOpenError::NamespaceLookup {
            namespace: declared.namespace.clone(),
        })
    );
    let cause = report
        .downcast_ref::<IcebergError>()
        .assured("the catalog's error stays beneath the failed lookup");
    assert_eq!(cause.kind(), IcebergErrorKind::Unexpected);
    let description = description(&report);
    assert!(
        description.starts_with("failed to look up ICEBERG namespace 'Analytics.Events': "),
        "{description}"
    );
    assert!(
        description.contains("the fixture catalog fails its namespace lookup"),
        "{description}"
    );
}
