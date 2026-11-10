//! Declared producer metadata through the production arrival-filter validator.
//!
//! Layer: test harness.
//! - **Owns.** Admission of declared identity metadata and refusal of undeclared fields.
//! - **Depends on.** Typed Models and the registry's schema validation.
//! - **Must not know.** Session transports, runtime scheduling or storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "metadata registry admission checks")
)]

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    BinaryOperator, ClientIngestorEntity, FieldReference, FieldScope, IngestorEntityKind, Literal,
};

use super::*;
use crate::registry::test_fixtures::named;

#[test]
fn client_arrival_filters_bind_only_the_metadata_the_producer_declares() {
    let ingestor = Arbitrary::new(&[7; 2048], Domain::Plexus)
        .create_ingestor_from(IngestorEntityKind::Client(ClientIngestorEntity::Producer));
    let input = CreateSchema {
        name: named("event"),
        fields: Vec::new(),
    };
    for (field, admitted) in [("user", true), ("missing", false)] {
        let predicate = Expression::Binary {
            operator: BinaryOperator::Equal,
            left: Box::new(Expression::Field(FieldReference::scoped(
                FieldScope::Metadata,
                named(field),
            ))),
            right: Box::new(Expression::Literal(Literal::String("alice".to_string()))),
        };
        let result = validate_ingestor_filter_where_for_internal_schemas(
            &named("default"),
            &named("source"),
            &ModelIndex::default(),
            &[(&named("input"), &input)],
            None,
            Some(&predicate),
            &ingestor.input,
        );
        assert_eq!(result.is_ok(), admitted, "metadata.{field}: {result:?}");
    }
}
