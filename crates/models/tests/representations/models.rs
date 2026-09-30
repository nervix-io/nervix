//! Models and statements keep every field through their archived forms, and a stored Model's
//! resource versions survive widening into the versions a statement asks for.
//!
//! The domain is the vocabulary's: every Model family and statement form, including states NSPL
//! cannot spell, such as negative and non-finite literals, empty arrays and collection casts. A
//! stored Model binds concrete resource versions; a statement carries `LATEST` or a number.

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{Model, RequestedResourceVersion, Statement};

/// Pins every resource version `model` asks for to a number, as planning does before a Model is
/// stored. `LATEST` pins to zero, which is as much a version number as any other.
fn pinned(model: Model<RequestedResourceVersion>) -> Model {
    let pinned: Result<Model, std::convert::Infallible> =
        model.try_map_resource_versions(|_, version| match version {
            RequestedResourceVersion::Number(number) => Ok(number),
            RequestedResourceVersion::Latest => Ok(0),
        });
    match pinned {
        Ok(model) => model,
        Err(never) => match never {},
    }
}

#[test]
fn bolero_models_and_statements_round_trip_through_their_archives() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);

            let stored = pinned(arbitrary.model());
            assert_eq!(crate::archive_round_trip!(&stored, Model), stored);

            // A stored Model widens into the versions a statement asks for and pins back to itself.
            let requested = Model::<RequestedResourceVersion>::from(stored.clone());
            assert_eq!(pinned(requested), stored);

            let statement = arbitrary.statement();
            assert_eq!(crate::archive_round_trip!(&statement, Statement), statement);
        });
}
