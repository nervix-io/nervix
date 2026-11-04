//! ICEBERG TABLE emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of ICEBERG TABLE emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], IcebergTablePublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(IcebergTablePublishingMode)
}
pub(super) fn commit_each<'src>()
-> impl Parser<'src, &'src [Token], (String, PayloadSizeLimit), extra::Err<ParseError<'src>>> + Clone
{
    kw_phrase2(Identifier::Commit, Identifier::Each)
        .ignore_then(duration_lit())
        .then_ignore(kw_phrase2(Identifier::Max, Identifier::Size))
        .then(payload_size_lit("COMMIT MAX SIZE"))
}
pub(super) fn table_body<'src>()
-> impl Parser<'src, &'src [Token], ParsedDestination<IcebergTableEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    values_destination(
        client_ref()
            .then_ignore(kw(Identifier::Table))
            .then(external_name::<IcebergTableName>())
            .then_ignore(kw(Identifier::Namespace))
            .then(external_name::<IcebergNamespace>())
            .then_ignore(kw(Identifier::Location))
            .then(string_lit().labelled("iceberg_location"))
            .then_ignore(kw(Identifier::Catalog))
            .then(client_ref())
            .then(commit_each())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .then_ignore(kw(Identifier::Values))
            .then(clickhouse_values())
            .map(
                |(
                    (
                        (
                            ((((client, table), namespace), location), catalog),
                            (commit_each, max_commit_size),
                        ),
                        mode,
                    ),
                    values,
                )| IcebergTableEmitter {
                    client,
                    table,
                    namespace,
                    location,
                    catalog: IcebergCatalog::Rest { client: catalog },
                    commit_each,
                    max_commit_size,
                    mode,
                    values,
                },
            ),
    )
}

/// `SET TABLE`, `SET NAMESPACE`, `SET LOCATION`, `SET CATALOG`, `SET COMMIT EACH` or `SET MODE`:
/// one part of an `ICEBERG TABLE` body.
pub(super) fn change<'src>()
-> impl Parser<'src, &'src [Token], IcebergTableEmitterChange, extra::Err<ParseError<'src>>> + Clone
{
    let table = kw(Identifier::Table)
        .ignore_then(external_name::<IcebergTableName>())
        .map(IcebergTableEmitterChange::Table);
    let namespace = kw(Identifier::Namespace)
        .ignore_then(external_name::<IcebergNamespace>())
        .map(IcebergTableEmitterChange::Namespace);
    let location = kw(Identifier::Location)
        .ignore_then(string_lit().labelled("iceberg_location"))
        .map(IcebergTableEmitterChange::Location);
    let catalog = kw(Identifier::Catalog)
        .ignore_then(client_ref())
        .map(|client| IcebergTableEmitterChange::Catalog(IcebergCatalog::Rest { client }));
    let commit =
        commit_each().map(
            |(commit_each, max_commit_size)| IcebergTableEmitterChange::CommitEach {
                commit_each,
                max_commit_size,
            },
        );
    let publishing = kw(Identifier::Mode)
        .ignore_then(mode())
        .map(IcebergTableEmitterChange::Mode);
    kw(Identifier::Set)
        .ignore_then(choice((
            table, namespace, location, catalog, commit, publishing,
        )))
        .boxed()
}

#[cfg(test)]
#[path = "iceberg_tests.rs"]
mod tests;
