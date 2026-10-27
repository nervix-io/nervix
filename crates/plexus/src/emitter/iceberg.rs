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

#[cfg(test)]
#[path = "iceberg_tests.rs"]
mod tests;
