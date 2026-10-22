//! Iceberg emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of Iceberg emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn iceberg_values<'src>()
-> impl Parser<'src, &'src [Token], Vec<IcebergValueMapping>, extra::Err<ParseError<'src>>> + Clone
{
    clickhouse_values()
}

pub(super) fn iceberg_catalog_parser<'src>()
-> impl Parser<'src, &'src [Token], IcebergCatalog, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Catalog)
        .ignore_then(client_ref())
        .map(|client| IcebergCatalog::Rest { client })
}

/// The Iceberg sink before its required commit cadence is attached.
pub(super) fn iceberg_sink_shape<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Iceberg)
        .ignore_then(kw(Identifier::On))
        .ignore_then(iceberg_storage_backend_parser().then(client_ref()))
        .then_ignore(kw(Identifier::Table))
        .then(table_ref())
        .then_ignore(kw(Identifier::Values))
        .then(iceberg_values())
        .then_ignore(kw(Identifier::Location))
        .then(string_lit().labelled("iceberg_location"))
        .then(iceberg_catalog_parser())
        .map(
            |(((((backend, client), table), values), location), catalog)| {
                EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
                    backend,
                    client,
                    table,
                    values,
                    location,
                    catalog,
                    commit_each: String::new(),
                    max_commit_size: String::new(),
                }))
            },
        )
}

/// `COMMIT EACH <duration> MAX SIZE <bytes>` is required by Iceberg and meaningless anywhere else,
/// so it is part of the Iceberg sink rather than a statement-level clause every sink is offered.
pub(super) fn iceberg_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    iceberg_sink_shape()
        .then(iceberg_commit_each())
        .map(|(sink, (commit_each, max_commit_size))| match sink {
            EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
                backend,
                client,
                table,
                values,
                location,
                catalog,
                ..
            })) => EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
                backend,
                client,
                table,
                values,
                location,
                catalog,
                commit_each,
                max_commit_size,
            })),
            other => other,
        })
        .boxed()
}

pub(super) fn iceberg_storage_backend_parser<'src>()
-> impl Parser<'src, &'src [Token], IcebergStorageBackend, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::S3).to(IcebergStorageBackend::S3),
        kw(Identifier::Gcs).to(IcebergStorageBackend::Gcs),
        kw(Identifier::AzureBlob).to(IcebergStorageBackend::AzureBlob),
    ))
}

pub(super) fn iceberg_commit_each<'src>()
-> impl Parser<'src, &'src [Token], (String, String), extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Commit, Identifier::Each)
        .ignore_then(duration_lit())
        .then_ignore(kw_phrase2(Identifier::Max, Identifier::Size))
        .then(byte_size_lit())
}
