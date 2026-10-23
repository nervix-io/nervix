//! CLICKHOUSE emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of CLICKHOUSE emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], ClickHouseTablePublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(ClickHouseTablePublishingMode)
}
pub(super) fn table_body<'src>()
-> impl Parser<'src, &'src [Token], ParsedDestination<ClickHouseTableSink>, extra::Err<ParseError<'src>>>
+ Clone {
    values_destination(
        client_ref()
            .then_ignore(kw(Identifier::Table))
            .then(external_name::<ClickHouseTableName>())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .then_ignore(kw(Identifier::Values))
            .then(clickhouse_values())
            .map(|(((client, table), mode), values)| ClickHouseTableSink {
                client,
                table,
                mode,
                values,
            }),
    )
}
