//! Composed top-level NSPL statement grammar.
//!
//! Layer: language.
//! - **Owns.** Statement-family composition, complete-input parsing and completion expectations.
//! - **Depends on.** Statement grammars, shared tokens and vocabulary models.
//! - **Must not know.** Validation, persistence or runtime execution.

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{Model, Statement};

use crate::{
    lexer::Token,
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, boxed_choice, completion_context,
        completion_tokens, filter_by_prefix, into_parse_error, lex_input, suggestions_from_errors,
    },
};

pub fn statement_parser<'src>()
-> impl Parser<'src, &'src [Token], Statement, extra::Err<ParseError<'src>>> + Clone {
    let domain_and_core = boxed_choice!(
        crate::domain::create_domain_parser().map(Statement::CreateDomain),
        crate::domain::alter_domain_parser().map(Statement::AlterDomain),
        crate::create_resource::create_resource_parser().map(Statement::CreateResource),
        crate::domain::start_domain_parser().map(Statement::StartDomain),
        crate::domain::stop_domain_parser().map(Statement::StopDomain),
        crate::branch::create_branch_parser()
            .map(|create| Statement::Create(create.map_body(Model::Branch).map_body(Box::new))),
        crate::placement::create_placement_parser()
            .map(|create| Statement::Create(create.map_body(Model::Placement).map_body(Box::new))),
        crate::describe_deduplicator::describe_deduplicator_parser()
            .map(Statement::DescribeDeduplicator),
        crate::describe_domain::describe_domain_parser().map(Statement::DescribeDomain),
        crate::describe_endpoint::describe_endpoint_parser().map(Statement::DescribeEndpoint),
        crate::describe_ingestor::describe_ingestor_parser().map(Statement::DescribeIngestor),
        crate::describe_junction::describe_junction_parser().map(Statement::DescribeJunction),
        crate::describe_lookup::describe_lookup_parser().map(Statement::DescribeLookup),
        crate::placement::describe_placement_parser().map(Statement::DescribePlacement),
        crate::describe_resource::describe_resource_parser().map(Statement::DescribeResource),
        crate::describe_stream::describe_stream_parser().map(Statement::DescribeRelay),
        crate::describe_window_processor::describe_window_processor_parser()
            .map(Statement::DescribeWindowProcessor),
        crate::lookup_query::lookup_query_parser().map(Statement::LookupQuery),
        crate::generator::create_generator_parser()
            .map(|create| Statement::Create(create.map_body(Model::Generator).map_body(Box::new))),
        crate::inferencer::create_inferencer_parser()
            .map(|create| Statement::Create(create.map_body(Model::Inferencer).map_body(Box::new))),
        crate::lookup::create_lookup_parser()
            .map(|create| Statement::Create(create.map_body(Model::Lookup).map_body(Box::new))),
        crate::reingestor::create_reingestor_parser()
            .map(|create| Statement::Create(create.map_body(Model::Reingestor).map_body(Box::new))),
        crate::reorderer::create_reorderer_parser()
            .map(|create| Statement::Create(create.map_body(Model::Reorderer).map_body(Box::new))),
        crate::codec::create_codec_parser()
            .map(|create| Statement::Create(create.map_body(Model::Codec).map_body(Box::new))),
        crate::junction::create_junction_parser()
            .map(|create| Statement::Create(create.map_body(Model::Junction).map_body(Box::new))),
        crate::deduplicator::create_deduplicator_parser().map(|create| {
            Statement::Create(create.map_body(Model::Deduplicator).map_body(Box::new))
        }),
        crate::window_processor::create_window_processor_parser().map(|create| {
            Statement::Create(create.map_body(Model::WindowProcessor).map_body(Box::new))
        }),
        crate::vhost::create_vhost_parser()
            .map(|create| Statement::Create(create.map_body(Model::Vhost).map_body(Box::new))),
        crate::udf::create_udf_parser()
            .map(|create| Statement::Create(create.map_body(Model::Udf).map_body(Box::new))),
    );
    let processing_and_io = boxed_choice!(
        crate::user::create_user_parser().map(Statement::CreateUser),
        crate::describe_correlator::describe_correlator_parser().map(Statement::DescribeCorrelator),
        crate::endpoint::create_endpoint_parser()
            .map(|create| Statement::Create(create.map_body(Model::Endpoint).map_body(Box::new))),
        crate::signaling_protocol::create_signaling_protocol_parser().map(|create| {
            Statement::Create(create.map_body(Model::SignalingProtocol).map_body(Box::new))
        }),
        crate::describe_emitter::describe_emitter_parser().map(Statement::DescribeEmitter),
        crate::describe_wasm_processor::describe_wasm_processor_parser()
            .map(Statement::DescribeWasmProcessor),
        crate::wasm_processor::create_wasm_processor_parser().map(|create| {
            Statement::Create(create.map_body(Model::WasmProcessor).map_body(Box::new))
        }),
        crate::describe_reingestor::describe_reingestor_parser().map(Statement::DescribeReingestor),
        crate::describe_reorderer::describe_reorderer_parser().map(Statement::DescribeReorderer),
        crate::correlator::create_correlator_parser()
            .map(|create| Statement::Create(create.map_body(Model::Correlator).map_body(Box::new))),
        crate::emitter::create_emitter_parser()
            .map(|create| Statement::Create(create.map_body(Model::Emitter).map_body(Box::new))),
        crate::ingestor::create_ingestor_parser()
            .map(|create| Statement::Create(create.map_body(Model::Ingestor).map_body(Box::new))),
        crate::relay::create_relay_parser()
            .map(|create| Statement::Create(create.map_body(Model::Relay).map_body(Box::new))),
        crate::schema::create_json_wire_schema_parser().map(|create| {
            Statement::Create(create.map_body(Model::WireJsonSchema).map_body(Box::new))
        }),
        crate::schema::create_cbor_wire_schema_parser().map(|create| {
            Statement::Create(create.map_body(Model::WireCborSchema).map_body(Box::new))
        }),
        crate::schema::create_avro_wire_schema_parser().map(|create| {
            Statement::Create(create.map_body(Model::WireAvroSchema).map_body(Box::new))
        }),
        crate::schema::create_schema_parser()
            .map(|create| Statement::Create(create.map_body(Model::Schema).map_body(Box::new))),
    );
    let clients = crate::client::create_client_model_parser()
        .map(Statement::Create)
        .boxed();
    let administration = boxed_choice!(
        crate::placement::alter_placement_parser().map(Statement::AlterPlacement),
        crate::schema::alter_schema_parser().map(Statement::AlterSchema),
        crate::schema::alter_wire_schema_parser_any(),
        crate::relay::alter_relay_parser().map(Statement::AlterRelay),
        crate::junction::alter_junction_parser().map(Statement::AlterJunction),
        crate::deduplicator::alter_deduplicator_parser().map(Statement::AlterDeduplicator),
        crate::reorderer::alter_reorderer_parser().map(Statement::AlterReorderer),
        crate::emitter::alter_emitter_parser().map(Statement::AlterEmitter),
        crate::ingestor::alter_ingestor_parser().map(Statement::AlterIngestor),
        crate::reingestor::alter_reingestor_parser().map(Statement::AlterReingestor),
        crate::generator::alter_generator_parser().map(Statement::AlterGenerator),
        crate::node_control::cordon_node_parser().map(Statement::CordonNode),
        crate::node_control::uncordon_node_parser().map(Statement::UncordonNode),
        crate::node_control::drain_node_parser().map(Statement::DrainNode),
        crate::relocation::relocate_parser().map(Statement::Relocate),
        crate::rebind_resource::rebind_resource_parser().map(Statement::RebindResource),
        crate::reset_wasm_state::reset_wasm_state_parser().map(Statement::ResetWasmState),
        crate::relocation::describe_relocation_parser().map(Statement::DescribeRelocation),
        crate::drop_stmt::drop_node_parser().map(Statement::DropNode),
        crate::drop_stmt::drop_parser().map(Statement::Drop),
        crate::show_cluster_status::show_cluster_status_parser().map(Statement::ShowClusterStatus),
        crate::show_transactions::show_transactions_parser().map(Statement::ShowTransactions),
        crate::describe_transaction::describe_transaction_parser()
            .map(Statement::DescribeTransaction),
        crate::show_create::show_create_parser().map(Statement::ShowCreate),
        crate::show_stream_state::show_stream_materialized_state_parser()
            .map(Statement::ShowRelayMaterializedState),
        crate::udf::describe_udf_parser().map(Statement::DescribeUdf),
        crate::udf::show_udfs_parser().map(Statement::ShowUdfs),
        crate::ingestor::show_ingestors_parser().map(Statement::ShowIngestors),
        crate::placement::show_placements_parser().map(Statement::ShowPlacements),
    );

    choice([domain_and_core, processing_and_io, clients, administration]).boxed()
}

pub fn parse_statement_tokens(tokens: &[Token]) -> Result<Statement, Vec<ParseError<'_>>> {
    let out = statement_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_statement(input: &str) -> error_stack::Result<Statement, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_statement_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_statement(input: &str, cursor: usize) -> Vec<String> {
    let (source, prefix) = completion_context(input, cursor);

    let Some(tokens) = completion_tokens(&source) else {
        return Vec::new();
    };

    let out = statement_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        let statement = out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output");
        // A statement that already parses has no expectations left to derive suggestions from,
        // because end of input is not representable as one. These few continuations are optional
        // tails of an otherwise complete statement, so they are named here.
        return statement_tail(&statement, &tokens, &source, &prefix);
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

pub(crate) fn statement_tail(
    statement: &Statement,
    tokens: &[Token],
    source: &str,
    prefix: &str,
) -> Vec<String> {
    let trimmed = source.trim_end();
    let normalized = trimmed.to_ascii_uppercase();
    // Nothing may follow a terminated statement, so a trailing `;` ends the offers.
    let open = source.len() > trimmed.len() && !trimmed.ends_with(';');
    let optional_tail = if open && normalized == "START" {
        vec![";".to_string(), "AT".to_string()]
    } else if open && normalized.starts_with("START AT ") && !normalized.contains(" TIME RATE ") {
        vec![";".to_string(), "TIME RATE".to_string()]
    } else if open
        && normalized.starts_with("DESCRIBE RESOURCE ")
        && !normalized.contains(" VERSION ")
    {
        vec!["VERSION".to_string()]
    } else if open && let Statement::DescribeTransaction(describe) = &statement {
        crate::describe_transaction::describe_transaction_tail(describe, tokens)
    } else if open && matches!(&statement, Statement::DescribeWasmProcessor(_)) {
        crate::describe_wasm_processor::describe_wasm_processor_tail(tokens)
    } else if open && let Statement::RebindResource(rebind) = &statement {
        if matches!(
            rebind.selection,
            nervix_models::RebindResourceSelection::Members(_)
        ) {
            vec![",".to_string(), ";".to_string()]
        } else {
            vec![";".to_string(), "FOR".to_string()]
        }
    } else if open
        && normalized.starts_with("CREATE ")
        && normalized.contains(" DOMAIN ")
        && !normalized.contains(" PLACEMENT ")
    {
        vec![";".to_string(), "PLACEMENT".to_string()]
    } else if open
        && normalized.starts_with("CREATE ")
        && normalized.contains(" PLACEMENT ")
        && !normalized.contains(" DOMAIN ")
        && !normalized.contains(" RANK ")
    {
        vec![";".to_string(), "RANK".to_string()]
    } else if open
        && (normalized.starts_with("RELOCATE ") || normalized.starts_with("DESCRIBE RELOCATION "))
        && normalized.ends_with(" PREFERENCES")
    {
        vec![";".to_string(), "FOR".to_string()]
    } else {
        Vec::new()
    };
    filter_by_prefix(optional_tail, prefix)
}

#[cfg(test)]
#[path = "statement_json_completion_tests.rs"]
mod json_completion_tests;

#[cfg(test)]
#[path = "statement_tests.rs"]
mod tests;
