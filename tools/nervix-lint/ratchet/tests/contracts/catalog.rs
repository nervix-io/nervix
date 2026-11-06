//! Repository test harness, outside the product layer order.
//! Owns: the allowed/refused semantic fixture catalog.
//! Depends on: fixture names and their required diagnostics.
//! Must not know: compiler execution or host state.

pub struct Refusal {
    pub name: &'static str,
    pub diagnostic: &'static str,
}

pub const REFUSED: &[Refusal] = &[
    Refusal {
        name: "ambiguous_task_binding",
        diagnostic: "binding owning one anonymous body",
    },
    Refusal {
        name: "api_aliases",
        diagnostic: "nervix::clamped_arithmetic",
    },
    Refusal {
        name: "api_arithmetic",
        diagnostic: "nervix::clamped_arithmetic",
    },
    Refusal {
        name: "api_branch_alias",
        diagnostic: "nervix::combinator_control_flow",
    },
    Refusal {
        name: "api_combinator",
        diagnostic: "nervix::combinator_control_flow",
    },
    Refusal {
        name: "api_invalid",
        diagnostic: "Nervix exceptions require a reason_bearing expect",
    },
    Refusal {
        name: "api_unfulfilled",
        diagnostic: "expectation is unfulfilled",
    },
    Refusal {
        name: "api_wide",
        diagnostic: "covers 2 distinct operations",
    },
    Refusal {
        name: "architecture_cluster",
        diagnostic: "nervix::cluster_await",
    },
    Refusal {
        name: "architecture_cluster_binding",
        diagnostic: "nervix::cluster_await",
    },
    Refusal {
        name: "architecture_cluster_trait",
        diagnostic: "nervix::cluster_await",
    },
    Refusal {
        name: "architecture_contract_invalid",
        diagnostic: "nervix::invalid_contract",
    },
    Refusal {
        name: "architecture_identity",
        diagnostic: "nervix::node_identity",
    },
    Refusal {
        name: "architecture_identity_input",
        diagnostic: "nervix::node_identity",
    },
    Refusal {
        name: "architecture_identity_mixed",
        diagnostic: "nervix::node_identity",
    },
    Refusal {
        name: "architecture_input",
        diagnostic: "nervix::model_in_data_plane",
    },
    Refusal {
        name: "architecture_language",
        diagnostic: "nervix::parser_dependency",
    },
    Refusal {
        name: "architecture_language_alias",
        diagnostic: "nervix::parser_dependency",
    },
    Refusal {
        name: "architecture_language_method",
        diagnostic: "nervix::parser_dependency",
    },
    Refusal {
        name: "architecture_model",
        diagnostic: "nervix::model_in_data_plane",
    },
    Refusal {
        name: "architecture_state",
        diagnostic: "nervix::installed_state",
    },
    Refusal {
        name: "architecture_state_review",
        diagnostic: "nervix::state_contract",
    },
    Refusal {
        name: "architecture_trait_scope",
        diagnostic: "nervix::model_in_data_plane",
    },
    Refusal {
        name: "architecture_unfulfilled",
        diagnostic: "expectation is unfulfilled",
    },
    Refusal {
        name: "blanket_suppression",
        diagnostic: "blanket suppression",
    },
    Refusal {
        name: "broad_expectation",
        diagnostic: "blanket suppression",
    },
    Refusal {
        name: "callback_effect",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "conflicting_contract",
        diagnostic: "conflicting context contracts",
    },
    Refusal {
        name: "conflicting_dispatch",
        diagnostic: "conflicting dispatch contracts",
    },
    Refusal {
        name: "dynamic_trait",
        diagnostic: "nervix::unknown_effect",
    },
    Refusal {
        name: "error_associated",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_async_block",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_async_closure",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_borrowed_failure",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_boundary_broad",
        diagnostic: "nervix::invalid_contract",
    },
    Refusal {
        name: "error_boundary_malformed",
        diagnostic: "nervix::invalid_contract",
    },
    Refusal {
        name: "error_boxed_failure",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_closure",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_discard_assignment",
        diagnostic: "nervix::discarded_outcome",
    },
    Refusal {
        name: "error_discard_box",
        diagnostic: "nervix::discarded_outcome",
    },
    Refusal {
        name: "error_discard_partial",
        diagnostic: "nervix::discarded_outcome",
    },
    Refusal {
        name: "error_discard_qualified",
        diagnostic: "nervix::discarded_outcome",
    },
    Refusal {
        name: "error_discard_vec",
        diagnostic: "nervix::discarded_outcome",
    },
    Refusal {
        name: "error_discard_wrapper",
        diagnostic: "nervix::discarded_outcome",
    },
    Refusal {
        name: "error_discarded_named",
        diagnostic: "nervix::discarded_outcome",
    },
    Refusal {
        name: "error_expectation_unfulfilled",
        diagnostic: "expectation is unfulfilled",
    },
    Refusal {
        name: "error_generic",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_nested_alias",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_nested_contract",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_optional_report",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_panic_alias",
        diagnostic: "nervix::bare_panic",
    },
    Refusal {
        name: "error_panic_deref",
        diagnostic: "nervix::bare_panic",
    },
    Refusal {
        name: "error_replacement_unfulfilled",
        diagnostic: "expectation is unfulfilled",
    },
    Refusal {
        name: "error_replacement_wide",
        diagnostic: "covers 2 distinct operations",
    },
    Refusal {
        name: "error_report_field_unfulfilled",
        diagnostic: "expectation is unfulfilled",
    },
    Refusal {
        name: "error_report_field_wide",
        diagnostic: "covers 2 distinct operations",
    },
    Refusal {
        name: "error_stream",
        diagnostic: "nervix::bare_error_signature",
    },
    Refusal {
        name: "error_unwrap_ufcs",
        diagnostic: "nervix::bare_panic",
    },
    Refusal {
        name: "generic_callback",
        diagnostic: "nervix::unknown_effect",
    },
    Refusal {
        name: "generic_parameter_contract",
        diagnostic: "context belongs on",
    },
    Refusal {
        name: "generic_trait",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "helper_effect",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "hot_creates_lifecycle_task",
        diagnostic: "nervix::lifecycle_call",
    },
    Refusal {
        name: "inherited_lifecycle_helper",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "inherited_recurring",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "iterator_effect",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "malformed_contract",
        diagnostic: "bounded protocols require a key",
    },
    Refusal {
        name: "misplaced_contract",
        diagnostic: "context belongs on",
    },
    Refusal {
        name: "parameter_contract",
        diagnostic: "context belongs on",
    },
    Refusal {
        name: "recurring_lock",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "task_body",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "trait_override",
        diagnostic: "cannot weaken",
    },
    Refusal {
        name: "trait_override_outside",
        diagnostic: "cannot weaken",
    },
    Refusal {
        name: "undocumented_expectation",
        diagnostic: "reason-bearing expect",
    },
    Refusal {
        name: "unfulfilled_expectation",
        diagnostic: "expectation is unfulfilled",
    },
    Refusal {
        name: "unfulfilled_macro",
        diagnostic: "expectation is unfulfilled",
    },
    Refusal {
        name: "unknown_annotation",
        diagnostic: "unknown Nervix source annotation",
    },
    Refusal {
        name: "unknown_dispatch",
        diagnostic: "nervix::unknown_effect",
    },
    Refusal {
        name: "unrelated_acquisition",
        diagnostic: "nervix::sync_acquisition",
    },
    Refusal {
        name: "variant_contract",
        diagnostic: "context belongs on",
    },
    Refusal {
        name: "warnings_expectation",
        diagnostic: "blanket suppression",
    },
    Refusal {
        name: "warnings_suppression",
        diagnostic: "blanket suppression",
    },
    Refusal {
        name: "wide_binding",
        diagnostic: "distinct operations",
    },
    Refusal {
        name: "wide_macro",
        diagnostic: "distinct operations",
    },
];

pub const ALLOWED: &[&str] = &[
    "api_allowed",
    "api_expected",
    "architecture_allowed",
    "architecture_expected",
    "architecture_foreign",
    "architecture_metadata",
    "architecture_projection",
    "bounded_lock",
    "bounded_task_body",
    "changed_caller_current",
    "changed_source_cold",
    "constant_allowed",
    "contracted_dispatch",
    "contracted_trait",
    "cross_crate_callee",
    "cross_crate_expected",
    "error_custom_apis",
    "error_expected",
    "error_handled",
    "error_metadata_callee",
    "error_metadata_caller",
    "error_metadata_failure",
    "error_outcomes",
    "error_replacement_allowed",
    "error_replacement_expected",
    "error_replacement_metadata",
    "error_replacement_metadata_allowed",
    "error_report_field_allowed",
    "error_report_field_expected",
    "error_reported",
    "error_resource_lifetimes",
    "error_return_contract",
    "error_storage_values",
    "expected_expression",
    "expected_lock",
    "inherited_override",
    "lifecycle_lock",
    "lifecycle_task_body",
    "stable_contracts",
];
