//! What a program compiled for a domain may name.
//!
//! Layer: decisions.
//!
//! - **Owns.** The namespaces the registry binds when it type-checks an expression, and the UDF
//!   signatures a domain's programs may call.
//! - **Depends on.** The UDF Models and the VM's compile options.
//! - **Must not know.** What the compiled program does with them.

use nervix_models::{Model, ModelIndex};
use nervix_roto::signatures_for as udf_signatures_for;
use nervix_vm::CompileOptions;
pub(in crate::registry) const BRANCH_NAMESPACE: &str = "branch";

pub(in crate::registry) const INGEST_MESSAGE_NAMESPACE: &str = "message";

pub(in crate::registry) const INNER_OUTPUT_NAMESPACE: &str = "inner_output";

pub(in crate::registry) fn udf_compile_options(
    models: &ModelIndex,
    mut options: CompileOptions,
) -> CompileOptions {
    options.udf_signatures = udf_signatures_for(models.models().filter_map(|model| match model {
        Model::Udf(udf) => Some(udf),
        _ => None,
    }));
    options
}
