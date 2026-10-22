//! Client emitter composition from its typed start plan.
//!
//! Layer: data plane.
//! - **Owns.** Opening the Client connector and pairing it with its prepared host input.
//! - **Depends on.** Its connector engine, typed plan, host context and input preparation.
//! - **Must not know.** Models, placement, buffering, flush cadence or retry scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

impl EmitterConnectorStarter {
    pub(super) fn client(
        connector: &ClientEmitterPlan,
        context: &EmitterConnectorContext,
        output_schema: &Arc<CompiledSchema>,
        plan: &EmitterStartPlan,
    ) -> EmitterRuntimeResult<Box<dyn EmitterConnector>> {
        let opened = Box::new(nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "sink initialization publishes one concrete client-emitter endpoint lifetime before \
             processing records",
            ClientConsumerConnector::new(
                context,
                connector,
                output_schema.clone(),
                plan.retry_policy,
            )
        ));
        Ok(opened)
    }
}
