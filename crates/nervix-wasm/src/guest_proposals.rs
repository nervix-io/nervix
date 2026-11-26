//! The WebAssembly features a WASM guest may use.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The guest contract's fixed set of WebAssembly features, what a relaxed SIMD
//!   instruction returns, and naming the proposals outside the contract that a module needs when
//!   they are why it does not compile.
//! - **Depends on.** Wasmtime's feature set and the `wasmparser` validator Wasmtime re-exports, so
//!   the features named here are always the ones the engine validates against, the WebAssembly
//!   text-format parser, and the bounded executor's cancellation.
//! - **Must not know.** Which processor, resource or branch a module belongs to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the WebAssembly features of the WASM guest contract"
    )
)]

use std::fmt;

use nervix_execution::{Cancellation, Cancelled};
use strum::IntoEnumIterator as _;
use wasmtime::{WasmFeatures, wasmparser::Validator};

/// Every WebAssembly feature a guest module may use: WebAssembly 2.0 without garbage-collected
/// references, tail calls, extended constant expressions, typed function references, the type
/// system of the GC proposal, multiple memories, relaxed SIMD, 64-bit memories and wide
/// arithmetic. The engine enables exactly these and no other, whatever Wasmtime enables by default.
///
/// `REFERENCE_TYPES` admits `funcref` only: `externref` also needs `GC_TYPES`, which
/// [`RefusedWasmProposal::GarbageCollectedReferences`] refuses. `GC` admits the type system of the
/// GC proposal, recursive type groups and declared subtypes, without any of its heap types.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WebAssembly Features section of the WASM Processor Guests contract lists \
                  these, the features Wasmtime 49 enabled for Nervix's build before the contract \
                  fixed them"
    )
)]
pub(crate) const GUEST_WASM_FEATURES: WasmFeatures = WasmFeatures::FLOATS
    .union(WasmFeatures::MUTABLE_GLOBAL)
    .union(WasmFeatures::SATURATING_FLOAT_TO_INT)
    .union(WasmFeatures::SIGN_EXTENSION)
    .union(WasmFeatures::MULTI_VALUE)
    .union(WasmFeatures::REFERENCE_TYPES)
    .union(WasmFeatures::BULK_MEMORY)
    .union(WasmFeatures::SIMD)
    .union(WasmFeatures::TAIL_CALL)
    .union(WasmFeatures::EXTENDED_CONST)
    .union(WasmFeatures::FUNCTION_REFERENCES)
    .union(WasmFeatures::GC)
    .union(WasmFeatures::MULTI_MEMORY)
    .union(WasmFeatures::RELAXED_SIMD)
    .union(WasmFeatures::MEMORY64)
    .union(WasmFeatures::WIDE_ARITHMETIC);

/// Whether a relaxed SIMD instruction returns its deterministic result on every host instead of
/// what the node's processor computes natively.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WebAssembly Features section of the WASM Processor Guests contract gives a \
                  relaxed SIMD instruction the node's native result, as Wasmtime computed it \
                  before the contract fixed it"
    )
)]
pub(crate) const RELAXED_SIMD_DETERMINISTIC: bool = false;

/// A WebAssembly feature outside the guest contract, named when a module that needs it is refused.
///
/// Together with the contract's features the proposals partition every feature the validator
/// knows, so a module the contract refuses always needs at least one of them. A proposal that
/// extends another, or is an earlier form of another, is declared before that other one, so a
/// construct both admit, such as `throw`, is named as part of the other one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::EnumIter)]
pub enum RefusedWasmProposal {
    /// Shared globals, tables and functions, which extend the threads proposal.
    #[strum(serialize = "the shared-everything threads proposal")]
    SharedEverythingThreads,
    /// Shared memories and atomic instructions.
    #[strum(serialize = "the threads proposal")]
    Threads,
    /// `try`, `catch` and `rethrow`, the exception handling proposal's earlier form.
    #[strum(serialize = "the legacy exception handling proposal")]
    LegacyExceptionHandling,
    /// Tags, `throw`, `try_table` and the `exnref` type.
    #[strum(serialize = "the exception handling proposal")]
    ExceptionHandling,
    /// Continuations and their instructions.
    #[strum(serialize = "the stack switching proposal")]
    StackSwitching,
    /// Descriptor types of structs, which extend garbage-collected references.
    #[strum(serialize = "the custom descriptors proposal")]
    CustomDescriptors,
    /// `externref`, `anyref`, `eqref`, `i31ref`, structs and arrays, which need a garbage collector.
    #[strum(serialize = "garbage-collected references")]
    GarbageCollectedReferences,
    /// Linear memories whose pages are not 64 KiB.
    #[strum(serialize = "the custom page sizes proposal")]
    CustomPageSizes,
    /// Instructions that discard or protect parts of a linear memory.
    #[strum(serialize = "the memory control proposal")]
    MemoryControl,
    /// The compact encoding of an import section.
    #[strum(serialize = "the compact imports proposal")]
    CompactImports,
    /// Components and every extension of the component model.
    #[strum(serialize = "the component model")]
    ComponentModel,
}

impl RefusedWasmProposal {
    /// The validator features this proposal stands for.
    const fn features(self) -> WasmFeatures {
        match self {
            Self::Threads => WasmFeatures::THREADS,
            Self::SharedEverythingThreads => WasmFeatures::SHARED_EVERYTHING_THREADS,
            Self::ExceptionHandling => WasmFeatures::EXCEPTIONS,
            Self::LegacyExceptionHandling => WasmFeatures::LEGACY_EXCEPTIONS,
            Self::GarbageCollectedReferences => WasmFeatures::GC_TYPES,
            Self::CustomPageSizes => WasmFeatures::CUSTOM_PAGE_SIZES,
            Self::StackSwitching => WasmFeatures::STACK_SWITCHING,
            Self::MemoryControl => WasmFeatures::MEMORY_CONTROL,
            Self::CustomDescriptors => WasmFeatures::CUSTOM_DESCRIPTORS,
            Self::CompactImports => WasmFeatures::COMPACT_IMPORTS,
            Self::ComponentModel => WasmFeatures::COMPONENT_MODEL
                .union(WasmFeatures::CM_VALUES)
                .union(WasmFeatures::CM_NESTED_NAMES)
                .union(WasmFeatures::CM_ASYNC)
                .union(WasmFeatures::CM_ASYNC_STACKFUL)
                .union(WasmFeatures::CM_MORE_ASYNC_BUILTINS)
                .union(WasmFeatures::CM_THREADING)
                .union(WasmFeatures::CM_ERROR_CONTEXT)
                .union(WasmFeatures::CM_FIXED_LENGTH_LISTS)
                .union(WasmFeatures::CM_GC)
                .union(WasmFeatures::CM_MAP)
                .union(WasmFeatures::CM64)
                .union(WasmFeatures::CM_IMPLEMENTS)
                .union(WasmFeatures::CM_CANON_NAMES),
        }
    }
}

/// The proposals outside the guest contract that a refused module needs, at least one, in the
/// order [`RefusedWasmProposal`] declares them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedWasmProposals {
    first: RefusedWasmProposal,
    rest: Vec<RefusedWasmProposal>,
}

impl RefusedWasmProposals {
    /// The proposals outside the guest contract that `wasm`, a module in the binary or the text
    /// format, needs, when they are why the engine refuses it. `None` when the module is valid
    /// within the contract, so something else refused it, or is invalid whichever features a
    /// validator enables.
    ///
    /// Starting from every feature the validator knows, under which the module is valid, it
    /// leaves out one refused proposal at a time and keeps each one without which the module is
    /// invalid. What it keeps is a smallest set of proposals the module needs beside the contract.
    /// Each step is one validation of the module, and the job's cancellation is checked between
    /// them.
    pub(crate) fn needed_by(
        wasm: &[u8],
        cancellation: &Cancellation,
    ) -> Result<Option<Self>, Cancelled> {
        // Text the text-format parser refuses needs no proposal: it is not a module at all.
        let Ok(binary) = wat::parse_bytes(wasm) else {
            return Ok(None);
        };
        cancellation.check()?;
        if Self::validates(&binary, GUEST_WASM_FEATURES) {
            return Ok(None);
        }
        cancellation.check()?;
        let mut enabled = WasmFeatures::all();
        if !Self::validates(&binary, enabled) {
            return Ok(None);
        }
        let mut needed = Vec::new();
        for proposal in RefusedWasmProposal::iter() {
            cancellation.check()?;
            let without = enabled.difference(proposal.features());
            if Self::validates(&binary, without) {
                enabled = without;
            } else {
                needed.push(proposal);
            }
        }
        let mut needed = needed.into_iter();
        // The contract and the refused proposals partition every feature, so a module invalid
        // within the contract and valid with every feature keeps at least one proposal. A
        // validator that broke that still leaves the refusal a compile failure, never a panic.
        let Some(first) = needed.next() else {
            return Ok(None);
        };
        Ok(Some(Self {
            first,
            rest: needed.collect(),
        }))
    }

    /// Whether `binary` is a valid module or component with exactly `features` enabled.
    fn validates(binary: &[u8], features: WasmFeatures) -> bool {
        Validator::new_with_features(features)
            .validate_all(binary)
            .is_ok()
    }

    /// Every proposal, in the order [`RefusedWasmProposal`] declares them.
    pub fn iter(&self) -> impl Iterator<Item = RefusedWasmProposal> + '_ {
        std::iter::once(self.first).chain(self.rest.iter().copied())
    }
}

impl fmt::Display for RefusedWasmProposals {
    /// Names each proposal, the last joined by "and": "the threads proposal and the custom page
    /// sizes proposal".
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.first)?;
        let Some((last, middle)) = self.rest.split_last() else {
            return Ok(());
        };
        for proposal in middle {
            write!(formatter, ", {proposal}")?;
        }
        write!(formatter, " and {last}")
    }
}

#[cfg(test)]
mod tests {
    use nervix_execution::{CpuClass, Executor, MemoryClass};

    use super::*;

    /// What [`RefusedWasmProposals::needed_by`] answers for `wasm`, run as a bulk job, the way a
    /// compilation runs it.
    async fn needed_by(wasm: &'static [u8]) -> Option<RefusedWasmProposals> {
        let executor = Executor::default();
        let reservation = executor
            .reserve_admission(MemoryClass::Bulk)
            .await
            .expect("an idle executor admits one byte of bulk memory");
        executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                RefusedWasmProposals::needed_by(wasm, cancellation)
            })
            .await
            .expect("an idle executor runs the job")
            .expect("nothing cancels the job")
    }

    #[nervix_primitives::test]
    async fn a_module_valid_within_the_contract_needs_no_refused_proposal() {
        assert_eq!(
            needed_by(br#"(module (memory (export "memory") 1) (func (result i32) i32.const 0))"#)
                .await,
            None
        );
    }

    #[nervix_primitives::test]
    async fn a_binary_module_names_the_proposals_it_needs_as_a_text_module_does() {
        let shared_memory: &[u8] = &[
            0x00, 0x61, 0x73, 0x6d, // the binary format's magic
            0x01, 0x00, 0x00, 0x00, // version 1
            0x05, 0x04, 0x01, // a memory section of four bytes holding one memory
            0x03, 0x01, 0x01, // shared, with a minimum and a maximum of one page
        ];

        let needed = needed_by(shared_memory).await;

        assert_eq!(
            needed.map(|proposals| proposals.iter().collect::<Vec<_>>()),
            Some(vec![RefusedWasmProposal::Threads])
        );
    }

    #[test]
    fn the_contract_and_the_refused_proposals_partition_every_feature_the_validator_knows() {
        let mut refused = WasmFeatures::empty();
        for proposal in RefusedWasmProposal::iter() {
            assert!(
                GUEST_WASM_FEATURES
                    .intersection(proposal.features())
                    .is_empty(),
                "{proposal} is also part of the guest contract"
            );
            assert!(
                refused.intersection(proposal.features()).is_empty(),
                "{proposal} overlaps another refused proposal"
            );
            refused = refused.union(proposal.features());
        }

        assert_eq!(
            GUEST_WASM_FEATURES.union(refused),
            WasmFeatures::all(),
            "every feature the validator knows is part of the contract or of a refused proposal"
        );
    }

    #[test]
    fn refused_proposals_name_each_proposal_and_join_the_last_with_and() {
        let one = RefusedWasmProposals {
            first: RefusedWasmProposal::Threads,
            rest: Vec::new(),
        };
        let two = RefusedWasmProposals {
            first: RefusedWasmProposal::Threads,
            rest: vec![RefusedWasmProposal::CustomPageSizes],
        };
        let three = RefusedWasmProposals {
            first: RefusedWasmProposal::Threads,
            rest: vec![
                RefusedWasmProposal::GarbageCollectedReferences,
                RefusedWasmProposal::CustomPageSizes,
            ],
        };

        assert_eq!(one.to_string(), "the threads proposal");
        assert_eq!(
            two.to_string(),
            "the threads proposal and the custom page sizes proposal"
        );
        assert_eq!(
            three.to_string(),
            "the threads proposal, garbage-collected references and the custom page sizes proposal"
        );
        assert_eq!(
            three.iter().collect::<Vec<_>>(),
            [
                RefusedWasmProposal::Threads,
                RefusedWasmProposal::GarbageCollectedReferences,
                RefusedWasmProposal::CustomPageSizes,
            ]
        );
    }
}
