//! Bounded current guest messages shared by protocol, native host and SDK properties.
//!
//! Layer: test harness.
//! - **Owns.** Replayable protocol metadata, opaque byte vectors and synthetic callback sidecars.
//! - **Depends on.** Vocabulary generators and the production guest protocol.
//! - **Must not know.** Arrow payload interpretation, live ACK guards or checkpoint durability.

use meticulous::ResultExt as _;
use nervix_wasm_protocol::{
    AckSidecar, AckToken, AckTokenSet, BranchInit, Envelope, GuestSnapshot, MessageErrorSet,
    NackSet, OutputColumnRef, OutputRow, ProcessorField, ProcessorSchema, ProcessorType,
    RoutedOutput,
};

use crate::{Arbitrary, Domain, Entropy};

/// At most four routes/sidecar sets, six tokens per set, sixteen fields and type depth three.
pub struct WasmValues<'a> {
    arbitrary: Arbitrary<'a>,
}

impl<'a> WasmValues<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            arbitrary: Arbitrary::new(bytes, Domain::Vocabulary),
        }
    }

    pub fn entropy(&mut self) -> &mut Entropy<'a> {
        self.arbitrary.entropy()
    }

    pub fn bytes(&mut self) -> Vec<u8> {
        let count = self.entropy().count(128);
        (0..count).map(|_| self.entropy().byte()).collect()
    }

    pub fn branch_key(&mut self) -> Option<Vec<u8>> {
        if !self.entropy().flag() {
            return None;
        }
        let sequence = self.entropy().any_u64();
        let tenant = self.arbitrary.name_text();
        Some(format!("{{\"sequence\":{sequence},\"tenant\":\"{tenant}\"}}").into_bytes())
    }

    pub fn branch_init(&mut self) -> BranchInit {
        let domain_name = self.arbitrary.name_text();
        let domain_type = self
            .entropy()
            .pick(["runtime", "PACED", "UNPACED"])
            .to_string();
        let branch_key = self.branch_key();
        let input_schema = self.schema();
        let count = self.entropy().count(4);
        let output_schemas = (0..count).map(|_| self.schema()).collect();
        BranchInit {
            domain_name,
            domain_type,
            branch_key,
            input_schema,
            output_schemas,
        }
    }

    pub fn schema(&mut self) -> ProcessorSchema {
        let name = self.arbitrary.name_text();
        let count = self.entropy().count(16);
        let mut fields = Vec::new();
        for index in 0..count {
            let ty = self.processor_type(3);
            fields.push(ProcessorField {
                name: format!("field_{index}"),
                ty,
                optional: self.entropy().flag(),
            });
        }
        ProcessorSchema { name, fields }
    }

    pub fn processor_type(&mut self, depth: u8) -> ProcessorType {
        let kind = self.entropy().byte() % if depth == 0 { 14 } else { 16 };
        match kind {
            14 => ProcessorType::Array {
                element: Box::new(self.processor_type(depth - 1)),
                len: u32::try_from(self.entropy().boundary_biased(0..=u64::from(u32::MAX)))
                    .verified("the range ends at u32::MAX"),
            },
            15 => ProcessorType::Vec {
                element: Box::new(self.processor_type(depth - 1)),
            },
            _ => Self::scalar(kind),
        }
    }

    /// The fourteen scalar kinds, in schema order with BYTES last.
    pub fn scalar(kind: u8) -> ProcessorType {
        match kind {
            0 => ProcessorType::U8,
            1 => ProcessorType::I8,
            2 => ProcessorType::U16,
            3 => ProcessorType::I16,
            4 => ProcessorType::U32,
            5 => ProcessorType::I32,
            6 => ProcessorType::U64,
            7 => ProcessorType::I64,
            8 => ProcessorType::Bool,
            9 => ProcessorType::String,
            10 => ProcessorType::Datetime,
            11 => ProcessorType::F32,
            12 => ProcessorType::F64,
            13 => ProcessorType::Bytes,
            _ => panic!("a scalar selector must be below fourteen"),
        }
    }

    pub fn tokens(&mut self) -> Vec<AckToken> {
        let count = self.entropy().count(6);
        (0..count)
            .map(|_| AckToken(self.entropy().any_u64()))
            .collect()
    }

    pub fn sidecar(&mut self) -> AckSidecar {
        let mut sidecar = AckSidecar::default();
        for _ in 0..self.entropy().count(4) {
            let tokens = self.tokens();
            let source_token = if self.entropy().flag() {
                Some(AckToken(self.entropy().any_u64()))
            } else {
                None
            };
            sidecar.rows.push(OutputRow {
                tokens,
                source_token,
            });
        }
        for _ in 0..self.entropy().count(4) {
            sidecar.acked.push(AckTokenSet {
                tokens: self.tokens(),
            });
        }
        for _ in 0..self.entropy().count(4) {
            sidecar.nacked.push(NackSet {
                tokens: self.tokens(),
                reason: self.arbitrary.string(),
            });
        }
        for _ in 0..self.entropy().count(4) {
            sidecar.message_errors.push(MessageErrorSet {
                tokens: self.tokens(),
                reason: self.arbitrary.string(),
            });
        }
        sidecar
    }

    pub fn outputs(&mut self) -> Vec<RoutedOutput> {
        let count = self.entropy().count(4);
        let mut outputs = Vec::new();
        for _ in 0..count {
            let output_relay = self.arbitrary.name_text();
            let mut columns = Vec::new();
            for _ in 0..self.entropy().count(8) {
                let column_index =
                    u32::try_from(self.entropy().boundary_biased(0..=u64::from(u32::MAX)))
                        .verified("the range ends at u32::MAX");
                columns.push(self.entropy().pick([
                    OutputColumnRef::Input { column_index },
                    OutputColumnRef::Generated { column_index },
                    OutputColumnRef::Uninitialized,
                ]));
            }
            outputs.push(RoutedOutput {
                output_relay,
                columns,
                acks: self.sidecar(),
            });
        }
        outputs
    }

    pub fn envelope(&mut self, output: bool) -> Envelope {
        if output {
            Envelope::Output {
                generated_arrow_ipc_batch: self.bytes(),
                outputs: self.outputs(),
            }
        } else {
            Envelope::Input {
                arrow_ipc_batch: self.bytes(),
                acks: self.sidecar(),
            }
        }
    }

    pub fn snapshot(&mut self) -> GuestSnapshot {
        GuestSnapshot {
            init_metadata: self.branch_init().encode(),
            application_state: self.bytes(),
        }
    }
}
