//! Vocabulary for selectors over committed cluster node records.
//!
//! Layer: vocabulary.
//! - **Owns.** The fixed fields and function name a node selector may read.
//! - **Depends on.** Only the vocabulary's Arrow type representations.
//! - **Must not know.** Compilation, membership, record capture or placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "declares node selector fields")
)]

#[cfg(feature = "arrow")]
use arrow_schema::{DataType, Field};
#[cfg(feature = "arrow")]
use nervix_primitives::sync::StdArc;

pub const NODE_SCOPE: &str = "node";
pub const NODE_TAG_FUNCTION: &str = "tag";

/// The fields shared by selector completion and the VM input schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter, strum::IntoStaticStr)]
pub enum NodeSelectorField {
    #[strum(serialize = "name")]
    Name,
    #[strum(serialize = "architecture")]
    Architecture,
    #[strum(serialize = "cpu.count")]
    CpuCount,
    #[strum(serialize = "cpu.model")]
    CpuModel,
    #[strum(serialize = "memory.limit")]
    MemoryLimit,
    #[strum(serialize = "gpu.count")]
    GpuCount,
    #[strum(serialize = "gpu.models")]
    GpuModels,
    #[strum(serialize = "gpu.memory")]
    GpuMemory,
    #[strum(serialize = "gpu.driver")]
    GpuDriver,
    #[strum(serialize = "onnx.version")]
    OnnxVersion,
    #[strum(serialize = "onnx.providers")]
    OnnxProviders,
    #[strum(serialize = "onnx.cuda")]
    OnnxCuda,
    #[strum(serialize = "cuda.available")]
    CudaAvailable,
}

impl NodeSelectorField {
    pub fn name(self) -> &'static str {
        self.into()
    }

    #[cfg(feature = "arrow")]
    pub fn arrow_field(self) -> Field {
        let (ty, nullable) = match self {
            Self::Name | Self::Architecture => (DataType::Utf8, false),
            Self::CpuModel | Self::GpuDriver | Self::OnnxVersion => (DataType::Utf8, true),
            Self::CpuCount | Self::MemoryLimit | Self::GpuMemory => (DataType::UInt64, true),
            Self::GpuCount => (DataType::UInt64, false),
            Self::GpuModels => (Self::text_list(true), false),
            Self::OnnxProviders => (Self::text_list(false), true),
            Self::OnnxCuda | Self::CudaAvailable => (DataType::Boolean, false),
        };
        Field::new(self.name(), ty, nullable)
    }

    #[cfg(feature = "arrow")]
    fn text_list(items_nullable: bool) -> DataType {
        DataType::List(StdArc::new(Field::new(
            "item",
            DataType::Utf8,
            items_nullable,
        )))
    }
}
