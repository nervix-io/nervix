//! Ruby inspection metadata and independently retained verified Row frames.
//!
//! Layer: edges.
//! - **Owns.** Read-only Ruby inspection views and their native schema/buffer lifetimes.
//! - **Depends on.** The shared inspection owner, magnus and the existing IO::Buffer exporter.
//! - **Must not know.** State storage, paging policy, cursor encoding or command routing.

use magnus::{
    DataTypeFunctions, Error, RArray, RModule, RString, Ruby, TypedData, Value, method, prelude::*,
    typed_data::Obj,
};
use meticulous::ResultExt as _;
use nervix_client_core::wire::{
    StateInspectionCounts, StateInspectionSection, StateInspectionUnit,
};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;

use crate::{enums, errors, schema::Schema, values};

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Inspection", free_immediately)]
pub(crate) struct Inspection {
    owner: Arc<host::Inspection>,
}

impl Inspection {
    pub(crate) fn new(inspection: host::Inspection) -> Self {
        Self {
            owner: Arc::new(inspection),
        }
    }
    fn domain(&self) -> &str {
        self.owner.domain()
    }
    fn entity_kind(&self) -> &str {
        self.owner.entity_kind()
    }
    fn entity_name(&self) -> &str {
        self.owner.entity_name()
    }
    fn json(&self) -> &str {
        self.owner.json()
    }
    fn rejection(&self) -> Option<i32> {
        self.owner.rejection().map(enums::InspectionRejection::code)
    }
    fn counts(&self) -> Option<InspectionCounts> {
        self.owner
            .counts()
            .map(|counts| InspectionCounts { counts })
    }
    fn unit_count(&self) -> Option<u64> {
        self.owner.unit_count()
    }
    fn continuation(&self) -> Option<&str> {
        self.owner.continuation()
    }
    fn units(&self) -> RArray {
        let ruby = Ruby::get().assured("a native Ruby accessor holds the GVL");
        ruby.ary_from_iter(
            self.owner
                .units()
                .iter()
                .enumerate()
                .map(|(unit, _)| InspectionUnit {
                    owner: self.owner.clone(),
                    unit,
                }),
        )
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::InspectionCounts", free_immediately)]
struct InspectionCounts {
    counts: StateInspectionCounts,
}

impl InspectionCounts {
    fn branches(&self) -> u64 {
        self.counts.branches
    }
    fn rows(&self) -> u64 {
        self.counts.rows
    }
    fn bytes(&self) -> u64 {
        self.counts.bytes
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::InspectionUnit", free_immediately)]
struct InspectionUnit {
    owner: Arc<host::Inspection>,
    unit: usize,
}

impl InspectionUnit {
    fn data(&self) -> &StateInspectionUnit {
        self.owner
            .unit(self.unit)
            .assured("the view enumerated this immutable inspection's units")
    }
    fn kind(&self) -> i32 {
        enums::InspectionUnitKind::code(
            self.owner
                .unit_kind(self.unit)
                .assured("the view's unit exists"),
        )
    }
    fn index(&self) -> Option<u64> {
        self.owner
            .unit_index(self.unit)
            .assured("the view's unit exists")
    }
    fn owner(&self) -> &str {
        self.data().owner.as_str()
    }
    fn replicas(&self) -> Vec<String> {
        self.data()
            .replicas
            .iter()
            .map(ToString::to_string)
            .collect()
    }
    fn origin_node(&self) -> &str {
        self.data().origin.node.node_id().as_str()
    }
    fn origin_incarnation(&self) -> u64 {
        self.data().origin.node.incarnation().get()
    }
    fn origin_fingerprint(&self) -> RString {
        Ruby::get()
            .assured("a native Ruby accessor holds the GVL")
            .str_from_slice(&self.data().origin.fingerprint)
    }
    fn origin_generation(&self) -> u64 {
        self.data().origin.generation
    }
    fn origin_revision(&self) -> u64 {
        self.data().origin.revision
    }
    fn origin_stage(&self) -> i32 {
        enums::InspectionStage::code(self.data().origin.stage)
    }
    fn counts(&self) -> InspectionCounts {
        InspectionCounts {
            counts: self.data().counts,
        }
    }
    fn sections(&self) -> RArray {
        let ruby = Ruby::get().assured("a native Ruby accessor holds the GVL");
        ruby.ary_from_iter(self.data().sections.iter().enumerate().map(|(section, _)| {
            InspectionSection {
                owner: self.owner.clone(),
                unit: self.unit,
                section,
            }
        }))
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::InspectionSection", free_immediately)]
struct InspectionSection {
    owner: Arc<host::Inspection>,
    unit: usize,
    section: usize,
}

impl InspectionSection {
    fn data(&self) -> &StateInspectionSection {
        self.owner
            .section(self.unit, self.section)
            .assured("the view enumerated this immutable unit's sections")
    }
    fn name(&self) -> &str {
        &self.data().name
    }
    fn omitted_rows(&self) -> u64 {
        self.data().omitted_rows
    }
    fn watermark_low(&self) -> Option<u32> {
        self.data().watermarks.map(|columns| columns.low)
    }
    fn watermark_high(&self) -> Option<u32> {
        self.data().watermarks.map(|columns| columns.high)
    }
    fn schema(&self) -> Schema {
        Schema::new(
            self.owner
                .schema(self.unit, self.section)
                .assured("the view's section exists"),
        )
    }
    fn frames(&self) -> Result<RArray, Error> {
        let ruby = Ruby::get().assured("a native Ruby accessor holds the GVL");
        let frames = ruby.ary_new_capa(self.data().frames.len());
        for index in 0..self.data().frames.len() {
            let frame = self
                .owner
                .row_frame(self.unit, self.section, index)
                .map_err(errors::raise)?;
            frames.push(InspectionFrame { frame })?;
        }
        Ok(frames)
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::InspectionFrame", free_immediately)]
struct InspectionFrame {
    frame: host::InspectionFrame,
}

impl InspectionFrame {
    fn row_count(&self) -> u64 {
        self.frame.row_count()
    }
    fn schema(&self) -> Schema {
        Schema::new(self.frame.schema())
    }
    fn frame(this: Obj<Self>) -> Result<Value, Error> {
        // SAFETY: this immutable Ruby object owns the exact verified frame. The read-only buffer
        // keeps it alive, including while derived slices survive their parent inspection.
        unsafe { values::buffer(this.as_value(), this.frame.bytes()) }
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    Inspection::register(ruby, module)?;
    InspectionCounts::register(ruby, module)?;
    InspectionUnit::register(ruby, module)?;
    InspectionSection::register(ruby, module)?;
    InspectionFrame::register(ruby, module)
}

impl Inspection {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let inspection = crate::class(ruby, module, "Inspection")?;
        inspection.define_method("domain", method!(Inspection::domain, 0))?;
        inspection.define_method("entity_kind", method!(Inspection::entity_kind, 0))?;
        inspection.define_method("entity_name", method!(Inspection::entity_name, 0))?;
        inspection.define_method("json", method!(Inspection::json, 0))?;
        inspection.define_method("rejection", method!(Inspection::rejection, 0))?;
        inspection.define_method("counts", method!(Inspection::counts, 0))?;
        inspection.define_method("unit_count", method!(Inspection::unit_count, 0))?;
        inspection.define_method("continuation", method!(Inspection::continuation, 0))?;
        inspection.define_method("units", method!(Inspection::units, 0))?;
        Ok(())
    }
}

impl InspectionCounts {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let counts = crate::class(ruby, module, "InspectionCounts")?;
        counts.define_method("branches", method!(InspectionCounts::branches, 0))?;
        counts.define_method("rows", method!(InspectionCounts::rows, 0))?;
        counts.define_method("bytes", method!(InspectionCounts::bytes, 0))?;
        Ok(())
    }
}

impl InspectionUnit {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let unit = crate::class(ruby, module, "InspectionUnit")?;
        unit.define_method("kind", method!(InspectionUnit::kind, 0))?;
        unit.define_method("index", method!(InspectionUnit::index, 0))?;
        unit.define_method("owner", method!(InspectionUnit::owner, 0))?;
        unit.define_method("replicas", method!(InspectionUnit::replicas, 0))?;
        unit.define_method("origin_node", method!(InspectionUnit::origin_node, 0))?;
        unit.define_method(
            "origin_incarnation",
            method!(InspectionUnit::origin_incarnation, 0),
        )?;
        unit.define_method(
            "origin_fingerprint",
            method!(InspectionUnit::origin_fingerprint, 0),
        )?;
        unit.define_method(
            "origin_generation",
            method!(InspectionUnit::origin_generation, 0),
        )?;
        unit.define_method(
            "origin_revision",
            method!(InspectionUnit::origin_revision, 0),
        )?;
        unit.define_method("origin_stage", method!(InspectionUnit::origin_stage, 0))?;
        unit.define_method("counts", method!(InspectionUnit::counts, 0))?;
        unit.define_method("sections", method!(InspectionUnit::sections, 0))?;
        Ok(())
    }
}

impl InspectionSection {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let section = crate::class(ruby, module, "InspectionSection")?;
        section.define_method("name", method!(InspectionSection::name, 0))?;
        section.define_method("omitted_rows", method!(InspectionSection::omitted_rows, 0))?;
        section.define_method(
            "watermark_low",
            method!(InspectionSection::watermark_low, 0),
        )?;
        section.define_method(
            "watermark_high",
            method!(InspectionSection::watermark_high, 0),
        )?;
        section.define_method("schema", method!(InspectionSection::schema, 0))?;
        section.define_method("frames", method!(InspectionSection::frames, 0))?;
        Ok(())
    }
}

impl InspectionFrame {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let frame = crate::class(ruby, module, "InspectionFrame")?;
        frame.define_method("row_count", method!(InspectionFrame::row_count, 0))?;
        frame.define_method("schema", method!(InspectionFrame::schema, 0))?;
        frame.define_method("frame", method!(InspectionFrame::frame, 0))?;
        Ok(())
    }
}
