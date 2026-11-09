//! The Ruby projection of the current inspection and its retained read-only frames.

#[path = "../../../../tests/common/native_inspection.rs"]
mod fixture;

use magnus::{Error, RString, Ruby, function, prelude::*};
use nervix_client_core::wire::{StateInspectionResult, StateInspectionStage};
use nervix_client_host as host;
use nervix_models::Unit;

fn state() -> nervix_client_core::wire::StateInspection {
    fixture::page(
        Unit::partition(u32::MAX),
        StateInspectionStage::Live,
        u64::MAX,
    )
}

fn page() -> crate::inspection::Inspection {
    crate::inspection::Inspection::new(host::Inspection::new(state()))
}

fn refused(index: usize) -> Result<crate::inspection::Inspection, Error> {
    let Some(reason) = fixture::REJECTIONS.get(index) else {
        return Err(crate::errors::invalid(
            "index",
            "does not name a fixture refusal",
        ));
    };
    Ok(crate::inspection::Inspection::new(host::Inspection::new(
        fixture::refused(*reason),
    )))
}

fn json() -> String {
    state().to_json()
}

fn frame(ruby: &Ruby) -> RString {
    let StateInspectionResult::Page(page) = state().result else {
        panic!("the fixture constructs a page");
    };
    ruby.str_from_slice(page.units[0].sections[0].frames[0].bytes())
}

pub(super) fn check(ruby: &Ruby) -> Result<(), Error> {
    let fixture = ruby.define_module("RubyInspectionFixture")?;
    fixture.define_singleton_method("page", function!(page, 0))?;
    fixture.define_singleton_method("refused", function!(refused, 1))?;
    fixture.define_singleton_method("json", function!(json, 0))?;
    fixture.define_singleton_method("frame", function!(frame, 0))?;
    ruby.eval::<magnus::Value>(
        r#"
nx = Nervix::Client
page = RubyInspectionFixture.page
raise 'entity identity' unless [page.domain, page.entity_kind, page.entity_name] == ['sales', 'RELAY', 'events']
raise 'lossless JSON' unless page.json == RubyInspectionFixture.json
raise 'page is not a refusal' unless page.rejection.nil?
raise 'page counts' unless [page.counts.branches, page.counts.rows, page.counts.bytes] == [2, 2, 80]
raise 'unit count' unless page.unit_count == 1 && page.units.length == 1
raise 'opaque continuation' unless page.continuation == "opaque\0position"
unit = page.units.fetch(0)
raise 'partition identity' unless unit.kind == nx::InspectionUnitKind::PARTITION && unit.index == 2**32 - 1
raise 'owner and replicas' unless unit.owner == 'node_a' && unit.replicas == ['node_b', 'node_c']
raise 'origin identity' unless unit.origin_node == 'node_a' && unit.origin_incarnation == 2**64 - 1
raise 'origin fingerprint' unless unit.origin_fingerprint.bytes == [37] * 32
raise 'origin coordinates' unless unit.origin_generation == 2**64 - 1 && unit.origin_revision == 2**64 - 1
raise 'origin stage' unless unit.origin_stage == nx::InspectionStage::LIVE
raise 'unit counts' unless [unit.counts.branches, unit.counts.rows, unit.counts.bytes] == [2, 2, 80]
section = unit.sections.fetch(0)
raise 'section metadata' unless section.name == 'records' && section.omitted_rows == 1
raise 'watermark positions' unless [section.watermark_low, section.watermark_high] == [3, 4]
raise 'section frames' unless section.frames.length == 1
schema = section.schema
raise 'schema order' unless schema.fields.map(&:name) == %w[sequence secret sample low high note raw text]
raise 'branch identity' unless schema.branch == 'by_tenant' && schema.key_fields.fetch(0).sensitive
raise 'field metadata' unless schema.fields.fetch(1).sensitive && schema.fields.fetch(5).nullable
frame = section.frames.fetch(0)
raise 'frame schema' unless frame.schema.fields.map(&:name) == schema.fields.map(&:name)
raise 'frame rows' unless frame.row_count == 1
buffer = frame.frame
expected = RubyInspectionFixture.frame
raise 'exact verified frame' unless buffer.readonly? && buffer.get_string == expected
slice = buffer.slice(3, buffer.size - 3)
page = unit = section = frame = nil
GC.start
GC.compact
raise 'retained frame' unless buffer.get_string == expected && slice.get_string == expected.byteslice(3..)
raise 'retained schema' unless schema.fields.fetch(0).name == 'sequence'
buffer = nil
GC.start
raise 'retained slice' unless slice.get_string == expected.byteslice(3..)
%w[UNKNOWN_ENTITY NOT_MATERIALIZED INVALID_SELECTION INVALID_LIMIT STALE_CONTINUATION OWNER_UNAVAILABLE NOT_READY DEADLINE REPLY_TOO_LARGE].each_with_index do |name, index|
  refused = RubyInspectionFixture.refused(index)
  raise 'typed refusal' unless refused.rejection == nx::InspectionRejection.const_get(name)
  raise 'refusal has no page' unless refused.counts.nil? && refused.unit_count.nil? && refused.continuation.nil? && refused.units.empty?
  raise 'refused identity' unless refused.domain == 'sales' && refused.entity_name == 'plain'
end
"#,
    )?;
    Ok(())
}
