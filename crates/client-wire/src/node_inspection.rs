//! Encodes the typed answer to a cluster node inspection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The FlatBuffers boundary for observed nodes and their committed facts.
//! - **Depends on.** Validated node vocabulary and session codec bounds.
//! - **Must not know.** Where a node record came from or how a selector matches it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns node inspection wire encoding")
)]

use std::{collections::BTreeMap, num::NonZeroU64};

use error_stack::{Report, ResultExt as _};
use flatbuffers::WIPOffset;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    CudaProviderRegistration, Detected, ExecutionProviders, GpuDevice, GpuDevices, GpuInventory,
    NodeInspection, NodeInspectionRecord, NodeInspectionRow, NodeProperties, NodeTags, OnnxRuntime,
    OnnxRuntimeDetection, PropertyText, TagKey, TagValue, UndetectedReason,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    wire,
};

wire_enum!(NODE_UNDETECTED_REASONS: UndetectedReason => wire::NodeUndetectedReason {
    NotReported,
    LibraryUnavailable,
    DriverUnavailable,
    QueryFailed,
    DeadlineExpired,
    DetectionFailed,
    ExceedsRecordBound,
});

wire_enum!(NODE_CUDA_REGISTRATIONS: CudaProviderRegistration => wire::NodeCudaRegistration {
    Registered,
    NotInLibrary,
    RegistrationFailed,
});

/// The lossless JSON projection shared by the server and native bindings.
pub fn node_inspection_json(inspection: &NodeInspection) -> String {
    serde_json::to_string(inspection).assured("node inspection fields serialize as JSON values")
}

pub(crate) fn encode<'fbb>(
    inspection: &NodeInspection,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::NodeInspection<'fbb>>, Report<WireEncodeError>> {
    if inspection
        .nodes
        .windows(2)
        .any(|rows| rows[0].name >= rows[1].name)
    {
        return Err(Report::new(WireEncodeError::InvalidValue {
            field: "NodeInspection.nodes",
            kind: "unique nodes in name order",
        }));
    }
    let nodes = encoder.table_vector("NodeInspection.nodes", &inspection.nodes, encode_row)?;
    Ok(wire::NodeInspection::create(
        encoder.fbb(),
        &wire::NodeInspectionArgs { nodes: Some(nodes) },
    ))
}

fn encode_row<'fbb>(
    row: &NodeInspectionRow,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::NodeInspectionRow<'fbb>>, Report<WireEncodeError>> {
    let name = encoder.text("NodeInspectionRow.name", row.name.as_str())?;
    let record = row
        .record
        .as_ref()
        .map(|record| encode_record(record, encoder))
        .transpose()?;
    Ok(wire::NodeInspectionRow::create(
        encoder.fbb(),
        &wire::NodeInspectionRowArgs {
            name: Some(name),
            live: Some(row.live),
            voter: Some(row.voter),
            cordoned: Some(row.cordoned),
            terminating: Some(row.terminating),
            record,
        },
    ))
}

fn encode_record<'fbb>(
    record: &NodeInspectionRecord,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::NodeInspectionRecord<'fbb>>, Report<WireEncodeError>> {
    let properties = encode_properties(&record.properties, encoder)?;
    encoder.entries("NodeInspectionRecord.tags", record.tags.len())?;
    let mut entries = Vec::with_capacity(record.tags.len());
    for (key, value) in record.tags.iter() {
        let key = encoder.text("NodeTagEntry.key", key.as_str())?;
        let value = encoder.text("NodeTagEntry.value", value.as_str())?;
        entries.push(wire::NodeTagEntry::create(
            encoder.fbb(),
            &wire::NodeTagEntryArgs {
                key: Some(key),
                value: Some(value),
            },
        ));
        encoder.within_limit("NodeInspectionRecord.tags")?;
    }
    let tags = encoder.tables("NodeInspectionRecord.tags", &entries)?;
    Ok(wire::NodeInspectionRecord::create(
        encoder.fbb(),
        &wire::NodeInspectionRecordArgs {
            revision: Some(record.revision),
            cpu_count: record.cpu_count.map(NonZeroU64::get),
            memory_limit: record.memory_limit.map(NonZeroU64::get),
            properties: Some(properties),
            tags: Some(tags),
        },
    ))
}

fn encode_properties<'fbb>(
    properties: &NodeProperties,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::NodeDetectedProperties<'fbb>>, Report<WireEncodeError>> {
    let architecture = encoder.text(
        "NodeDetectedProperties.architecture",
        properties.architecture.as_str(),
    )?;
    let cpu_model = encode_text_detection(&properties.cpu_model, encoder)?;
    let gpus = encode_gpus(&properties.gpus, encoder)?;
    let onnx = encode_onnx(&properties.onnx, encoder)?;
    Ok(wire::NodeDetectedProperties::create(
        encoder.fbb(),
        &wire::NodeDetectedPropertiesArgs {
            architecture: Some(architecture),
            cpu_model_type: cpu_model.discriminant,
            cpu_model: Some(cpu_model.value),
            gpus_type: gpus.discriminant,
            gpus: Some(gpus.value),
            onnx_type: onnx.discriminant,
            onnx: Some(onnx.value),
        },
    ))
}

fn undetected<'fbb>(
    reason: UndetectedReason,
    encoder: &mut Encoder<'fbb>,
) -> WIPOffset<wire::NodeUndetected<'fbb>> {
    wire::NodeUndetected::create(
        encoder.fbb(),
        &wire::NodeUndetectedArgs {
            reason: Some(reason.into()),
        },
    )
}

fn encode_text_detection<'fbb>(
    text: &Detected<PropertyText>,
    encoder: &mut Encoder<'fbb>,
) -> Result<EncodedUnion<wire::NodeTextDetection>, Report<WireEncodeError>> {
    Ok(match text {
        Detected::Value(text) => {
            let value = encoder.text("NodeDetectedText.value", text.as_str())?;
            EncodedUnion::new(
                wire::NodeTextDetection::NodeDetectedText,
                wire::NodeDetectedText::create(
                    encoder.fbb(),
                    &wire::NodeDetectedTextArgs { value: Some(value) },
                ),
            )
        }
        Detected::Undetected(reason) => EncodedUnion::new(
            wire::NodeTextDetection::NodeUndetected,
            undetected(*reason, encoder),
        ),
    })
}

fn encode_gpu<'fbb>(
    device: &GpuDevice,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::NodeGpuDevice<'fbb>>, Report<WireEncodeError>> {
    let model = encode_text_detection(&device.model, encoder)?;
    let memory = match device.memory_bytes {
        Detected::Value(bytes) => EncodedUnion::new(
            wire::NodeBytesDetection::NodeDetectedBytes,
            wire::NodeDetectedBytes::create(
                encoder.fbb(),
                &wire::NodeDetectedBytesArgs {
                    bytes: Some(bytes.get()),
                },
            ),
        ),
        Detected::Undetected(reason) => EncodedUnion::new(
            wire::NodeBytesDetection::NodeUndetected,
            undetected(reason, encoder),
        ),
    };
    Ok(wire::NodeGpuDevice::create(
        encoder.fbb(),
        &wire::NodeGpuDeviceArgs {
            model_type: model.discriminant,
            model: Some(model.value),
            memory_type: memory.discriminant,
            memory: Some(memory.value),
        },
    ))
}

fn encode_gpus(
    inventory: &GpuInventory,
    encoder: &mut Encoder<'_>,
) -> Result<EncodedUnion<wire::NodeGpuInventory>, Report<WireEncodeError>> {
    Ok(match inventory {
        GpuInventory::Devices { devices, driver } => {
            let devices =
                encoder.table_vector("NodeGpuDevices.devices", devices.as_slice(), encode_gpu)?;
            let driver = encode_text_detection(driver, encoder)?;
            EncodedUnion::new(
                wire::NodeGpuInventory::NodeGpuDevices,
                wire::NodeGpuDevices::create(
                    encoder.fbb(),
                    &wire::NodeGpuDevicesArgs {
                        devices: Some(devices),
                        driver_type: driver.discriminant,
                        driver: Some(driver.value),
                    },
                ),
            )
        }
        GpuInventory::NoDevice => EncodedUnion::new(
            wire::NodeGpuInventory::NodeNoGpuDevice,
            wire::NodeNoGpuDevice::create(encoder.fbb(), &wire::NodeNoGpuDeviceArgs {}),
        ),
        GpuInventory::Undetected(reason) => EncodedUnion::new(
            wire::NodeGpuInventory::NodeUndetected,
            undetected(*reason, encoder),
        ),
    })
}

fn encode_providers(
    providers: &Detected<ExecutionProviders>,
    encoder: &mut Encoder<'_>,
) -> Result<EncodedUnion<wire::NodeProvidersDetection>, Report<WireEncodeError>> {
    Ok(match providers {
        Detected::Value(providers) => {
            encoder.entries("NodeDetectedProviders.values", providers.as_slice().len())?;
            let mut values = Vec::with_capacity(providers.as_slice().len());
            for provider in providers.as_slice() {
                values.push(encoder.text("NodeDetectedProviders.value", provider.as_str())?);
            }
            let values = encoder.tables("NodeDetectedProviders.values", &values)?;
            EncodedUnion::new(
                wire::NodeProvidersDetection::NodeDetectedProviders,
                wire::NodeDetectedProviders::create(
                    encoder.fbb(),
                    &wire::NodeDetectedProvidersArgs {
                        values: Some(values),
                    },
                ),
            )
        }
        Detected::Undetected(reason) => EncodedUnion::new(
            wire::NodeProvidersDetection::NodeUndetected,
            undetected(*reason, encoder),
        ),
    })
}

fn encode_onnx(
    detection: &OnnxRuntimeDetection,
    encoder: &mut Encoder<'_>,
) -> Result<EncodedUnion<wire::NodeOnnxDetection>, Report<WireEncodeError>> {
    Ok(match detection {
        OnnxRuntimeDetection::Loaded(runtime) => {
            let version = encode_text_detection(&runtime.version, encoder)?;
            let providers = encode_providers(&runtime.providers, encoder)?;
            EncodedUnion::new(
                wire::NodeOnnxDetection::NodeOnnxRuntime,
                wire::NodeOnnxRuntime::create(
                    encoder.fbb(),
                    &wire::NodeOnnxRuntimeArgs {
                        version_type: version.discriminant,
                        version: Some(version.value),
                        providers_type: providers.discriminant,
                        providers: Some(providers.value),
                        cuda: Some(runtime.cuda.into()),
                    },
                ),
            )
        }
        OnnxRuntimeDetection::Undetected(reason) => EncodedUnion::new(
            wire::NodeOnnxDetection::NodeUndetected,
            undetected(*reason, encoder),
        ),
    })
}

pub(crate) fn decode(
    decoder: Decoder<'_>,
    inspection: wire::NodeInspection<'_>,
) -> Result<NodeInspection, Report<WireDecodeError>> {
    let nodes = decoder.table_vector("NodeInspection.nodes", inspection.nodes(), |row| {
        decode_row(decoder, row)
    })?;
    if nodes.windows(2).any(|rows| rows[0].name >= rows[1].name) {
        return Err(Report::new(WireDecodeError::NonCanonicalSet {
            field: "NodeInspection.nodes",
        }));
    }
    Ok(NodeInspection { nodes })
}

fn decode_row(
    decoder: Decoder<'_>,
    row: wire::NodeInspectionRow<'_>,
) -> Result<NodeInspectionRow, Report<WireDecodeError>> {
    let record = row
        .record()
        .map(|record| decode_record(decoder, record))
        .transpose()?;
    Ok(NodeInspectionRow {
        name: decoder.name("NodeInspectionRow.name", row.name())?,
        live: decoder.required("NodeInspectionRow.live", row.live())?,
        voter: decoder.required("NodeInspectionRow.voter", row.voter())?,
        cordoned: decoder.required("NodeInspectionRow.cordoned", row.cordoned())?,
        terminating: decoder.required("NodeInspectionRow.terminating", row.terminating())?,
        record,
    })
}

fn decode_record(
    decoder: Decoder<'_>,
    record: wire::NodeInspectionRecord<'_>,
) -> Result<NodeInspectionRecord, Report<WireDecodeError>> {
    let entries = record.tags();
    decoder.entries("NodeInspectionRecord.tags", entries.len())?;
    let mut tags = BTreeMap::new();
    for entry in entries {
        let raw_key = decoder.check_text("NodeTagEntry.key", entry.key())?;
        let key = TagKey::parse(raw_key).change_context(WireDecodeError::InvalidValue {
            field: "NodeTagEntry.key",
            kind: "node tag key",
        })?;
        if let Some((previous, _)) = tags.last_key_value()
            && previous >= &key
        {
            return Err(Report::new(WireDecodeError::NonCanonicalSet {
                field: "NodeInspectionRecord.tags",
            }));
        }
        let raw_value = decoder.check_text("NodeTagEntry.value", entry.value())?;
        let value =
            TagValue::parse(&key, raw_value).change_context(WireDecodeError::InvalidValue {
                field: "NodeTagEntry.value",
                kind: "node tag value",
            })?;
        tags.insert(key, value);
    }
    let tags = NodeTags::try_from(tags).change_context(WireDecodeError::InvalidValue {
        field: "NodeInspectionRecord.tags",
        kind: "bounded node tag map",
    })?;
    let cpu_count = record
        .cpu_count()
        .map(|value| decoder.non_zero("NodeInspectionRecord.cpu_count", value))
        .transpose()?;
    let memory_limit = record
        .memory_limit()
        .map(|value| decoder.non_zero("NodeInspectionRecord.memory_limit", value))
        .transpose()?;
    Ok(NodeInspectionRecord {
        revision: decoder.required("NodeInspectionRecord.revision", record.revision())?,
        cpu_count,
        memory_limit,
        properties: decode_properties(decoder, record.properties())?,
        tags,
    })
}

fn decode_reason(
    decoder: Decoder<'_>,
    value: wire::NodeUndetected<'_>,
) -> Result<UndetectedReason, Report<WireDecodeError>> {
    decoder.required_enumeration("NodeUndetected.reason", value.reason())
}

fn property_text(
    decoder: Decoder<'_>,
    field: &'static str,
    text: &str,
) -> Result<PropertyText, Report<WireDecodeError>> {
    let text = decoder.check_text(field, text)?;
    PropertyText::new(text).change_context(WireDecodeError::InvalidValue {
        field,
        kind: "node property text",
    })
}

fn decode_text_detection(
    decoder: Decoder<'_>,
    value: Option<wire::NodeDetectedText<'_>>,
    undetected: Option<wire::NodeUndetected<'_>>,
    kind: wire::NodeTextDetection,
) -> Result<Detected<PropertyText>, Report<WireDecodeError>> {
    if let Some(value) = value {
        return Ok(Detected::Value(property_text(
            decoder,
            "NodeDetectedText.value",
            value.value(),
        )?));
    }
    if let Some(undetected) = undetected {
        return Ok(Detected::Undetected(decode_reason(decoder, undetected)?));
    }
    Err(decoder.unknown_union("NodeTextDetection", kind.0))
}

fn decode_gpu(
    decoder: Decoder<'_>,
    device: wire::NodeGpuDevice<'_>,
) -> Result<GpuDevice, Report<WireDecodeError>> {
    let model = decode_text_detection(
        decoder,
        device.model_as_node_detected_text(),
        device.model_as_node_undetected(),
        device.model_type(),
    )?;
    let memory_bytes = match device.memory_type() {
        wire::NodeBytesDetection::NodeDetectedBytes => {
            let value = device
                .memory_as_node_detected_bytes()
                .assured("the verified GPU memory union carries bytes");
            let bytes = decoder.required("NodeDetectedBytes.bytes", value.bytes())?;
            Detected::Value(decoder.non_zero("NodeDetectedBytes.bytes", bytes)?)
        }
        wire::NodeBytesDetection::NodeUndetected => {
            let value = device
                .memory_as_node_undetected()
                .assured("the verified GPU memory union carries a reason");
            Detected::Undetected(decode_reason(decoder, value)?)
        }
        unknown => return Err(decoder.unknown_union("NodeGpuDevice.memory", unknown.0)),
    };
    Ok(GpuDevice {
        model,
        memory_bytes,
    })
}

fn decode_gpus(
    decoder: Decoder<'_>,
    value: wire::NodeDetectedProperties<'_>,
) -> Result<GpuInventory, Report<WireDecodeError>> {
    match value.gpus_type() {
        wire::NodeGpuInventory::NodeGpuDevices => {
            let value = value
                .gpus_as_node_gpu_devices()
                .assured("the verified GPU union carries devices");
            let devices =
                decoder.table_vector("NodeGpuDevices.devices", value.devices(), |device| {
                    decode_gpu(decoder, device)
                })?;
            let devices =
                GpuDevices::try_from(devices).change_context(WireDecodeError::InvalidValue {
                    field: "NodeGpuDevices.devices",
                    kind: "bounded nonempty device list",
                })?;
            let driver = decode_text_detection(
                decoder,
                value.driver_as_node_detected_text(),
                value.driver_as_node_undetected(),
                value.driver_type(),
            )?;
            Ok(GpuInventory::Devices { devices, driver })
        }
        wire::NodeGpuInventory::NodeNoGpuDevice => Ok(GpuInventory::NoDevice),
        wire::NodeGpuInventory::NodeUndetected => {
            let value = value
                .gpus_as_node_undetected()
                .assured("the verified GPU union carries a reason");
            Ok(GpuInventory::Undetected(decode_reason(decoder, value)?))
        }
        unknown => Err(decoder.unknown_union("NodeDetectedProperties.gpus", unknown.0)),
    }
}

fn decode_providers(
    decoder: Decoder<'_>,
    runtime: wire::NodeOnnxRuntime<'_>,
) -> Result<Detected<ExecutionProviders>, Report<WireDecodeError>> {
    match runtime.providers_type() {
        wire::NodeProvidersDetection::NodeDetectedProviders => {
            let value = runtime
                .providers_as_node_detected_providers()
                .assured("the verified providers union carries values");
            let values = value.values();
            decoder.entries("NodeDetectedProviders.values", values.len())?;
            let mut providers = Vec::with_capacity(values.len());
            for value in values.iter() {
                providers.push(property_text(
                    decoder,
                    "NodeDetectedProviders.value",
                    value,
                )?);
            }
            let providers = ExecutionProviders::try_from(providers).change_context(
                WireDecodeError::InvalidValue {
                    field: "NodeDetectedProviders.values",
                    kind: "bounded provider list",
                },
            )?;
            Ok(Detected::Value(providers))
        }
        wire::NodeProvidersDetection::NodeUndetected => {
            let value = runtime
                .providers_as_node_undetected()
                .assured("the verified providers union carries a reason");
            Ok(Detected::Undetected(decode_reason(decoder, value)?))
        }
        unknown => Err(decoder.unknown_union("NodeOnnxRuntime.providers", unknown.0)),
    }
}

fn decode_onnx(
    decoder: Decoder<'_>,
    value: wire::NodeDetectedProperties<'_>,
) -> Result<OnnxRuntimeDetection, Report<WireDecodeError>> {
    match value.onnx_type() {
        wire::NodeOnnxDetection::NodeOnnxRuntime => {
            let runtime = value
                .onnx_as_node_onnx_runtime()
                .assured("the verified ONNX union carries a runtime");
            let version = decode_text_detection(
                decoder,
                runtime.version_as_node_detected_text(),
                runtime.version_as_node_undetected(),
                runtime.version_type(),
            )?;
            let providers = decode_providers(decoder, runtime)?;
            let cuda = decoder.required_enumeration("NodeOnnxRuntime.cuda", runtime.cuda())?;
            Ok(OnnxRuntimeDetection::Loaded(OnnxRuntime {
                version,
                providers,
                cuda,
            }))
        }
        wire::NodeOnnxDetection::NodeUndetected => {
            let value = value
                .onnx_as_node_undetected()
                .assured("the verified ONNX union carries a reason");
            Ok(OnnxRuntimeDetection::Undetected(decode_reason(
                decoder, value,
            )?))
        }
        unknown => Err(decoder.unknown_union("NodeDetectedProperties.onnx", unknown.0)),
    }
}

fn decode_properties(
    decoder: Decoder<'_>,
    properties: wire::NodeDetectedProperties<'_>,
) -> Result<NodeProperties, Report<WireDecodeError>> {
    let architecture = property_text(
        decoder,
        "NodeDetectedProperties.architecture",
        properties.architecture(),
    )?;
    let cpu_model = decode_text_detection(
        decoder,
        properties.cpu_model_as_node_detected_text(),
        properties.cpu_model_as_node_undetected(),
        properties.cpu_model_type(),
    )?;
    let gpus = decode_gpus(decoder, properties)?;
    let onnx = decode_onnx(decoder, properties)?;
    Ok(NodeProperties {
        architecture,
        cpu_model,
        gpus,
        onnx,
    })
}

#[cfg(test)]
pub(crate) mod tests;
