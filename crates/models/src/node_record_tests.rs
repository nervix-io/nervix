//! Tests of the node record's tags and detected-property values at their construction boundary.
//!
//! Layer: test harness.
//! - **Owns.** Coverage of the tag rules and bounds, of the property value bounds, and of the
//!   derived facts the record answers.
//! - **Depends on.** The node record vocabulary.
//! - **Must not know.** Detection, consensus or how the record is published.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{collections::BTreeMap, num::NonZeroU64, str::FromStr};

use super::*;

fn tags(text: &str) -> Result<NodeTags, NodeTagError> {
    NodeTags::from_str(text).map_err(|report| report.current_context().clone())
}

#[test]
fn tags_read_in_key_order_and_render_as_the_command_line_writes_them() {
    let read = tags("zone=eu,pool=gpu,rack=r-7.a_1").expect("valid tags");
    let keys = read.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>();
    assert_eq!(keys, ["pool", "rack", "zone"]);
    assert_eq!(read.to_string(), "pool=gpu,rack=r-7.a_1,zone=eu");
    assert_eq!(read.get("zone").map(TagValue::as_str), Some("eu"));
    assert_eq!(read.get("absent"), None);
    assert_eq!(tags(&read.to_string()).expect("its own rendering"), read);
}

#[test]
fn the_first_equals_sign_ends_the_key_so_a_value_may_hold_another() {
    let read = tags("selector=a=b").expect("valid tag");
    assert_eq!(read.get("selector").map(TagValue::as_str), Some("a=b"));
}

#[test]
fn each_tag_rule_refuses_naming_the_tag() {
    assert_eq!(
        tags("pool"),
        Err(NodeTagError::MissingSeparator {
            tag: "pool".to_string()
        })
    );
    assert_eq!(
        tags("=gpu"),
        Err(NodeTagError::EmptyKey {
            tag: "=gpu".to_string()
        })
    );
    for key in ["Pool", "-pool", ".pool", "_pool", "po ol", "pöol", "po/ol"] {
        assert_eq!(
            tags(&format!("{key}=gpu")),
            Err(NodeTagError::KeyAlphabet {
                key: key.to_string()
            }),
            "{key}"
        );
    }
    assert_eq!(
        tags("pool="),
        Err(NodeTagError::EmptyValue {
            key: "pool".to_string()
        })
    );
    for value in ["a\tb", "a\nb", "a\u{2028}b", "a\u{0}b"] {
        assert_eq!(
            tags(&format!("pool={value}")),
            Err(NodeTagError::ValueCharacter {
                key: "pool".to_string()
            }),
            "{value:?}"
        );
    }
    assert_eq!(
        tags("pool=gpu,pool=cpu"),
        Err(NodeTagError::RepeatedKey {
            key: "pool".to_string()
        })
    );
    let message = NodeTagError::KeyAlphabet {
        key: "Pool".to_string(),
    }
    .to_string();
    assert!(message.contains("node tag key 'Pool' must start with a lower-case letter"));
}

#[test]
fn keys_values_and_counts_are_bounded_exactly_at_their_limits() {
    let key = "k".repeat(MAX_TAG_KEY_BYTES);
    assert!(tags(&format!("{key}=v")).is_ok());
    let longer = "k".repeat(MAX_TAG_KEY_BYTES + 1);
    assert_eq!(
        tags(&format!("{longer}=v")),
        Err(NodeTagError::KeyTooLong {
            key: longer,
            actual: MAX_TAG_KEY_BYTES + 1,
            max: MAX_TAG_KEY_BYTES,
        })
    );

    let value = "v".repeat(MAX_TAG_VALUE_BYTES);
    assert!(tags(&format!("pool={value}")).is_ok());
    let multibyte = "é".repeat(MAX_TAG_VALUE_BYTES / 2);
    assert!(tags(&format!("pool={multibyte}")).is_ok());
    let longer = "v".repeat(MAX_TAG_VALUE_BYTES + 1);
    assert_eq!(
        tags(&format!("pool={longer}")),
        Err(NodeTagError::ValueTooLong {
            key: "pool".to_string(),
            actual: MAX_TAG_VALUE_BYTES + 1,
            max: MAX_TAG_VALUE_BYTES,
        })
    );

    let at_bound = (0..MAX_NODE_TAGS)
        .map(|index| format!("k{index}=v"))
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(tags(&at_bound).expect("the bound").len(), MAX_NODE_TAGS);
    let beyond = (0..=MAX_NODE_TAGS)
        .map(|index| format!("k{index}=v"))
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(
        tags(&beyond),
        Err(NodeTagError::TooMany {
            actual: MAX_NODE_TAGS + 1,
            max: MAX_NODE_TAGS,
        })
    );
}

#[test]
fn decoding_validates_every_tag_value_with_its_key() {
    let key = TagKey::parse("pool").expect("valid key");
    let refused = BTreeMap::from([(key, TagValue("a,b".to_string()))]);
    assert_eq!(
        NodeTags::try_from(refused),
        Err(NodeTagError::ValueCharacter {
            key: "pool".to_string()
        })
    );
    let json = serde_json::to_string(&tags("pool=gpu").expect("valid tags")).expect("serializes");
    assert_eq!(json, r#"{"pool":"gpu"}"#);
    assert!(serde_json::from_str::<NodeTags>(r#"{"Pool":"gpu"}"#).is_err());
    assert!(serde_json::from_str::<NodeTags>(r#"{"pool":""}"#).is_err());
}

#[test]
fn property_text_and_lists_hold_only_what_the_record_bounds() {
    assert_eq!(PropertyText::new(""), Err(PropertyValueError::Empty));
    assert!(PropertyText::new("x".repeat(MAX_PROPERTY_TEXT_BYTES)).is_ok());
    assert_eq!(
        PropertyText::new("x".repeat(MAX_PROPERTY_TEXT_BYTES + 1)),
        Err(PropertyValueError::TooLong {
            actual: MAX_PROPERTY_TEXT_BYTES + 1,
            max: MAX_PROPERTY_TEXT_BYTES,
        })
    );
    assert_eq!(
        PropertyText::new("a\nb"),
        Err(PropertyValueError::ControlCharacter)
    );
    assert_eq!(
        GpuDevices::try_from(Vec::new()),
        Err(PropertyValueError::NoDevices)
    );
    let text = PropertyText::new("CPUExecutionProvider").expect("short text");
    let providers = vec![text; MAX_PROPERTY_LIST_ITEMS + 1];
    assert_eq!(
        ExecutionProviders::try_from(providers),
        Err(PropertyValueError::TooManyItems {
            actual: MAX_PROPERTY_LIST_ITEMS + 1,
            max: MAX_PROPERTY_LIST_ITEMS,
        })
    );
}

fn device(memory: Option<u64>) -> GpuDevice {
    GpuDevice {
        model: Detected::Value(PropertyText::new("GPU").expect("short text")),
        memory_bytes: match memory.and_then(NonZeroU64::new) {
            Some(memory) => Detected::Value(memory),
            None => Detected::Undetected(UndetectedReason::QueryFailed),
        },
    }
}

fn properties(gpus: GpuInventory, cuda: CudaProviderRegistration) -> NodeProperties {
    NodeProperties {
        architecture: PropertyText::new("x86_64").expect("short text"),
        cpu_model: Detected::Undetected(UndetectedReason::NotReported),
        gpus,
        onnx: OnnxRuntimeDetection::Loaded(OnnxRuntime {
            version: Detected::Undetected(UndetectedReason::NotReported),
            providers: Detected::Undetected(UndetectedReason::QueryFailed),
            cuda,
        }),
    }
}

#[test]
fn cuda_is_available_exactly_with_a_gpu_and_a_registered_provider() {
    let driver = Detected::Value(PropertyText::new("550.54").expect("short text"));
    let one_gpu = GpuInventory::Devices {
        devices: GpuDevices::try_from(vec![device(Some(16 << 30))]).expect("one device"),
        driver: driver.clone(),
    };
    struct Case {
        gpus: GpuInventory,
        cuda: CudaProviderRegistration,
        available: bool,
    }

    let cases = [
        Case {
            gpus: one_gpu.clone(),
            cuda: CudaProviderRegistration::Registered,
            available: true,
        },
        Case {
            gpus: one_gpu.clone(),
            cuda: CudaProviderRegistration::NotInLibrary,
            available: false,
        },
        Case {
            gpus: one_gpu,
            cuda: CudaProviderRegistration::RegistrationFailed,
            available: false,
        },
        Case {
            gpus: GpuInventory::NoDevice {
                driver: driver.clone(),
            },
            cuda: CudaProviderRegistration::Registered,
            available: false,
        },
        Case {
            gpus: GpuInventory::Undetected(UndetectedReason::LibraryUnavailable),
            cuda: CudaProviderRegistration::Registered,
            available: false,
        },
    ];
    for case in cases {
        assert_eq!(
            properties(case.gpus, case.cuda).cuda_available(),
            case.available
        );
    }
    let undetected_runtime = NodeProperties {
        onnx: OnnxRuntimeDetection::Undetected(UndetectedReason::LibraryUnavailable),
        ..properties(
            GpuInventory::NoDevice { driver },
            CudaProviderRegistration::Registered,
        )
    };
    assert!(!undetected_runtime.onnx.cuda());
}

#[test]
fn the_gpu_inventory_answers_count_smallest_memory_and_driver_by_its_outcome() {
    let inventory = GpuInventory::Devices {
        devices: GpuDevices::try_from(vec![
            device(Some(24 << 30)),
            device(None),
            device(Some(16 << 30)),
        ])
        .expect("three devices"),
        driver: Detected::Value(PropertyText::new("550.54").expect("short text")),
    };
    assert_eq!(inventory.count(), 3);
    assert_eq!(inventory.smallest_memory_bytes(), NonZeroU64::new(16 << 30));
    assert_eq!(
        inventory.driver().value().map(|driver| driver.as_str()),
        Some("550.54")
    );
    let unreadable = GpuInventory::Devices {
        devices: GpuDevices::try_from(vec![device(None)]).expect("one device"),
        driver: Detected::Undetected(UndetectedReason::QueryFailed),
    };
    assert_eq!(unreadable.count(), 1);
    assert_eq!(unreadable.smallest_memory_bytes(), None);
    let without_library = GpuInventory::Undetected(UndetectedReason::LibraryUnavailable);
    assert_eq!(without_library.count(), 0);
    assert!(without_library.devices().is_empty());
    assert_eq!(
        without_library.driver(),
        Detected::Undetected(UndetectedReason::LibraryUnavailable)
    );
}

#[test]
fn properties_read_back_their_json_form_and_every_outcome_has_its_own_text() {
    let text = |text: &str| PropertyText::new(text).expect("short text");
    let properties = NodeProperties {
        architecture: text("x86_64"),
        cpu_model: Detected::Value(text("Test CPU")),
        gpus: GpuInventory::Devices {
            devices: GpuDevices::try_from(vec![device(Some(16 << 30)), device(None)])
                .expect("two devices"),
            driver: Detected::Value(text("550.54")),
        },
        onnx: OnnxRuntimeDetection::Loaded(OnnxRuntime {
            version: Detected::Value(text("1.30.0")),
            providers: Detected::Value(
                ExecutionProviders::try_from(vec![
                    text("CUDAExecutionProvider"),
                    text("CPUExecutionProvider"),
                ])
                .expect("two providers"),
            ),
            cuda: CudaProviderRegistration::Registered,
        }),
    };
    let json = serde_json::to_string(&properties).expect("properties serialize");
    let read = serde_json::from_str::<NodeProperties>(&json).expect("their own form reads back");
    assert_eq!(read, properties);

    let devices = vec![device(Some(1)); MAX_PROPERTY_LIST_ITEMS + 1];
    assert_eq!(
        GpuDevices::try_from(devices),
        Err(PropertyValueError::TooManyItems {
            actual: MAX_PROPERTY_LIST_ITEMS + 1,
            max: MAX_PROPERTY_LIST_ITEMS,
        })
    );
    assert_eq!(
        TagKey::parse(""),
        Err(NodeTagError::EmptyKey { tag: String::new() })
    );
    let tag = NodeTag::from_str("pool=a=b").expect("valid tag");
    assert_eq!(tag.to_string(), "pool=a=b");

    let reasons = [
        UndetectedReason::NotReported,
        UndetectedReason::LibraryUnavailable,
        UndetectedReason::DriverUnavailable,
        UndetectedReason::QueryFailed,
        UndetectedReason::DeadlineExpired,
        UndetectedReason::DetectionFailed,
        UndetectedReason::ExceedsRecordBound,
    ];
    let mut reason_texts = std::collections::BTreeSet::new();
    for reason in reasons {
        reason_texts.insert(reason.to_string());
    }
    assert_eq!(reason_texts.len(), reasons.len());
    let registrations = [
        CudaProviderRegistration::Registered,
        CudaProviderRegistration::NotInLibrary,
        CudaProviderRegistration::RegistrationFailed,
    ];
    let mut registration_texts = std::collections::BTreeSet::new();
    for registration in registrations {
        registration_texts.insert(registration.to_string());
    }
    assert_eq!(registration_texts.len(), registrations.len());
}
