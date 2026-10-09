use error_stack::ResultExt as _;

use super::*;

#[derive(Debug, PartialEq, Eq, Hash)]
struct BranchKeyInner {
    fields: BTreeMap<FieldName, RuntimeValue>,
    json: String,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BranchKey(Arc<BranchKeyInner>);

/// Why a set of fields does not form a concrete branch key.
///
/// A concrete branch key is non-empty and typed, and unbranched execution is an absent key rather
/// than any value of [`BranchKey`]. No variant carries or names a branch, so a failure can never be
/// read as an empty or synthetic root branch.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum BranchKeyError {
    #[error("a concrete branch key names at least one field")]
    NoFields,
    #[error("remote branch key field '{field}' is not a valid field name")]
    RemoteFieldName { field: String },
    #[error("remote branch key field '{field}' holds a value that is not a runtime value")]
    RemoteFieldValue { field: FieldName },
    #[error("branch key field '{field}' holds a non-finite float")]
    NonFiniteFloat { field: FieldName },
}

impl std::fmt::Debug for BranchKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BranchKey")
            .field("fields", &self.0.fields)
            .field("json", &self.0.json)
            .finish()
    }
}

impl BranchKey {
    pub(crate) fn field_value(&self, name: &str) -> Option<&RuntimeValue> {
        self.0.fields.get(name)
    }

    pub(crate) fn field_count(&self) -> usize {
        self.0.fields.len()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies an iterator of typed branch fields")
    )]
    pub(crate) fn from_fields(
        fields: impl IntoIterator<Item = (FieldName, RuntimeValue)>,
    ) -> error_stack::Result<Self, BranchKeyError> {
        let fields = fields.into_iter().collect::<BTreeMap<_, _>>();
        if fields.is_empty() {
            return Err(Report::new(BranchKeyError::NoFields));
        }
        // The VM turns a non-finite float result into a row error, so a branch is keyed only by
        // finite values; a non-finite float also has no canonical text to key a branch by. Only a
        // damaged checkpoint or a malformed peer frame names one, and it is refused here.
        for (field, value) in &fields {
            if !value.holds_only_finite_floats() {
                return Err(Report::new(BranchKeyError::NonFiniteFloat {
                    field: field.clone(),
                }));
            }
        }
        let mut object = serde_json::Map::new();
        for (field, value) in &fields {
            object.insert(field.as_str().to_string(), value.to_json_value());
        }
        let json = serde_json::Value::Object(object).to_string();
        Ok(Self(Arc::new(BranchKeyInner { fields, json })))
    }

    pub(crate) fn from_remote_key(
        fields: Option<Vec<RemoteRuntimeField>>,
    ) -> error_stack::Result<Option<Self>, BranchKeyError> {
        let Some(fields) = fields else {
            return Ok(None);
        };
        let mut values = BTreeMap::new();
        for field in fields {
            let name = FieldName::parse(&field.name).change_context_lazy(|| {
                BranchKeyError::RemoteFieldName {
                    field: field.name.clone(),
                }
            })?;
            let value = RuntimeValue::from_remote(field.value).change_context_lazy(|| {
                BranchKeyError::RemoteFieldValue {
                    field: name.clone(),
                }
            })?;
            values.insert(name, value);
        }
        let key = Self::from_fields(values)?;
        Ok(Some(key))
    }

    pub(crate) fn to_remote_key(key: &Option<Self>) -> Option<Vec<RemoteRuntimeField>> {
        key.as_ref().map(|key| {
            key.0
                .fields
                .iter()
                .map(|(name, value)| RemoteRuntimeField {
                    name: name.as_str().to_string(),
                    value: value.to_remote(),
                })
                .collect()
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.json.as_str()
    }

    /// The non-sensitive identity the control plane names this concrete branch by.
    pub(crate) fn fingerprint(&self) -> BranchKeyFingerprint {
        BranchKeyFingerprint::of_canonical_text(self.as_str())
    }

    /// Orders two branch scopes by their canonical key text, the unbranched execution first, so
    /// that reports and published generations list their branches in one stable order.
    pub(crate) fn canonical_order(left: &Option<Self>, right: &Option<Self>) -> std::cmp::Ordering {
        match (left, right) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(left), Some(right)) => left.as_str().cmp(right.as_str()),
        }
    }
}

#[cfg(test)]
impl BranchKey {
    /// A concrete branch key of one to three fields, each holding a runtime value of any kind a
    /// branch field reaches, for generated storage and wire properties.
    pub(in crate::runtime) fn generated(arbitrary: &mut nervix_arbitrary::Arbitrary<'_>) -> Self {
        use meticulous::ResultExt as _;

        let mut fields = BTreeMap::new();
        let count = arbitrary.entropy().between(1..=3);
        for _ in 0..count {
            let name = arbitrary.rule_name::<FieldName>();
            let value = Self::generated_value(arbitrary, 2);
            fields.insert(name, value);
        }
        Self::from_fields(fields).assured("at least one field was drawn above")
    }

    /// A concrete branch key, or the absent key of unbranched execution.
    pub(in crate::runtime) fn generated_scope(
        arbitrary: &mut nervix_arbitrary::Arbitrary<'_>,
    ) -> Option<Self> {
        if arbitrary.entropy().flag() {
            Some(Self::generated(arbitrary))
        } else {
            None
        }
    }

    /// A runtime value of any kind a branch field holds: integers of every width, booleans, text, a
    /// datetime anywhere a timestamp reaches with a whole-minute offset as RFC 3339 writes one,
    /// finite floats of any bit pattern, signed zeros and subnormals included, and arrays and lists
    /// nested `depth` levels at most. The VM turns a non-finite float result into a row error, so
    /// no branch is keyed by one.
    fn generated_value(arbitrary: &mut nervix_arbitrary::Arbitrary<'_>, depth: u8) -> RuntimeValue {
        use meticulous::{OptionExt as _, ResultExt as _};

        let kinds = if depth == 0 { 13 } else { 15 };
        match arbitrary.entropy().byte() % kinds {
            0 => RuntimeValue::String(arbitrary.string()),
            1 => RuntimeValue::Bool(arbitrary.entropy().flag()),
            2 => RuntimeValue::U8(arbitrary.entropy().byte()),
            3 => {
                let bits = u8::try_from(arbitrary.entropy().up_to(u64::from(u8::MAX)))
                    .verified("the draw ends at u8::MAX");
                RuntimeValue::I8(bits.cast_signed())
            }
            4 => RuntimeValue::U16(
                u16::try_from(arbitrary.entropy().up_to(u64::from(u16::MAX)))
                    .verified("the draw ends at u16::MAX"),
            ),
            5 => {
                let bits = u16::try_from(arbitrary.entropy().up_to(u64::from(u16::MAX)))
                    .verified("the draw ends at u16::MAX");
                RuntimeValue::I16(bits.cast_signed())
            }
            6 => RuntimeValue::U32(
                u32::try_from(arbitrary.entropy().up_to(u64::from(u32::MAX)))
                    .verified("the draw ends at u32::MAX"),
            ),
            7 => {
                let bits = u32::try_from(arbitrary.entropy().up_to(u64::from(u32::MAX)))
                    .verified("the draw ends at u32::MAX");
                RuntimeValue::I32(bits.cast_signed())
            }
            8 => RuntimeValue::U64(arbitrary.entropy().any_u64()),
            9 => RuntimeValue::I64(arbitrary.entropy().any_i64()),
            10 => {
                let instant = chrono::DateTime::from_timestamp_nanos(arbitrary.entropy().any_i64());
                let minutes = arbitrary.entropy().up_to(2 * 1439);
                let minutes = i32::try_from(minutes).verified("the draw ends below i32::MAX");
                let minutes = minutes
                    .checked_sub(1439)
                    .verified("the draw stays within a day either side of zero");
                let offset_seconds = minutes
                    .checked_mul(60)
                    .verified("a whole-minute offset within a day fits in i32");
                let offset = chrono::FixedOffset::east_opt(offset_seconds)
                    .assured("an offset of less than a day is a valid fixed offset");
                RuntimeValue::Datetime(instant.with_timezone(&offset))
            }
            11 => {
                let bits = u32::try_from(arbitrary.entropy().up_to(u64::from(u32::MAX)))
                    .verified("the draw ends at u32::MAX");
                let value = f32::from_bits(bits);
                let value = if value.is_finite() { value } else { -0.0 };
                RuntimeValue::F32(OrderedFloat(value))
            }
            12 => {
                let value = f64::from_bits(arbitrary.entropy().any_u64());
                let value = if value.is_finite() {
                    value
                } else {
                    f64::MIN_POSITIVE
                };
                RuntimeValue::F64(OrderedFloat(value))
            }
            13 => {
                let next = depth
                    .checked_sub(1)
                    .verified("a nested kind is drawn above depth zero");
                let elements =
                    arbitrary.records(|arbitrary| Self::generated_value(arbitrary, next));
                RuntimeValue::Array(elements)
            }
            _ => {
                let next = depth
                    .checked_sub(1)
                    .verified("a nested kind is drawn above depth zero");
                let elements =
                    arbitrary.records(|arbitrary| Self::generated_value(arbitrary, next));
                RuntimeValue::Vec(elements)
            }
        }
    }
}

/// The execution a branch scope selects, as errors, runtime events, negative acknowledgements and
/// logs name it: a concrete branch by the fingerprint of its key, or the unbranched execution.
///
/// A branch schema may declare its key fields `SENSITIVE`, and these texts reach sessions as server
/// notices and logs as they are, so a branch is never named by its key's field values. It reads as
/// `branch <fingerprint>`, with the lowercase hexadecimal fingerprint `DESCRIBE` and backup
/// inspection print for the same branch, or as `unbranched`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BranchScope(Option<BranchKeyFingerprint>);

impl From<&Option<BranchKey>> for BranchScope {
    fn from(key: &Option<BranchKey>) -> Self {
        match key {
            Some(key) => Self(Some(key.fingerprint())),
            None => Self(None),
        }
    }
}

impl std::fmt::Display for BranchScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(fingerprint) => write!(formatter, "branch {fingerprint}"),
            None => formatter.write_str("unbranched"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{branch_runtime::BranchEntrypointError, test_fixtures::string_branch_key};

    /// How every diagnostic names the branch keyed by tenant `acme-secret`.
    const SECRET_TENANT_BRANCH: &str =
        "branch 3525112b62a3c2262596c624543a4333a324f9e7bc945c6031936604e166af12";

    /// A branch is named by the lowercase hexadecimal fingerprint of its key, the text `DESCRIBE`
    /// prints for the same branch, and never by the key's values, which a branch schema may declare
    /// `SENSITIVE`. Unbranched execution has no key to name.
    #[test]
    fn a_branch_scope_names_a_concrete_branch_by_the_fingerprint_of_its_key() {
        let secret = string_branch_key("tenant", "acme-secret");

        assert_eq!(BranchScope::from(&secret).to_string(), SECRET_TENANT_BRANCH);
        assert_eq!(BranchScope::from(&None).to_string(), "unbranched");
    }

    /// Every error a branch-local failure is reported with names its branch through its scope, so
    /// a key value never reaches the runtime events, negative acknowledgements and logs the failure
    /// is rendered into.
    #[test]
    fn every_branch_local_failure_names_its_branch_by_fingerprint() {
        let branch = BranchScope::from(&string_branch_key("tenant", "acme-secret"));
        let processor = ModelName::parse("orders")
            .assured("the fixed test literal satisfies the model name grammar");
        let failures = [
            ProcessorBranchTaskError::Instantiate { branch }.to_string(),
            ProcessorBranchTaskError::InitializeWindow { branch }.to_string(),
            ProcessorBranchTaskError::AcceptedInputClock { branch }.to_string(),
            ProcessorBranchTaskError::Unavailable { branch }.to_string(),
            ProcessorBranchTaskError::HandedOffLifetimeAfterLifecycle {
                branch,
                incarnation: 7,
                lsm: 3,
            }
            .to_string(),
            ProcessorMaterializedError::DomainRouting { branch }.to_string(),
            ProcessorMaterializedError::Resolve { branch }.to_string(),
            ProcessorMaterializedError::EvictedRequiredSkip { branch }.to_string(),
            ProcessorMaterializedError::EvictedRequiredWait { branch }.to_string(),
            ProcessorLiveStateError { branch }.to_string(),
            ProcessorTemplateError::ReplicatedState {
                kind: ModelKind::Deduplicator,
                processor: processor.clone(),
                branch,
            }
            .to_string(),
            ProcessorTemplateError::WindowRestore { processor, branch }.to_string(),
            CorrelatorError::MixedBranchKeys {
                left: branch,
                right: BranchScope::from(&None),
            }
            .to_string(),
            BranchEntrypointError::Instantiate { branch }.to_string(),
            BranchEntrypointError::DispatchTask { branch }.to_string(),
        ];

        for failure in failures {
            assert!(failure.contains(SECRET_TENANT_BRANCH), "{failure}");
            assert!(!failure.contains("acme-secret"), "{failure}");
        }
    }

    /// Reports list branches by their canonical key text, with unbranched execution first.
    #[test]
    fn branch_scopes_order_by_canonical_key_text_with_unbranched_execution_first() {
        let acme = string_branch_key("tenant", "acme");
        let beta = string_branch_key("tenant", "beta");
        let mut scopes = [beta.clone(), None, acme.clone(), beta.clone()];

        scopes.sort_by(BranchKey::canonical_order);

        assert_eq!(scopes, [None, acme, beta.clone(), beta]);
    }

    #[test]
    fn branch_key_rejects_empty_fields() {
        let error = BranchKey::from_fields([]).expect_err("an empty field set is not a branch key");

        assert_eq!(error.current_context(), &BranchKeyError::NoFields);
    }

    #[test]
    fn a_remote_key_decodes_to_an_absent_or_concrete_branch() {
        assert_eq!(BranchKey::from_remote_key(None).ok(), Some(None));

        let fields = vec![RemoteRuntimeField {
            name: "tenant".to_string(),
            value: RuntimeValue::String("acme".to_string()).to_remote(),
        }];
        let key = BranchKey::from_remote_key(Some(fields))
            .assured("a valid remote field decodes to a branch key")
            .assured("a present remote key decodes to a present branch key");

        assert_eq!(key.as_str(), r#"{"tenant":"acme"}"#);
    }

    #[test]
    fn a_remote_key_rejects_an_empty_field_set_and_an_invalid_field_name() {
        let empty = BranchKey::from_remote_key(Some(Vec::new()))
            .expect_err("a present remote key must name at least one field");
        assert_eq!(empty.current_context(), &BranchKeyError::NoFields);

        let invalid = BranchKey::from_remote_key(Some(vec![RemoteRuntimeField {
            name: "bad/name".to_string(),
            value: RuntimeValue::String("acme".to_string()).to_remote(),
        }]))
        .expect_err("a remote field must carry a valid field name");
        assert_eq!(
            invalid.current_context(),
            &BranchKeyError::RemoteFieldName {
                field: "bad/name".to_string()
            }
        );
    }

    /// Stored and peer branch keys carry floats as bits, so a non-finite float, which no branch is
    /// keyed by and whose canonical text cannot be written, fails the key's typed validation
    /// instead of stopping the node that reads it.
    #[test]
    fn a_remote_key_rejects_a_non_finite_float() {
        for value in [
            nervix_models::RemoteRuntimeValue::F64(f64::NAN),
            nervix_models::RemoteRuntimeValue::F64(f64::NEG_INFINITY),
            nervix_models::RemoteRuntimeValue::F32(f32::INFINITY),
        ] {
            let malformed = BranchKey::from_remote_key(Some(vec![RemoteRuntimeField {
                name: "reading".to_string(),
                value,
            }]));

            let error = malformed.expect_err("a non-finite float must not form a branch key");
            assert_eq!(
                error.current_context(),
                &BranchKeyError::NonFiniteFloat {
                    field: FieldName::parse("reading")
                        .assured("the fixed test literal satisfies the field name grammar"),
                }
            );
        }
    }

    /// Stored and peer branch keys carry a datetime as text, so text that is not RFC 3339 fails
    /// the key's typed validation instead of stopping the node that reads it.
    #[test]
    fn a_remote_key_rejects_datetime_text_it_cannot_read() {
        let malformed = BranchKey::from_remote_key(Some(vec![RemoteRuntimeField {
            name: "seen_at".to_string(),
            value: nervix_models::RemoteRuntimeValue::Datetime("2026-13-40T25:61:61Z".to_string()),
        }]))
        .expect_err("a datetime field whose text is not RFC 3339 must not form a branch key");

        assert_eq!(
            malformed.current_context(),
            &BranchKeyError::RemoteFieldValue {
                field: FieldName::parse("seen_at")
                    .assured("the fixed test literal satisfies the field name grammar"),
            }
        );
    }

    /// A stored key carries only a branch's canonical text, and the control plane names the branch
    /// by the fingerprint of its typed key, so both must identify the same branch.
    #[test]
    fn a_branch_fingerprint_is_the_fingerprint_of_its_canonical_text() {
        let tenant = |value: &str| {
            BranchKey::from_fields([(
                FieldName::parse("tenant")
                    .assured("the fixed test literal satisfies the field name grammar"),
                RuntimeValue::String(value.to_string()),
            )])
            .assured("the fixed field produces a non-empty branch key")
        };
        let acme = tenant("acme");

        assert_eq!(
            acme.fingerprint(),
            BranchKeyFingerprint::of_canonical_text(acme.as_str())
        );
        assert_ne!(acme.fingerprint(), tenant("beta").fingerprint());
    }

    #[test]
    fn cloning_names_and_branch_keys_does_not_allocate() {
        let relay = RelayName::parse("orders")
            .assured("the fixed test literal satisfies the relay name grammar");
        let branch = BranchKey::from_fields([(
            FieldName::parse("tenant")
                .assured("the fixed test literal satisfies the field name grammar"),
            RuntimeValue::String("acme".to_string()),
        )])
        .assured("the fixed field produces a non-empty branch key");

        let (name_allocations, ()) = alloc_count::alloc_count!({
            std::hint::black_box(relay.clone());
        });
        let (branch_allocations, ()) = alloc_count::alloc_count!({
            std::hint::black_box(branch.clone());
        });

        assert_eq!(
            (name_allocations.alloc_calls, name_allocations.realloc_calls),
            (0, 0),
            "cloning names must remain allocation-free"
        );
        assert_eq!(
            (
                branch_allocations.alloc_calls,
                branch_allocations.realloc_calls
            ),
            (0, 0),
            "cloning branch keys must remain allocation-free"
        );
    }
}
