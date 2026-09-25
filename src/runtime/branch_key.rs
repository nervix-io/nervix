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

    pub(crate) fn from_fields(
        fields: impl IntoIterator<Item = (FieldName, RuntimeValue)>,
    ) -> error_stack::Result<Self, BranchKeyError> {
        let fields = fields.into_iter().collect::<BTreeMap<_, _>>();
        if fields.is_empty() {
            return Err(Report::new(BranchKeyError::NoFields));
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
            values.insert(name, RuntimeValue::from_remote(field.value));
        }
        let key = Self::from_fields(values)?;
        Ok(Some(key))
    }

    pub(in crate::runtime) fn to_remote_key(key: &Option<Self>) -> Option<Vec<RemoteRuntimeField>> {
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
}

pub(super) fn branch_key_display(key: &Option<BranchKey>) -> &str {
    match key {
        Some(key) => key.as_str(),
        None => "none",
    }
}

impl std::fmt::Display for BranchKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
