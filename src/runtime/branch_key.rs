use super::*;

#[derive(Debug, PartialEq, Eq, Hash)]
struct BranchKeyInner {
    fields: BTreeMap<FieldName, RuntimeValue>,
    json: String,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BranchKey(Arc<BranchKeyInner>);

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
    ) -> Result<Self, String> {
        let fields = fields.into_iter().collect::<BTreeMap<_, _>>();
        if fields.is_empty() {
            return Err("branch key must contain at least one field".to_string());
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
    ) -> Result<Option<Self>, String> {
        let Some(fields) = fields else {
            return Ok(None);
        };
        let mut values = BTreeMap::new();
        for field in fields {
            let name = FieldName::try_from(field.name.clone()).map_err(|error| {
                format!(
                    "remote branch key field '{}' is invalid: {error}",
                    field.name
                )
            })?;
            values.insert(name, RuntimeValue::from_remote(field.value));
        }
        Self::from_fields(values).map(Some)
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
        Self::fingerprint_of_canonical_text(self.as_str())
    }

    /// The identity of the concrete branch whose canonical key text is `text`. A stored runtime
    /// state key carries that text, so its branch is identified without decoding a key.
    pub(in crate::runtime) fn fingerprint_of_canonical_text(text: &str) -> BranchKeyFingerprint {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/branch-key");
        hasher.update(text.as_bytes());
        BranchKeyFingerprint::new(*hasher.finalize().as_bytes())
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
        assert!(BranchKey::from_fields([]).is_err());
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
            BranchKey::fingerprint_of_canonical_text(acme.as_str())
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
