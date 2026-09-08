use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BranchKey {
    pub(super) fields: BTreeMap<FieldName, RuntimeValue>,
    pub(super) json: String,
}

impl BranchKey {
    pub(crate) fn field_value(&self, name: &str) -> Option<&RuntimeValue> {
        self.fields
            .iter()
            .find_map(|(field, value)| (field.as_str() == name).then_some(value))
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
        Ok(Self { fields, json })
    }

    pub(super) fn from_remote_record<'a>(
        record: &nervix_models::RemoteRuntimeRecord,
        field_names: impl IntoIterator<Item = &'a FieldName>,
    ) -> Result<Option<Self>, String> {
        let mut fields = BTreeMap::new();
        for field_name in field_names {
            let Some(field) = record
                .fields
                .iter()
                .find(|field| field.name == field_name.as_str())
            else {
                return Ok(None);
            };
            fields.insert(
                field_name.clone(),
                RuntimeValue::from_remote(field.value.clone()),
            );
        }
        Self::from_fields(fields).map(Some)
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

    pub(crate) fn to_remote_key(key: &Option<Self>) -> Option<Vec<RemoteRuntimeField>> {
        key.as_ref().map(|key| {
            key.fields
                .iter()
                .map(|(name, value)| RemoteRuntimeField {
                    name: name.as_str().to_string(),
                    value: value.to_remote(),
                })
                .collect()
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        self.json.as_str()
    }
}

pub(super) fn branch_key_display(key: &Option<BranchKey>) -> &str {
    key.as_ref().map(BranchKey::as_str).unwrap_or("none")
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
}
