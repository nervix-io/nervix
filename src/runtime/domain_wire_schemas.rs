use super::*;

/// The wire schemas one domain's graph declares, kept in one map per wire format.
///
/// A codec's format decides which map its reference is looked up in, so a JSON codec can only
/// ever be handed a JSON wire schema. Nothing here can pair a format with a definition of another
/// kind, and no caller has to check that it did not.
#[derive(Debug, Default)]
pub(super) struct DomainWireSchemas {
    json: HashMap<WireSchemaName, CreateJsonWireSchema>,
    cbor: HashMap<WireSchemaName, CreateCborWireSchema>,
    avro: HashMap<WireSchemaName, CreateAvroWireSchema>,
}

impl DomainWireSchemas {
    pub(super) fn insert_json(&mut self, wire_schema: CreateJsonWireSchema) {
        self.json.insert(wire_schema.name.clone(), wire_schema);
    }

    pub(super) fn insert_cbor(&mut self, wire_schema: CreateCborWireSchema) {
        self.cbor.insert(wire_schema.name.clone(), wire_schema);
    }

    pub(super) fn insert_avro(&mut self, wire_schema: CreateAvroWireSchema) {
        self.avro.insert(wire_schema.name.clone(), wire_schema);
    }

    /// Pairs `wire_format` with the wire schema it names, reporting the domain a missing wire
    /// schema belongs to.
    pub(super) fn resolve<'a>(
        &'a self,
        domain: &DomainName,
        wire_format: &'a CodecWireFormat,
    ) -> Result<ResolvedCodecWireFormat<'a>, RuntimeError> {
        wire_format
            .resolve(self)
            .map_err(|missing| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!("missing compiled wire schema '{}'", missing.as_str()),
            })
    }
}

impl WireSchemaLookup for DomainWireSchemas {
    /// The name that no compiled wire schema was found for.
    type Error = WireSchemaName;

    fn json_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateJsonWireSchema, Self::Error> {
        self.json.get(name).ok_or_else(|| name.clone())
    }

    fn cbor_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateCborWireSchema, Self::Error> {
        self.cbor.get(name).ok_or_else(|| name.clone())
    }

    fn avro_wire_schema(
        &self,
        name: &WireSchemaName,
    ) -> Result<&CreateAvroWireSchema, Self::Error> {
        self.avro.get(name).ok_or_else(|| name.clone())
    }
}
