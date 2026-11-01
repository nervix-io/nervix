//! The declarations that nest source and emitter Models connector by entity.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The shape every connector's entities and the unions over them are declared in: a
//!   data enum over the entity Models, the kind that names each entity, the alterations of the
//!   ingestors that read each entity, and the conversions from an entity Model or its operations
//!   into the enums that hold them.
//! - **Depends on.** The source and emitter declarations each entity Model implements, and the
//!   alteration operations an ingestor applies.
//! - **Must not know.** Any one connector, entity or capability.
//!
//! A connector module declares its own entities with [`connector_ingestors`] and
//! [`connector_emitters`]. The unions are declared once each with [`entity_union`] and
//! [`ingestor_alteration_union`], which list connectors and never entities, so adding an entity to
//! a connector touches that connector's module alone.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

/// Declares the kind that names each source entity of one connector, and the pair each entity
/// Model is a source of.
///
/// Each line gives the entity's keyword and the label a form shows for it. What the entity
/// declares is read from its Model's [`IngestorDeclaration`](crate::IngestorDeclaration) implementation, so the
/// kind states nothing a second time.
macro_rules! connector_ingestor_kinds {
    (
        connector $Connector:ident = $connector_keyword:literal;
        $(#[$entity_meta:meta])*
        pub enum $Entity:ident {
            $(
                $(#[$variant_meta:meta])*
                $Variant:ident($Model:ident) = $keyword:literal, $form_label:literal;
            )+
        }
    ) => {
        $(#[$entity_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $Entity {
            $(
                $(#[$variant_meta])*
                $Variant,
            )+
        }

        impl $Entity {
            /// Every source entity of this connector, in declared order.
            pub const ALL: &'static [Self] = &[$(Self::$Variant,)+];
        }

        impl $crate::connector::ingestor::IngestorEntityMetadata for $Entity {
            fn connector(&self) -> $crate::ConnectorKind {
                $crate::ConnectorKind::$Connector
            }

            fn keyword(&self) -> &'static str {
                match self {
                    $(Self::$Variant => $keyword,)+
                }
            }

            fn phrase(&self) -> &'static str {
                match self {
                    $(Self::$Variant => concat!($connector_keyword, " ", $keyword),)+
                }
            }

            fn form_label(&self) -> &'static str {
                match self {
                    $(Self::$Variant => $form_label,)+
                }
            }

            fn capabilities(&self) -> $crate::IngestorEntityCapabilities {
                match self {
                    $(Self::$Variant => <$Model as $crate::IngestorDeclaration>::CAPABILITIES,)+
                }
            }
        }

        $(
            impl $crate::IngestorEntityModel for $Model {
                const KIND: $crate::IngestorEntityKind =
                    $crate::IngestorEntityKind::$Connector($Entity::$Variant);
            }
        )+
    };
}

/// The guarantee every operation of an alteration relies on to find its pair's Model in the input
/// it changes.
pub(crate) const ALTERED_PAIR_CHECKED: &str =
    "apply_alter compared the stored pair with the pair the alteration's head names before \
     applying any operation, and no operation changes the pair an input reads";

/// Declares the sources of one connector: the union of its entity Models, the kind that names each
/// entity, and the alterations of the ingestors that read each entity.
///
/// An alteration of one entity holds operations whose `SET FROM` states that entity's own Model,
/// so it can set no source of another entity, and its operations decode a transport's payloads
/// with the codec `SET DECODE USING` names.
macro_rules! connector_ingestors {
    (
        connector $Connector:ident = $connector_keyword:literal;
        $(#[$source_meta:meta])*
        pub enum $Source:ident;
        $(#[$alteration_meta:meta])*
        pub enum $Alteration:ident;
        $(#[$entity_meta:meta])*
        pub enum $Entity:ident {
            $(
                $(#[$variant_meta:meta])*
                $Variant:ident($Model:ident) = $keyword:literal, $form_label:literal;
            )+
        }
    ) => {
        $(#[$source_meta])*
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::rkyv::Archive,
            ::rkyv::Serialize,
            ::rkyv::Deserialize,
        )]
        pub enum $Source {
            $(
                $(#[$variant_meta])*
                $Variant($Model),
            )+
        }

        impl $Source {
            /// The entity this source reads.
            pub const fn entity(&self) -> $Entity {
                match self {
                    $(Self::$Variant(_) => $Entity::$Variant,)+
                }
            }
        }

        impl $crate::IngestorEntityBody for $Source {
            fn reference(&self) -> $crate::IngestorReference<'_> {
                match self {
                    $(Self::$Variant(source) => $crate::IngestorEntityBody::reference(source),)+
                }
            }

            fn quiesce(&self) -> &$crate::IngestQuiesceMode {
                match self {
                    $(Self::$Variant(source) => $crate::IngestorEntityBody::quiesce(source),)+
                }
            }

            fn quiesce_mut(&mut self) -> &mut $crate::IngestQuiesceMode {
                match self {
                    $(Self::$Variant(source) => $crate::IngestorEntityBody::quiesce_mut(source),)+
                }
            }

            fn acknowledgement(&self) -> $crate::IngestAcknowledgement<'_> {
                match self {
                    $(Self::$Variant(source) => $crate::IngestorEntityBody::acknowledgement(source),)+
                }
            }

            fn chosen(&self, option: &str) -> Option<&'static str> {
                match self {
                    $(Self::$Variant(source) => $crate::IngestorEntityBody::chosen(source, option),)+
                }
            }
        }

        $(
            impl From<$Model> for $Source {
                fn from(source: $Model) -> Self {
                    Self::$Variant(source)
                }
            }

            impl From<$Model> for $crate::IngestorEntity {
                fn from(source: $Model) -> Self {
                    Self::$Connector($Source::$Variant(source))
                }
            }
        )+

        $(#[$alteration_meta])*
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::rkyv::Archive,
            ::rkyv::Serialize,
            ::rkyv::Deserialize,
        )]
        pub enum $Alteration {
            $(
                $(#[$variant_meta])*
                $Variant(Vec<$crate::AlterTransportIngestorOperation<$Model>>),
            )+
        }

        impl $Alteration {
            /// The entity whose ingestors this alteration changes.
            pub const fn entity(&self) -> $Entity {
                match self {
                    $(Self::$Variant(_) => $Entity::$Variant,)+
                }
            }

            /// Applies the operations in order to an ingestor that reads this entity.
            pub(crate) fn apply_to(
                &self,
                ingestor: &mut $crate::CreateIngestor,
            ) -> ::error_stack::Result<(), $crate::AlterIngestorError> {
                match self {
                    $(
                        Self::$Variant(operations) => {
                            ingestor.apply_transport_operations(operations)
                        }
                    )+
                }
            }

            /// The canonical Plexus of each operation, in order.
            pub(crate) fn operations_to_plexus(
                &self,
            ) -> ::error_stack::Result<Vec<String>, $crate::CanonicalPlexusError> {
                match self {
                    $(
                        Self::$Variant(operations) => {
                            $crate::canonical::alter_transport_ingestor_operations_to_plexus(
                                operations,
                            )
                        }
                    )+
                }
            }
        }

        $(
            impl From<Vec<$crate::AlterTransportIngestorOperation<$Model>>> for $Alteration {
                fn from(operations: Vec<$crate::AlterTransportIngestorOperation<$Model>>) -> Self {
                    Self::$Variant(operations)
                }
            }

            impl From<Vec<$crate::AlterTransportIngestorOperation<$Model>>>
                for $crate::IngestorAlteration
            {
                fn from(operations: Vec<$crate::AlterTransportIngestorOperation<$Model>>) -> Self {
                    Self::$Connector($Alteration::$Variant(operations))
                }
            }

            impl $crate::connector::ingestor::AlteredIngestorEntity for $Model {
                fn set_into(&self, input: &mut $crate::IngestorInput) {
                    let source = ::meticulous::OptionExt::verified(
                        input.transport_source_mut(),
                        $crate::connector::entity_union::ALTERED_PAIR_CHECKED,
                    );
                    *source = self.clone().into();
                }

                fn set_quiesce_of(
                    input: &mut $crate::IngestorInput,
                    quiesce: $crate::IngestQuiesceMode,
                ) -> ::error_stack::Result<(), $crate::AlterIngestorError> {
                    let source = ::meticulous::OptionExt::verified(
                        input.transport_source_mut(),
                        $crate::connector::entity_union::ALTERED_PAIR_CHECKED,
                    );
                    source.set_quiesce(quiesce)
                }
            }
        )+

        $crate::connector::entity_union::connector_ingestor_kinds! {
            connector $Connector = $connector_keyword;
            $(#[$entity_meta])*
            pub enum $Entity {
                $(
                    $(#[$variant_meta])*
                    $Variant($Model) = $keyword, $form_label;
                )+
            }
        }
    };
}

/// Declares the emitters of one connector: the union of its entity Models and the kind that names
/// each entity.
///
/// What an entity declares is read from its Model's [`EmitterDeclaration`](crate::EmitterDeclaration)
/// implementation, so the kind states nothing a second time.
macro_rules! connector_emitters {
    (
        connector $Connector:ident = $connector_keyword:literal;
        $(#[$emitter_meta:meta])*
        pub enum $Emitter:ident;
        $(#[$change_meta:meta])*
        pub enum $ModeChange:ident;
        $(#[$entity_meta:meta])*
        pub enum $Entity:ident {
            $(
                $(#[$variant_meta:meta])*
                $Variant:ident($Model:ident, $Mode:ident) = $keyword:literal;
            )+
        }
    ) => {
        $(#[$emitter_meta])*
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::rkyv::Archive,
            ::rkyv::Serialize,
            ::rkyv::Deserialize,
        )]
        pub enum $Emitter {
            $(
                $(#[$variant_meta])*
                $Variant($Model),
            )+
        }

        $(#[$change_meta])*
        #[derive(Debug, Clone, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize,
                 ::rkyv::Archive, ::rkyv::Serialize, ::rkyv::Deserialize)]
        pub enum $ModeChange { $($Variant($Mode),)+ }

        impl $ModeChange {
            pub const fn entity(&self) -> $Entity {
                match self { $(Self::$Variant(_) => $Entity::$Variant,)+ }
            }
            pub fn publishing(&self) -> $crate::EmitterPublishing<'_> {
                match self { $(Self::$Variant(mode) => mode.publishing(),)+ }
            }
        }

        impl $crate::EmitterPublication for $Emitter {
            fn publishing(&self) -> $crate::EmitterPublishing<'_> {
                match self { $(Self::$Variant(destination) => $crate::EmitterPublication::publishing(destination),)+ }
            }
            fn mode_change(&self) -> $crate::EmitterModeChange {
                match self { $(Self::$Variant(destination) => $crate::EmitterPublication::mode_change(destination),)+ }
            }
            fn replace_mode(&mut self, mode: &$crate::EmitterModeChange)
                -> ::error_stack::Result<(), $crate::AlterEmitterError> {
                match self { $(Self::$Variant(destination) => $crate::EmitterPublication::replace_mode(destination, mode),)+ }
            }
        }

        $(
            impl $crate::EmitterEntityModel for $Model {
                type PublishingMode = $Mode;
                const KIND: $crate::EmitterEntityKind = $crate::EmitterEntityKind::$Connector($Entity::$Variant);
            }
            impl $crate::EmitterPublication for $Model {
                fn publishing(&self) -> $crate::EmitterPublishing<'_> { self.mode.publishing() }
                fn mode_change(&self) -> $crate::EmitterModeChange { self.mode.clone().into() }
                fn replace_mode(&mut self, requested: &$crate::EmitterModeChange)
                    -> ::error_stack::Result<(), $crate::AlterEmitterError> {
                    if let $crate::EmitterModeChange::$Connector($ModeChange::$Variant(mode)) = requested {
                        self.mode = mode.clone();
                        Ok(())
                    } else {
                        Err(::error_stack::Report::new($crate::AlterEmitterError::DestinationKindMismatch {
                            stored: <Self as $crate::EmitterEntityModel>::KIND,
                            requested: requested.kind(),
                        }))
                    }
                }
            }
            impl From<$Mode> for $ModeChange {
                fn from(mode: $Mode) -> Self { Self::$Variant(mode) }
            }
            impl From<$Mode> for $crate::EmitterModeChange {
                fn from(mode: $Mode) -> Self { Self::$Connector($ModeChange::$Variant(mode)) }
            }
        )+

        impl $Emitter {
            /// The entity this emitter writes.
            pub const fn entity(&self) -> $Entity {
                match self {
                    $(Self::$Variant(_) => $Entity::$Variant,)+
                }
            }
        }

        impl $crate::EmitterEntityBody for $Emitter {
            fn reference(&self) -> $crate::EmitterReference<'_> {
                match self {
                    $(Self::$Variant(connector) => $crate::EmitterEntityBody::reference(connector),)+
                }
            }

            fn client_mut(&mut self) -> Option<&mut $crate::ClientName> {
                match self {
                    $(Self::$Variant(connector) => $crate::EmitterEntityBody::client_mut(connector),)+
                }
            }

            fn catalog_client(&self) -> Option<$crate::EmitterClientReference<'_>> {
                match self {
                    $(Self::$Variant(connector) => $crate::EmitterEntityBody::catalog_client(connector),)+
                }
            }

            fn value_mappings(&self) -> $crate::EmitterValueMappings<'_> {
                match self {
                    $(Self::$Variant(connector) => $crate::EmitterEntityBody::value_mappings(connector),)+
                }
            }

            fn mapped_value_types(&self) -> Option<$crate::MappedValueTypes> {
                match self {
                    $(Self::$Variant(connector) => $crate::EmitterEntityBody::mapped_value_types(connector),)+
                }
            }

            fn commit_policy(&self) -> Option<(&str, $crate::PayloadSizeLimit)> {
                match self {
                    $(Self::$Variant(connector) => $crate::EmitterEntityBody::commit_policy(connector),)+
                }
            }

            fn routing_key(&self) -> Option<&$crate::Expression> {
                match self {
                    $(Self::$Variant(connector) => $crate::EmitterEntityBody::routing_key(connector),)+
                }
            }
        }

        $(
            impl From<$Model> for $Emitter {
                fn from(connector: $Model) -> Self {
                    Self::$Variant(connector)
                }
            }

            impl From<$Model> for $crate::EmitterEntity {
                fn from(connector: $Model) -> Self {
                    Self::$Connector($Emitter::$Variant(connector))
                }
            }
        )+

        $(#[$entity_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ::serde::Serialize, ::serde::Deserialize, ::rkyv::Archive, ::rkyv::Serialize, ::rkyv::Deserialize)]
        pub enum $Entity {
            $(
                $(#[$variant_meta])*
                $Variant,
            )+
        }

        impl $Entity {
            /// Every emitter entity of this connector, in declared order.
            pub const ALL: &'static [Self] = &[$(Self::$Variant,)+];
        }

        impl $crate::connector::emitter::EmitterEntityMetadata for $Entity {
            fn connector(&self) -> $crate::ConnectorKind {
                $crate::ConnectorKind::$Connector
            }

            fn keyword(&self) -> &'static str {
                match self {
                    $(Self::$Variant => $keyword,)+
                }
            }

            fn phrase(&self) -> &'static str {
                match self {
                    $(Self::$Variant => concat!($connector_keyword, " ", $keyword),)+
                }
            }

            fn capabilities(&self) -> $crate::EmitterEntityCapabilities {
                match self {
                    $(Self::$Variant => <$Model as $crate::EmitterDeclaration>::CAPABILITIES,)+
                }
            }
        }
    };
}

/// Declares one of the two unions over every connector's entities: the Model union and the kind
/// union that names each connector and entity pair.
///
/// A line names a connector with the enums its module declared. A connector listed under `kinds`
/// has entities this union's Models never hold, so it appears in the kind union alone, and a kind
/// union with such a connector also lists the pairs a Model exists for as `modeled`. The union
/// reaches what a Model or a kind declares through `$Body` and `$EntityKind`, which every listed
/// enum implements; the forwarding methods themselves are written beside each invocation.
macro_rules! entity_union {
    // A union that holds a Model of every pair has no narrower list of pairs than `all`.
    (@modeled $Kind:ident; kinds {}; $($Connector:ident($Entity:ident),)+) => {};
    (
        @modeled $Kind:ident;
        kinds { $($KindOnly:ident($KindOnlyEntity:ident),)+ };
        $($Connector:ident($Entity:ident),)+
    ) => {
        impl $Kind {
            /// Every pair this union holds a Model of, connector by connector in declared order.
            pub fn modeled() -> impl Iterator<Item = Self> {
                ::std::iter::empty()
                    $(.chain($Entity::ALL.iter().copied().map(Self::$Connector)))+
            }
        }
    };
    (
        $(#[$union_meta:meta])*
        pub enum $Union:ident as $Body:path;
        $(#[$kind_meta:meta])*
        pub enum $Kind:ident as $EntityKind:path;
        kinds {
            $($KindOnly:ident($KindOnlyEntity:ident),)*
        }
        models {
            $($Connector:ident($Model:ident, $Entity:ident),)+
        }
    ) => {
        $(#[$union_meta])*
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::rkyv::Archive,
            ::rkyv::Serialize,
            ::rkyv::Deserialize,
        )]
        pub enum $Union {
            $($Connector($Model),)+
        }

        impl $Union {
            /// The connector and entity this Model is of.
            pub const fn kind(&self) -> $Kind {
                match self {
                    $(Self::$Connector(model) => $Kind::$Connector(model.entity()),)+
                }
            }

            /// What the entity Model this union holds declares.
            fn body(&self) -> &dyn $Body {
                match self {
                    $(Self::$Connector(model) => model,)+
                }
            }

            /// The mutable form of [`Self::body`].
            fn body_mut(&mut self) -> &mut dyn $Body {
                match self {
                    $(Self::$Connector(model) => model,)+
                }
            }
        }

        $(
            impl From<$Model> for $Union {
                fn from(model: $Model) -> Self {
                    Self::$Connector(model)
                }
            }
        )+

        $(#[$kind_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $Kind {
            $($KindOnly($KindOnlyEntity),)*
            $($Connector($Entity),)+
        }

        impl $Kind {
            /// Every connector and entity pair, connector by connector in declared order.
            pub fn all() -> impl Iterator<Item = Self> {
                ::std::iter::empty()
                    $(.chain($KindOnlyEntity::ALL.iter().copied().map(Self::$KindOnly)))*
                    $(.chain($Entity::ALL.iter().copied().map(Self::$Connector)))+
            }

            /// What the entity of this pair declares.
            fn entity(&self) -> &dyn $EntityKind {
                match self {
                    $(Self::$KindOnly(entity) => entity,)*
                    $(Self::$Connector(entity) => entity,)+
                }
            }
        }

        $crate::connector::entity_union::entity_union!(
            @modeled $Kind;
            kinds { $($KindOnly($KindOnlyEntity),)* };
            $($Connector($Entity),)+
        );

        $(
            impl From<$KindOnlyEntity> for $Kind {
                fn from(entity: $KindOnlyEntity) -> Self {
                    Self::$KindOnly(entity)
                }
            }
        )*

        $(
            impl From<$Entity> for $Kind {
                fn from(entity: $Entity) -> Self {
                    Self::$Connector(entity)
                }
            }
        )+
    };
}

/// Declares the union of every connector's ingestor alterations, which an
/// `ALTER <CONNECTOR> <ENTITY> INGESTOR` statement holds.
///
/// A line names a connector with the alteration enum its module declared, whose variants name the
/// connector's entities. The union states the pair the statement's head names, applies the
/// operations to an ingestor that reads it, and writes them back as canonical Plexus.
macro_rules! ingestor_alteration_union {
    (
        $(#[$union_meta:meta])*
        pub enum $Union:ident;
        $($Connector:ident($Alteration:ident),)+
    ) => {
        $(#[$union_meta])*
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::rkyv::Archive,
            ::rkyv::Serialize,
            ::rkyv::Deserialize,
        )]
        pub enum $Union {
            $($Connector($Alteration),)+
        }

        impl $Union {
            /// The connector and entity the statement's head names.
            pub const fn kind(&self) -> $crate::IngestorEntityKind {
                match self {
                    $(
                        Self::$Connector(alteration) => {
                            $crate::IngestorEntityKind::$Connector(alteration.entity())
                        }
                    )+
                }
            }

            /// Applies the operations in order to an ingestor that reads the head's pair.
            pub(crate) fn apply_to(
                &self,
                ingestor: &mut $crate::CreateIngestor,
            ) -> ::error_stack::Result<(), $crate::AlterIngestorError> {
                match self {
                    $(Self::$Connector(alteration) => alteration.apply_to(ingestor),)+
                }
            }

            /// The canonical Plexus of each operation, in order.
            pub(crate) fn operations_to_plexus(
                &self,
            ) -> ::error_stack::Result<Vec<String>, $crate::CanonicalPlexusError> {
                match self {
                    $(Self::$Connector(alteration) => alteration.operations_to_plexus(),)+
                }
            }
        }

        $(
            impl From<$Alteration> for $Union {
                fn from(alteration: $Alteration) -> Self {
                    Self::$Connector(alteration)
                }
            }
        )+
    };
}

pub(crate) use connector_emitters;
pub(crate) use connector_ingestor_kinds;
pub(crate) use connector_ingestors;
pub(crate) use entity_union;
pub(crate) use ingestor_alteration_union;

/// Declare destination Models, their kinds and policy changes from one connector catalog.
macro_rules! emitter_destinations {
    (
        $(#[$model_meta:meta])* pub enum $Model:ident as $Body:path;
        $(#[$kind_meta:meta])* pub enum $Kind:ident as $EntityKind:path;
        pub enum $Change:ident;
        models { $($Connector:ident($Destination:ident, $Entity:ident, $Mode:ident),)+ }
    ) => {
        $crate::connector::entity_union::entity_union! {
            $(#[$model_meta])* pub enum $Model as $Body;
            $(#[$kind_meta])*
            #[derive(::serde::Serialize, ::serde::Deserialize, ::rkyv::Archive, ::rkyv::Serialize, ::rkyv::Deserialize)]
            pub enum $Kind as $EntityKind;
            kinds {}
            models { $($Connector($Destination, $Entity),)+ }
        }
        /// A typed publishing-policy update whose variant names its exact emitter entity.
        #[derive(Debug, Clone, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize,
                 ::rkyv::Archive, ::rkyv::Serialize, ::rkyv::Deserialize)]
        pub enum $Change { $($Connector($Mode),)+ }
        impl $Change {
            pub const fn kind(&self) -> $Kind {
                match self { $(Self::$Connector(mode) => $Kind::$Connector(mode.entity()),)+ }
            }
            pub fn publishing(&self) -> $crate::EmitterPublishing<'_> {
                match self { $(Self::$Connector(mode) => mode.publishing(),)+ }
            }
        }
    };
}
pub(crate) use emitter_destinations;
