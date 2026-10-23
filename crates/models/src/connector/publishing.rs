//! The publishing policies each emitter entity can represent.
//!
//! Layer: vocabulary.
//! - **Owns.** Generation of each entity's closed policy enum and a borrowed observation of it.
//! - **Depends on.** Acknowledgement windows, retry policy and publishing capability kinds.
//! - **Must not know.** Grammar, validation, connector drivers or execution plans.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

/// Declare an entity's complete set of representable publishing policies once. Its capability
/// declaration reads KINDS, and observers borrow the policy instead of retaining a second copy.
macro_rules! emitter_modes {
    (pub enum $Mode:ident { $( $Variant:ident { $($field:ident : $Type:ty),+ $(,)? } => $Kind:ident, )+ }) => {
        #[derive(Debug, Clone, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize,
                 ::rkyv::Archive, ::rkyv::Serialize, ::rkyv::Deserialize)]
        pub enum $Mode {
            $( $Variant { $($field: $Type,)+ }, )+
        }
        impl $Mode {
            /// Every policy this entity represents, in declaration order.
            pub const KINDS: &'static [$crate::EmitterPublishingModeKind] = &[$($crate::EmitterPublishingModeKind::$Kind,)+];

            /// A borrowed observation for rendering and policy-independent inspection.
            pub fn publishing(&self) -> $crate::EmitterPublishing<'_> {
                match self {
                    $(Self::$Variant { $($field,)+ } => $crate::EmitterPublishing::$Kind { $($field,)+ },)+
                }
            }

            /// The current Plexus policy clause, without its MODE keyword.
            pub fn to_canonical_plexus(&self) -> String {
                self.publishing().to_canonical_plexus()
            }
        }
    };
}
pub(crate) use emitter_modes;
