//! Layer: vocabulary.
//! Owns: setting classes, definition metadata and construction of validated setting values.
//! Depends on: primitive values and the defining owner's typed validation report.
//! Must not know: command-line parsing, machines, gossip, consensus or running services.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns typed setting definitions and values")
)]

use std::{fmt, marker::PhantomData};

mod sealed {
    pub trait Class {}
}

/// The closed classification of a setting, chosen by its definition.
pub trait SettingClass: sealed::Class {
    const NAME: &'static str;
}

/// What a node must bring to reach the live cluster.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Prerequisites;

/// What the cluster accepts or promises, with one committed value.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ClusterParameters;

/// What one node can carry, without changing cluster acceptance or promises.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NodeCapabilities;

impl sealed::Class for Prerequisites {}
impl sealed::Class for ClusterParameters {}
impl sealed::Class for NodeCapabilities {}

impl SettingClass for Prerequisites {
    const NAME: &'static str = "prerequisite";
}
impl SettingClass for ClusterParameters {
    const NAME: &'static str = "cluster parameter";
}
impl SettingClass for NodeCapabilities {
    const NAME: &'static str = "node capability";
}

/// One setting's class, name, single default, derivation and validation owner.
///
/// `Context` carries typed inputs to a derivation or relationship check. The edge keeps a
/// machine-dependent argument as `Option` until the owner resolves it with that context.
/// Defaults and explicit values pass the same validation before becoming a [`Setting`].
///
/// ```
/// use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
/// use std::{convert::Infallible, num::NonZeroUsize};
/// struct Workers;
/// impl SettingDefinition for Workers {
///     type Class = NodeCapabilities;
///     type Value = NonZeroUsize;
///     type Context = NonZeroUsize;
///     type Error = Infallible;
///     const NAME: &'static str = "execution-workers";
///     const DERIVATION: Option<&'static str> = Some("the detected available CPU count");
///     fn default_value(cpus: &Self::Context) -> Self::Value { *cpus }
///     fn validate(_: &Self::Value, _: &Self::Context) -> Result<(), error_stack::Report<Self::Error>> { Ok(()) }
/// }
/// let cpus = NonZeroUsize::MIN;
/// let workers = Setting::<Workers>::resolve(None, &cpus)?;
/// assert_eq!(*workers.value(), cpus);
/// # Ok::<(), error_stack::Report<Infallible>>(())
/// ```
///
/// A definition without its class fails to compile:
///
/// ```compile_fail,E0046
/// use nervix_models::SettingDefinition;
/// use std::convert::Infallible;
/// struct Workers;
/// impl SettingDefinition for Workers {
///     type Value = u32;
///     type Context = ();
///     type Error = Infallible;
///     const NAME: &'static str = "execution-workers";
///     const DERIVATION: Option<&'static str> = None;
///     fn default_value(_: &()) -> u32 { 4 }
///     fn validate(_: &u32, _: &()) -> Result<(), error_stack::Report<Infallible>> { Ok(()) }
/// }
/// ```
pub trait SettingDefinition {
    type Class: SettingClass;
    type Value;
    type Context;
    type Error: std::error::Error + Send + Sync + 'static;

    const NAME: &'static str;
    const DERIVATION: Option<&'static str>;

    fn default_value(context: &Self::Context) -> Self::Value;
    fn validate(
        value: &Self::Value,
        context: &Self::Context,
    ) -> Result<(), error_stack::Report<Self::Error>>;
}

/// A resolved value whose definition declares its class and has validated its construction.
///
/// Area settings groups use these fields rather than unclassified scalar fields. There is no
/// mutable value access: replacing a value requires validation against the current context.
pub struct Setting<D: SettingDefinition> {
    value: D::Value,
    definition: PhantomData<fn() -> D>,
}

impl<D: SettingDefinition> Clone for Setting<D>
where
    D::Value: Clone,
{
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            definition: PhantomData,
        }
    }
}

impl<D: SettingDefinition> Copy for Setting<D> where D::Value: Copy {}

impl<D: SettingDefinition> PartialEq for Setting<D>
where
    D::Value: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<D: SettingDefinition> Eq for Setting<D> where D::Value: Eq {}

// A generic carrier cannot decide whether its value contains credentials. Value rendering
// belongs to its owner; Debug identifies the declaration without exposing its contents.
impl<D: SettingDefinition> fmt::Debug for Setting<D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Setting")
            .field("name", &D::NAME)
            .finish_non_exhaustive()
    }
}

impl<D: SettingDefinition> Setting<D> {
    pub fn new(
        value: D::Value,
        context: &D::Context,
    ) -> Result<Self, error_stack::Report<D::Error>> {
        D::validate(&value, context)?;
        Ok(Self {
            value,
            definition: PhantomData,
        })
    }

    /// Resolve an omitted argument through its owner's one default, then validate it.
    pub fn resolve(
        value: Option<D::Value>,
        context: &D::Context,
    ) -> Result<Self, error_stack::Report<D::Error>> {
        let value = match value {
            Some(value) => value,
            None => D::default_value(context),
        };
        Self::new(value, context)
    }

    pub fn value(&self) -> &D::Value {
        &self.value
    }

    pub fn into_value(self) -> D::Value {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("the worker count must fit the node's CPU budget")]
    struct WorkerLimit;

    struct Workers;
    impl SettingDefinition for Workers {
        type Class = NodeCapabilities;
        type Value = usize;
        type Context = usize;
        type Error = WorkerLimit;
        const NAME: &'static str = "execution-workers";
        const DERIVATION: Option<&'static str> = Some("the node's CPU budget");

        fn default_value(cpus: &usize) -> usize {
            *cpus
        }
        fn validate(value: &usize, cpus: &usize) -> Result<(), error_stack::Report<WorkerLimit>> {
            if *value == 0 || value > cpus {
                return Err(WorkerLimit.into());
            }
            Ok(())
        }
    }

    #[test]
    fn defaults_and_overrides_use_the_same_validation_boundary() {
        let derived =
            Setting::<Workers>::resolve(None, &8).assured("eight CPUs yield a valid worker budget");
        assert_eq!(*derived.value(), 8);
        let copied = derived;
        assert_eq!(copied, derived);
        let explicit =
            Setting::<Workers>::resolve(Some(3), &8).assured("three workers fit eight CPUs");
        assert_eq!(explicit.into_value(), 3);
        assert!(Setting::<Workers>::resolve(Some(9), &8).is_err());
        assert!(Setting::<Workers>::resolve(None, &0).is_err());
        assert!(Setting::<Workers>::new(0, &8).is_err());
        assert_eq!(Workers::NAME, "execution-workers");
        assert_eq!(Workers::DERIVATION, Some("the node's CPU budget"));
        assert_eq!(
            <Workers as SettingDefinition>::Class::NAME,
            "node capability"
        );
        assert_eq!(Prerequisites::NAME, "prerequisite");
        assert_eq!(ClusterParameters::NAME, "cluster parameter");
    }
}
