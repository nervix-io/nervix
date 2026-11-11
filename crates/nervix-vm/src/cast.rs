//! What `AS` and `TRY_CAST` convert a value to, and the conversion of a list to another list type.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Which pairs of types a conversion accepts and why it refuses the others, the target
//!   a compiled cast converts to, the plan a program prepares once for a pair of list types, and
//!   the kernel that converts a list column through its offsets, validity and child values.
//! - **Depends on.** Arrow arrays and kernels, the scalar conversion every element takes, and the
//!   VM's register types, row errors and runtime errors.
//! - **Must not know.** Programs, instructions or register allocation, or what a caller does with
//!   a row whose value does not convert.
//!
//! A list converts level by level, from its outermost container in. At each level an `ARRAY`
//! becomes a `VEC` of the same elements, a `VEC` becomes an `ARRAY` when it holds exactly the
//! `ARRAY`'s length, and a container that keeps its kind keeps its layout. The innermost elements
//! convert by the conversion `AS` applies to a scalar of their type, with the same rule for every
//! value. A list holds no null element, so a row fails whole when any of its elements does not
//! convert or any of its `VEC`s does not hold the length of the `ARRAY` it becomes. Pairs that can
//! never convert are refused before the program runs: a list and a scalar, lists nested to
//! different depths, `ARRAY`s of different lengths, and the `BYTES` elements the scalar rule
//! refuses.
//!
//! The kernel builds no value per row. A conversion whose elements all convert and whose layout
//! the target keeps reuses the input's offsets and validity and, when the elements keep their
//! type, the element values themselves. A batch where an element fails, or whose layout the target
//! cannot keep, gathers the elements of the rows that convert by index. A failed or null row is
//! then null at the outermost level and holds placeholders beneath it, an empty `VEC` or the empty
//! value of the element type in an `ARRAY`, so the converted column holds no null element, which
//! no list may hold and no reader of a list accepts.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{iter, ops::Range};

use arch_into::ArchInto as _;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, ListArray, Scalar, UInt64Array,
    builder::UInt64Builder, make_array, new_null_array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, FieldRef};
use arrow_select::{take::take, zip::zip};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::StdArc;
use thiserror::Error;

use crate::{
    batch::TypedArray,
    error::{CollectionLimit, RowErrors, RuntimeError, SideError, SideErrorReason},
    ir::RegisterType,
    program::{CastFailure, Span},
    runtime::cast_values,
    semantics::{ArmExecution, cast_arm_execution},
};

/// The operation a list conversion names in a runtime error.
const LIST_CONVERSION: &str = "list conversion";

/// What a compiled cast converts its operand to.
#[derive(Debug, Clone, PartialEq)]
pub enum CastTarget {
    /// A scalar type, which a scalar operand converts to.
    Scalar(RegisterType),
    /// A list type, which a list operand converts to through the plan prepared for its type.
    List(ListCast),
}

impl CastTarget {
    /// What a conversion of a value of type `value` to `target` converts it to, or why no value of
    /// that type ever converts to `target`.
    pub(crate) fn between(value: &DataType, target: &DataType) -> Result<Self, CastRefusal> {
        match (ListLevel::of(value), ListLevel::of(target)) {
            (None, None) => {
                let target = scalar_target(value, target)?;
                Ok(Self::Scalar(target))
            }
            (Some(_), None) => Err(CastRefusal::ListToScalar),
            (None, Some(_)) => Err(CastRefusal::ScalarToList),
            (Some(_), Some(_)) => {
                let plan = ListCast::between(value, target)?;
                Ok(Self::List(plan))
            }
        }
    }

    /// The register type the cast writes its result to.
    pub(crate) fn register_type(&self) -> RegisterType {
        match self {
            Self::Scalar(target) => *target,
            Self::List(_) => RegisterType::Generic,
        }
    }

    /// How a conditional arm executes the cast of a value held in a register of type `value`: a
    /// cast that reads or writes text, of a scalar or of every element of a list, parses or
    /// formats each selected row, and every other cast converts the whole batch.
    pub(crate) fn arm_execution(&self, value: RegisterType) -> ArmExecution {
        match self {
            Self::Scalar(target) => cast_arm_execution(value, *target),
            Self::List(plan) => plan.elements.arm_execution(),
        }
    }
}

/// Why no value of a type ever converts to another type.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "cast validation returns a pure type validation refusal"
    )
)]
pub(crate) enum CastRefusal {
    /// A `BYTES` value and a value of another type, which only the bytes functions convert.
    #[error("BYTES conversions require bytes_from_utf8, bytes_to_utf8, base64 or hex functions")]
    Bytes,
    /// A list converted to a scalar type, `STRING` included.
    #[error("an ARRAY or VEC value converts only to an ARRAY or VEC type")]
    ListToScalar,
    /// A scalar converted to a list type.
    #[error("only an ARRAY or VEC value converts to an ARRAY or VEC type")]
    ScalarToList,
    /// Lists nested to different depths, which no level-by-level conversion matches.
    #[error(
        "an ARRAY or VEC value nested {value} deep converts only to a type nested {value} deep, \
         found one nested {target} deep"
    )]
    Depth { value: usize, target: usize },
    /// An `ARRAY` converted to an `ARRAY` of another length, which none of its values holds.
    #[error("an ARRAY of length {value} never converts to an ARRAY of length {target}")]
    ArrayLength { value: usize, target: usize },
    /// A type the VM holds no register for.
    #[error(
        "casts are only implemented for supported nervix VM types, found {value:?} AS {target:?}"
    )]
    Unsupported { value: DataType, target: DataType },
}

/// The scalar register type a scalar of type `value` converts to, or why it never converts.
fn scalar_target(value: &DataType, target: &DataType) -> Result<RegisterType, CastRefusal> {
    if (value == &DataType::Binary || target == &DataType::Binary) && value != target {
        return Err(CastRefusal::Bytes);
    }
    let (Some(_), Some(target_register)) = (
        RegisterType::from_data_type(value),
        RegisterType::from_data_type(target),
    ) else {
        return Err(CastRefusal::Unsupported {
            value: value.clone(),
            target: target.clone(),
        });
    };
    Ok(target_register)
}

/// The kind of container one level of a list type is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Container {
    Vec,
    /// An `ARRAY`, with the size of the fixed-size list Arrow holds it in.
    Array(i32),
}

/// One level of a list type: its container and the field its elements are held in.
#[derive(Debug, Clone, Copy)]
struct ListLevel<'a> {
    container: Container,
    items: &'a FieldRef,
}

impl<'a> ListLevel<'a> {
    /// The outermost level of `data_type`, or `None` for a scalar type.
    fn of(data_type: &'a DataType) -> Option<Self> {
        match data_type {
            DataType::List(items) => Some(Self {
                container: Container::Vec,
                items,
            }),
            DataType::FixedSizeList(items, size) => Some(Self {
                container: Container::Array(*size),
                items,
            }),
            _ => None,
        }
    }
}

/// How many list levels nest in `data_type`.
fn list_depth(data_type: &DataType) -> usize {
    let mut depth = 0_usize;
    let mut level = ListLevel::of(data_type);
    while let Some(current) = level {
        depth = depth
            .checked_add(1)
            .assured("one finite type nests far fewer than usize::MAX lists");
        level = ListLevel::of(current.items.data_type());
    }
    depth
}

/// The length of an `ARRAY` level: the size of the fixed-size list Arrow holds it in, and the
/// count of elements each of its values holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ArrayLength {
    size: i32,
    elements: usize,
}

impl ArrayLength {
    /// The length of an `ARRAY` held in a fixed-size list of `size`, which a Nervix type never
    /// declares empty.
    fn of(size: i32) -> Option<Self> {
        let Ok(elements) = usize::try_from(size) else {
            return None;
        };
        if elements == 0 {
            return None;
        }
        Some(Self { size, elements })
    }
}

/// How a value of one list type converts to another, prepared once for the pair of types when the
/// program compiles.
#[derive(Debug, Clone, PartialEq)]
pub struct ListCast {
    /// The type every value converts from, which a column the plan converts holds.
    value: DataType,
    /// The type every value converts to.
    target: DataType,
    /// Every nesting level, from the outermost in.
    levels: Vec<LevelCast>,
    /// How each innermost element converts.
    elements: ElementCast,
}

/// How one nesting level converts.
#[derive(Debug, Clone, PartialEq)]
struct LevelCast {
    container: ContainerCast,
    /// The field the converted level holds its elements in, which is the target type's.
    items: FieldRef,
}

/// How the container of one nesting level converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerCast {
    /// A `VEC` stays a `VEC`.
    Vec,
    /// An `ARRAY` stays an `ARRAY` of the same length.
    Array(ArrayLength),
    /// An `ARRAY` becomes a `VEC` of the elements it holds.
    ArrayToVec(ArrayLength),
    /// A `VEC` becomes an `ARRAY`, which a `VEC` of any other length fails.
    VecToArray(ArrayLength),
}

impl ContainerCast {
    /// The length every converted value of this level holds, or `None` where it becomes a `VEC`.
    fn converted_length(self) -> Option<ArrayLength> {
        match self {
            Self::Vec | Self::ArrayToVec(_) => None,
            Self::Array(length) | Self::VecToArray(length) => Some(length),
        }
    }
}

/// How each innermost element of a list converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ElementCast {
    /// The elements keep their type, so the converted list keeps their values.
    Keep,
    /// Each element converts as `AS` converts a scalar of its type.
    Convert {
        value: RegisterType,
        target: RegisterType,
    },
}

impl ElementCast {
    /// How an element of type `value` converts to `target`, or why it never does.
    fn between(value: &DataType, target: &DataType) -> Result<Self, CastRefusal> {
        let target_register = scalar_target(value, target)?;
        let value_register = RegisterType::from_data_type(value)
            .verified("the scalar conversion above accepted only register types");
        if value == target {
            return Ok(Self::Keep);
        }
        Ok(Self::Convert {
            value: value_register,
            target: target_register,
        })
    }

    fn arm_execution(self) -> ArmExecution {
        match self {
            Self::Keep => ArmExecution::WholeBatch,
            Self::Convert { value, target } => cast_arm_execution(value, target),
        }
    }

    /// Every element of `elements` converted, with a null wherever one does not convert.
    fn convert(self, elements: &ArrayRef) -> error_stack::Result<ArrayRef, RuntimeError> {
        match self {
            Self::Keep => Ok(StdArc::clone(elements)),
            Self::Convert { target, .. } => {
                let values = TypedArray::try_from_array_ref(StdArc::clone(elements))?;
                cast_values(&values, target).map_err(list_arrow_error)
            }
        }
    }
}

impl ListCast {
    /// The plan converting a list of type `value` to the list type `target`, or why no value of
    /// `value` ever converts to it. Both types are lists.
    fn between(value: &DataType, target: &DataType) -> Result<Self, CastRefusal> {
        let value_depth = list_depth(value);
        let target_depth = list_depth(target);
        if value_depth != target_depth {
            return Err(CastRefusal::Depth {
                value: value_depth,
                target: target_depth,
            });
        }
        let mut levels = Vec::with_capacity(value_depth);
        let mut converted_from = value;
        let mut converted_to = target;
        while let (Some(from), Some(to)) =
            (ListLevel::of(converted_from), ListLevel::of(converted_to))
        {
            let Some(container) = ContainerCast::between(from.container, to.container)? else {
                return Err(CastRefusal::Unsupported {
                    value: value.clone(),
                    target: target.clone(),
                });
            };
            levels.push(LevelCast {
                container,
                items: StdArc::clone(to.items),
            });
            converted_from = from.items.data_type();
            converted_to = to.items.data_type();
        }
        let elements = ElementCast::between(converted_from, converted_to)?;
        Ok(Self {
            value: value.clone(),
            target: target.clone(),
            levels,
            elements,
        })
    }
}

impl ContainerCast {
    /// How a container of kind `value` converts to one of kind `target`, `None` for an `ARRAY`
    /// held in a fixed-size list of no element, or why no value of it ever converts.
    fn between(value: Container, target: Container) -> Result<Option<Self>, CastRefusal> {
        let converted = match (value, target) {
            (Container::Vec, Container::Vec) => Some(Self::Vec),
            (Container::Vec, Container::Array(size)) => ArrayLength::of(size).map(Self::VecToArray),
            (Container::Array(size), Container::Vec) => ArrayLength::of(size).map(Self::ArrayToVec),
            (Container::Array(value_size), Container::Array(target_size)) => {
                let (Some(value_length), Some(target_length)) =
                    (ArrayLength::of(value_size), ArrayLength::of(target_size))
                else {
                    return Ok(None);
                };
                if value_length != target_length {
                    return Err(CastRefusal::ArrayLength {
                        value: value_length.elements,
                        target: target_length.elements,
                    });
                }
                Some(Self::Array(value_length))
            }
        };
        Ok(converted)
    }
}

/// One row whose list does not convert, and why.
#[derive(Debug, Clone, PartialEq)]
struct RowFailure {
    row: usize,
    reason: SideErrorReason,
}

/// The elements a conversion could not convert, and the type it converted them to.
struct ElementFailures {
    failed: BooleanBuffer,
    target: RegisterType,
}

impl ListCast {
    /// Converts every row of `input`, a column of the type the plan converts from. A row whose
    /// list does not convert becomes null, and a cast that reports its failures records one for
    /// that row, inside `span`.
    pub(crate) fn convert(
        &self,
        input: TypedArray,
        on_failure: CastFailure,
        row_errors: &mut RowErrors,
        span: Span,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        let input = input.into_array_ref();
        if input.data_type() != &self.value {
            return Err(Report::new(RuntimeError::CollectionTypeMismatch {
                operation: LIST_CONVERSION,
                expected: self.value.clone(),
                actual: input.data_type().clone(),
            }));
        }
        // A list converted to its own type is returned unchanged.
        if self.value == self.target {
            return Ok(TypedArray::Generic(input));
        }
        let column = ListColumn::of(&input, self.levels.len());
        let converted = self.elements.convert(column.elements)?;
        let element_failures = self.element_failures(column.elements, &converted);
        let lengths_held = self.lengths_held(&column);
        let failures = self.failures(&column, &lengths_held, element_failures.as_ref());
        // An element that failed beneath a null row would leave a null element no list may hold, so
        // only a column whose every element converted keeps its layout.
        let keeps_layout = failures.is_empty()
            && element_failures.is_none()
            && self.keeps_layout(&column, &lengths_held, &converted);
        let output = if keeps_layout {
            self.rebuild(&column, converted)?
        } else {
            self.gather(&column, &converted, &failures)?
        };
        if on_failure.reports_error() {
            for failure in failures {
                let error = SideError {
                    reason: failure.reason,
                    span,
                };
                row_errors.push(failure.row, error);
            }
        }
        Ok(TypedArray::Generic(output))
    }

    /// Where the converted elements hold a null for an element of the input that held a value,
    /// which its conversion failed for. `None` when no element failed.
    fn element_failures(
        &self,
        elements: &ArrayRef,
        converted: &ArrayRef,
    ) -> Option<ElementFailures> {
        let ElementCast::Convert { target, .. } = self.elements else {
            return None;
        };
        // A conversion keeps every null and adds one for each element it cannot convert, so equal
        // counts prove that none failed without comparing the validity buffers.
        if converted.null_count() == elements.null_count() {
            return None;
        }
        let converted_nulls = converted.nulls().verified(
            "a conversion never removes a null, so the unequal counts above leave the converted \
             elements with a null and therefore a null buffer",
        );
        let not_converted = !converted_nulls.inner();
        let failed = match elements.nulls() {
            Some(nulls) => nulls.inner() & &not_converted,
            None => not_converted,
        };
        Some(ElementFailures { failed, target })
    }

    /// For every level, whether each of its values, a null or hidden one included, holds exactly
    /// the length of the `ARRAY` it becomes. A level that does not become an `ARRAY` from a `VEC`
    /// holds whatever it needs.
    fn lengths_held(&self, column: &ListColumn<'_>) -> Vec<bool> {
        let mut held = Vec::with_capacity(self.levels.len());
        for (level, input) in self.levels.iter().zip(&column.levels) {
            let level_held = match level.container {
                ContainerCast::VecToArray(length) => {
                    let offsets = input.variable_offsets().verified(
                        "a level that becomes an ARRAY from a VEC is a VEC in the input, whose \
                         type the plan's type check confirmed",
                    );
                    every_value_holds(offsets, length)
                }
                ContainerCast::Vec | ContainerCast::Array(_) | ContainerCast::ArrayToVec(_) => true,
            };
            held.push(level_held);
        }
        held
    }

    /// Every row that holds a list and does not convert, in row order: a row with a `VEC` whose
    /// length is not its `ARRAY`'s, from the outermost level in, or else a row with an element
    /// that does not convert.
    fn failures(
        &self,
        column: &ListColumn<'_>,
        lengths_held: &[bool],
        element_failures: Option<&ElementFailures>,
    ) -> Vec<RowFailure> {
        let every_length_held = !lengths_held.contains(&false);
        if element_failures.is_none() && every_length_held {
            return Vec::new();
        }
        let top = column.top();
        let mut failures = Vec::new();
        for row in 0..top.len() {
            if top.is_null(row) {
                continue;
            }
            let reason = self.row_failure(column, row, lengths_held, element_failures);
            if let Some(reason) = reason {
                failures.push(RowFailure { row, reason });
            }
        }
        failures
    }

    /// Why the list of `row` does not convert, or `None` when it does. Only a level
    /// `lengths_held` does not mark holds a `VEC` of the wrong length anywhere.
    fn row_failure(
        &self,
        column: &ListColumn<'_>,
        row: usize,
        lengths_held: &[bool],
        element_failures: Option<&ElementFailures>,
    ) -> Option<SideErrorReason> {
        let next_row = row
            .checked_add(1)
            .assured("a row below the column's length has a following position");
        let mut values = row..next_row;
        for ((level, input), held) in self.levels.iter().zip(&column.levels).zip(lengths_held) {
            if !*held && let ContainerCast::VecToArray(length) = level.container {
                for value in values.clone() {
                    let found = input.elements_of(value).len();
                    if found != length.elements {
                        return Some(SideErrorReason::ListCastLengthMismatch {
                            expected: length.elements,
                            found,
                        });
                    }
                }
            }
            values = input.elements_of_values(values);
        }
        let failures = element_failures?;
        let failed_elements = failures
            .failed
            .slice(values.start, values.len())
            .count_set_bits();
        if failed_elements == 0 {
            return None;
        }
        Some(SideErrorReason::ListElementCastFailed {
            target: failures.target,
        })
    }

    /// Whether the converted column can reuse the input's layout: every level that becomes a
    /// `VEC` holds no null element, which Arrow refuses even beneath a null row, and every level
    /// that becomes an `ARRAY` from a `VEC` holds exactly its length in every value, a null one
    /// included.
    fn keeps_layout(
        &self,
        column: &ListColumn<'_>,
        lengths_held: &[bool],
        converted: &ArrayRef,
    ) -> bool {
        if lengths_held.contains(&false) {
            return false;
        }
        for (index, level) in self.levels.iter().enumerate() {
            let (ContainerCast::Vec | ContainerCast::ArrayToVec(_)) = level.container else {
                continue;
            };
            let below = index
                .checked_add(1)
                .assured("a level index is below the count of levels");
            let null_elements = match column.levels.get(below) {
                Some(inner) => inner.null_count(),
                None => converted.null_count(),
            };
            if null_elements > 0 {
                return false;
            }
        }
        true
    }

    /// The converted column on the input's layout, from the innermost level out: offsets,
    /// validity and every element buffer are the input's, and only an `ARRAY` that becomes a
    /// `VEC` builds new offsets.
    fn rebuild(
        &self,
        column: &ListColumn<'_>,
        converted: ArrayRef,
    ) -> error_stack::Result<ArrayRef, RuntimeError> {
        let mut elements = converted;
        for (level, input) in self.levels.iter().zip(&column.levels).rev() {
            let nulls = input.nulls().cloned();
            let items = StdArc::clone(&level.items);
            elements = match level.container {
                ContainerCast::Vec => {
                    let offsets = input.variable_offsets().verified(
                        "a level that stays a VEC is a VEC in the input, whose type the plan's \
                         type check confirmed",
                    );
                    variable_list(items, offsets.clone(), elements, nulls)?
                }
                ContainerCast::ArrayToVec(length) => {
                    let lengths = iter::repeat_n(length.elements, input.len());
                    let offsets = offsets_from_lengths(lengths)?;
                    variable_list(items, offsets, elements, nulls)?
                }
                ContainerCast::Array(length) => fixed_list(items, length, elements, nulls)?,
                ContainerCast::VecToArray(length) => {
                    let first = input.first_element(0);
                    let count = input
                        .len()
                        .checked_mul(length.elements)
                        .ok_or_else(|| collection_too_large(CollectionLimit::AddressableLength))?;
                    fixed_list(items, length, elements.slice(first, count), nulls)?
                }
            };
        }
        Ok(elements)
    }

    /// The converted column of the rows that convert, gathered by index. A failed or null row is
    /// null at the outermost level and holds placeholders beneath it, since a list's elements are
    /// never null: an empty `VEC`, and the empty value of the element type in an `ARRAY`.
    fn gather(
        &self,
        column: &ListColumn<'_>,
        converted: &ArrayRef,
        failures: &[RowFailure],
    ) -> error_stack::Result<ArrayRef, RuntimeError> {
        let top = column.top();
        let mut failed = vec![false; top.len()];
        for failure in failures {
            failed[failure.row] = true;
        }
        let mut sources = Vec::with_capacity(top.len());
        for (row, row_failed) in failed.into_iter().enumerate() {
            if top.is_null(row) || row_failed {
                sources.push(None);
            } else {
                sources.push(Some(row));
            }
        }
        let mut shapes = Vec::with_capacity(self.levels.len());
        for (index, (level, input)) in self.levels.iter().zip(&column.levels).enumerate() {
            let validity = if index == 0 {
                GatheredValidity::NullWhereAbsent
            } else {
                GatheredValidity::Valid
            };
            let gathered = GatheredLevel::of(level.container, input, &sources, validity)?;
            sources = gathered.sources;
            shapes.push(gathered.shape);
        }
        let mut indices = UInt64Builder::with_capacity(sources.len());
        for source in sources {
            match source {
                Some(index) => indices.append_value(index.arch_into()),
                None => indices.append_null(),
            }
        }
        let indices: UInt64Array = indices.finish();
        let gathered = take(converted.as_ref(), &indices, None).map_err(list_arrow_error)?;
        let mut elements = without_null_elements(gathered).map_err(list_arrow_error)?;
        for (level, shape) in self.levels.iter().zip(shapes).rev() {
            let items = StdArc::clone(&level.items);
            elements = match shape {
                LevelShape::Fixed { length, nulls } => fixed_list(items, length, elements, nulls)?,
                LevelShape::Variable { offsets, nulls } => {
                    variable_list(items, offsets, elements, nulls)?
                }
            };
        }
        Ok(elements)
    }
}

/// Which values of a gathered level are null: those of an absent row at the outermost level, and
/// none beneath it, where an absent row's values are placeholders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatheredValidity {
    NullWhereAbsent,
    Valid,
}

/// The values of one gathered level, and where each of its elements comes from.
struct GatheredLevel {
    shape: LevelShape,
    /// The input element each converted element reads, or `None` for a placeholder beneath a
    /// failed or null row.
    sources: Vec<Option<usize>>,
}

/// The layout of one gathered level: its validity, and the length of an `ARRAY` or the offsets
/// of a `VEC`.
enum LevelShape {
    Fixed {
        length: ArrayLength,
        nulls: Option<NullBuffer>,
    },
    Variable {
        offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
    },
}

impl GatheredLevel {
    /// The level whose value `index` reads the input value `sources[index]`, or is absent for
    /// `None`: null or a placeholder as `validity` says, holding no element as a `VEC` and
    /// placeholders as an `ARRAY`.
    fn of(
        container: ContainerCast,
        input: &InputLevel<'_>,
        sources: &[Option<usize>],
        validity: GatheredValidity,
    ) -> error_stack::Result<Self, RuntimeError> {
        let present = BooleanBuffer::collect_bool(sources.len(), |index| sources[index].is_some());
        let nulls = match validity {
            GatheredValidity::NullWhereAbsent if present.count_set_bits() < sources.len() => {
                Some(NullBuffer::new(present))
            }
            GatheredValidity::NullWhereAbsent | GatheredValidity::Valid => None,
        };
        let mut elements = Vec::new();
        let shape = match container.converted_length() {
            Some(length) => {
                for source in sources {
                    match source {
                        Some(value) => elements.extend(input.elements_of(*value).map(Some)),
                        None => elements.extend(iter::repeat_n(None, length.elements)),
                    }
                }
                LevelShape::Fixed { length, nulls }
            }
            None => {
                let mut lengths = Vec::with_capacity(sources.len());
                for source in sources {
                    match source {
                        Some(value) => {
                            let range = input.elements_of(*value);
                            lengths.push(range.len());
                            elements.extend(range.map(Some));
                        }
                        None => lengths.push(0),
                    }
                }
                let offsets = offsets_from_lengths(lengths)?;
                LevelShape::Variable { offsets, nulls }
            }
        };
        Ok(Self {
            shape,
            sources: elements,
        })
    }
}

/// The levels of a list column, from the outermost in, and the elements the innermost holds.
struct ListColumn<'a> {
    levels: Vec<InputLevel<'a>>,
    elements: &'a ArrayRef,
}

impl<'a> ListColumn<'a> {
    /// The `depth` levels of `column`, whose type the caller checked to be the plan's.
    fn of(column: &'a ArrayRef, depth: usize) -> Self {
        let mut levels = Vec::with_capacity(depth);
        let mut elements = column;
        for _ in 0..depth {
            let level = InputLevel::of(elements).verified(
                "the plan's type check confirmed a list type nested exactly as deep as the plan",
            );
            elements = level.values();
            levels.push(level);
        }
        Self { levels, elements }
    }

    fn top(&self) -> &InputLevel<'a> {
        self.levels
            .first()
            .verified("a list type has at least one level, which the plan's type check confirmed")
    }
}

/// One level of a list column.
enum InputLevel<'a> {
    Variable(&'a ListArray),
    Fixed(&'a FixedSizeListArray),
}

impl<'a> InputLevel<'a> {
    /// The level `array` holds, or `None` when it holds no list.
    fn of(array: &'a ArrayRef) -> Option<Self> {
        match array.data_type() {
            DataType::List(_) => {
                let array = array.as_any().downcast_ref::<ListArray>()?;
                Some(Self::Variable(array))
            }
            DataType::FixedSizeList(_, _) => {
                let array = array.as_any().downcast_ref::<FixedSizeListArray>()?;
                Some(Self::Fixed(array))
            }
            _ => None,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Variable(array) => array.len(),
            Self::Fixed(array) => array.len(),
        }
    }

    fn is_null(&self, value: usize) -> bool {
        match self {
            Self::Variable(array) => array.is_null(value),
            Self::Fixed(array) => array.is_null(value),
        }
    }

    fn null_count(&self) -> usize {
        match self {
            Self::Variable(array) => array.null_count(),
            Self::Fixed(array) => array.null_count(),
        }
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        match self {
            Self::Variable(array) => array.nulls(),
            Self::Fixed(array) => array.nulls(),
        }
    }

    fn values(&self) -> &'a ArrayRef {
        match self {
            Self::Variable(array) => array.values(),
            Self::Fixed(array) => array.values(),
        }
    }

    /// The offsets of a `VEC` level, or `None` for an `ARRAY` level.
    fn variable_offsets(&self) -> Option<&OffsetBuffer<i32>> {
        match self {
            Self::Variable(array) => Some(array.offsets()),
            Self::Fixed(_) => None,
        }
    }

    /// Where, among the elements this level's values hold, value `value` begins. Arrow slices an
    /// `ARRAY`'s elements with its values, so its value begins at its position times the length.
    fn first_element(&self, value: usize) -> usize {
        match self {
            Self::Variable(array) => usize::try_from(array.offsets()[value])
                .assured("Arrow list offsets are non-negative by construction"),
            Self::Fixed(array) => {
                let length = usize::try_from(array.value_length())
                    .assured("Arrow fixed-size list sizes are non-negative by construction");
                value
                    .checked_mul(length)
                    .assured("a value of a fixed-size list begins within its element array")
            }
        }
    }

    /// The elements value `value` holds.
    fn elements_of(&self, value: usize) -> Range<usize> {
        let next = value
            .checked_add(1)
            .assured("a value below the level's length has a following position");
        self.elements_of_values(value..next)
    }

    /// The elements the consecutive values `values` hold, which lie consecutively too.
    fn elements_of_values(&self, values: Range<usize>) -> Range<usize> {
        self.first_element(values.start)..self.first_element(values.end)
    }
}

/// Whether every value of a `VEC` level, a null one included, holds exactly the length of the
/// `ARRAY` it becomes.
fn every_value_holds(offsets: &OffsetBuffer<i32>, length: ArrayLength) -> bool {
    for window in offsets.windows(2) {
        let held = window[1]
            .checked_sub(window[0])
            .assured("Arrow list offsets never decrease");
        if held != length.size {
            return false;
        }
    }
    true
}

/// `elements` with every null replaced by the empty value of their type: zero, `false`, empty text
/// or bytes, or the epoch. A list's elements are never null, so a value beneath a null list, which
/// no reader observes, takes this placeholder rather than a null element Arrow would refuse.
pub(crate) fn without_null_elements(elements: ArrayRef) -> Result<ArrayRef, ArrowError> {
    let Some(nulls) = elements.nulls() else {
        return Ok(elements);
    };
    if nulls.null_count() == 0 {
        return Ok(elements);
    }
    let present = BooleanArray::new(nulls.inner().clone(), None);
    let empty = new_null_array(elements.data_type(), 1)
        .to_data()
        .into_builder()
        .nulls(None)
        .build()?;
    zip(&present, &elements, &Scalar::new(make_array(empty)))
}

/// Offsets for `VEC` values holding `lengths` elements each, or a typed refusal when they exceed
/// the offsets Arrow's list holds.
fn offsets_from_lengths(
    lengths: impl IntoIterator<Item = usize, IntoIter: ExactSizeIterator>,
) -> error_stack::Result<OffsetBuffer<i32>, RuntimeError> {
    let lengths = lengths.into_iter();
    let capacity = lengths
        .len()
        .checked_add(1)
        .assured("a count of values one column holds is below usize::MAX");
    let mut offsets = Vec::with_capacity(capacity);
    let mut end = 0_i32;
    offsets.push(end);
    for length in lengths {
        let length = i32::try_from(length)
            .map_err(|_| collection_too_large(CollectionLimit::VectorOffsets))?;
        end = end
            .checked_add(length)
            .ok_or_else(|| collection_too_large(CollectionLimit::VectorOffsets))?;
        offsets.push(end);
    }
    Ok(OffsetBuffer::new(ScalarBuffer::from(offsets)))
}

fn variable_list(
    items: FieldRef,
    offsets: OffsetBuffer<i32>,
    elements: ArrayRef,
    nulls: Option<NullBuffer>,
) -> error_stack::Result<ArrayRef, RuntimeError> {
    let list = ListArray::try_new(items, offsets, elements, nulls).map_err(list_arrow_error)?;
    Ok(StdArc::new(list))
}

fn fixed_list(
    items: FieldRef,
    length: ArrayLength,
    elements: ArrayRef,
    nulls: Option<NullBuffer>,
) -> error_stack::Result<ArrayRef, RuntimeError> {
    let list = FixedSizeListArray::try_new(items, length.size, elements, nulls)
        .map_err(list_arrow_error)?;
    Ok(StdArc::new(list))
}

fn list_arrow_error(source: ArrowError) -> Report<RuntimeError> {
    Report::new(RuntimeError::CollectionArrow {
        operation: LIST_CONVERSION,
        source,
    })
}

fn collection_too_large(limit: CollectionLimit) -> Report<RuntimeError> {
    Report::new(RuntimeError::CollectionTooLarge {
        operation: LIST_CONVERSION,
        limit,
    })
}

#[cfg(test)]
#[path = "cast_tests.rs"]
mod tests;
