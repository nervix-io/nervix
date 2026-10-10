//! A qualified inherent API remains Option's panic operation.
type Optional<T> = Option<T>;
pub fn operation(value: Optional<u32>) -> u32 {
    Optional::unwrap(value)
}
