#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JsonType {
    String,
    Number,
    Integer,
    Object,
    Array,
    Boolean,
    Null,
}
