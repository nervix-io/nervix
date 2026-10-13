//! Bounded source syntax, macro fragment and complete current representation qualification.

use meticulous::ResultExt as _;

use super::*;

fn sites(source: &str) -> Vec<SourceSite> {
    scan(vec![SourceInput {
        path: "src/fixture.rs".into(),
        source: source.into(),
    }])
    .assured("the fixture supplies valid current Rust source")
}

#[test]
fn syntax_distinguishes_casts_from_names_and_paths() {
    let result = sites(
        r#"
use example::Value as Alias;
struct Label { r#as: u32 }
fn r#as(r#as: u32) -> u32 { r#as }
fn nested() { <[u8; { 1u8 as usize }] as Default>::default(); }
#[cfg(any())] fn inactive(value: u64) -> u32 { value as u32 }
macro_rules! cast { ($value:expr) => { $value as u16 }; }
macro_rules! repeated { ($($value:expr),*) => { [ $($value as u16),* ] }; }
#[cfg(test)] mod checks { fn cast(value: u32) -> u8 { value as u8 } }
"#,
    );
    assert_eq!(
        result.iter().filter(|site| site.rule == "as_casts").count(),
        4
    );
    assert_eq!(
        result.iter().map(|site| site.line).collect::<Vec<_>>(),
        [5, 6, 7, 8]
    );
}

#[test]
fn field_gates_include_tuples_and_inactive_structs() {
    let result = sites(
        r#"
struct Tuple(#[cfg(feature = "testing")] u32);
#[cfg(any())] struct Hidden { #[cfg(not(feature = "testing"))] value: u32 }
struct Conditional { #[cfg_attr(feature = "testing", cfg(feature = "other"))] value: u32 }
struct Nested { #[cfg_attr(feature = "other", cfg(feature = "testing"))] value: u32 }
struct Stable { #[cfg_attr(feature = "testing", doc = "test note")] value: u32 }
enum Choice { Data { #[cfg(feature = "testing")] value: u32 } }
#[cfg(all(feature = "other", test))] struct TestOnly { #[cfg(feature = "testing")] value: u32 }
"#,
    );
    assert_eq!(result.len(), 4);
    assert!(
        result
            .iter()
            .all(|site| site.rule == "testing_feature_struct_fields")
    );
    assert_eq!(
        result.iter().map(|site| site.line).collect::<Vec<_>>(),
        [2, 3, 4, 5]
    );
}

#[test]
fn malformed_authored_source_fails_with_its_own_location() {
    let result = scan(vec![SourceInput {
        path: "src/broken.rs".into(),
        source: "fn {".into(),
    }]);
    let error = result.expect_err("malformed authored source cannot be a clean count");
    assert!(
        matches!(error.current_context(), SourceError::Syntax { path, .. } if path == "src/broken.rs")
    );
}

#[test]
fn opaque_macros_and_literal_contents_do_not_invent_casts() {
    let result = sites(
        r##"
macro_rules! words { ($value:expr as $kind:ty) => { stringify!($value as $kind) }; }
fn values() { let text = r#"value as u32"#; let byte = 'a'; let _ = (text, byte); }
opaque!(as word; r#as);
"##,
    );
    // The authored stringify payload has cast syntax; arbitrary matcher/DSL words have none.
    assert_eq!(result.len(), 1);
}

#[test]
fn repetition_operands_and_test_only_members_retain_the_source_contract() {
    let result = sites(
        r#"
macro_rules! cast_tokens { ($($value:tt)*) => { $($value)* as u16 }; }
struct Value;
impl Value { #[cfg(test)] fn probe(value: u32) -> u8 { value as u8 } }
trait Contract { #[cfg(test)] fn probe(value: u32) -> u8 { value as u8 } }
"#,
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].line, 2);
}

#[test]
fn bolero_source_syntax_preserves_casts_and_gates() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
        let mut source = format!("// bounded Unicode π {}\n", seed[0]);
        let casts = usize::from(seed[1] % 8);
        let gates = usize::from(seed[2] % 8);
        for index in 0..casts {
            source.push_str(&format!(
                "#[cfg(any())] fn cast_{index}(value: u32) -> u8 {{ value as u8 }}\n"
            ));
        }
        for index in 0..gates {
            source.push_str(&format!(
                "struct Data_{index}(#[cfg(feature = \"testing\")] u32);\n"
            ));
        }
        source.push_str("fn r#as(r#as: u32) -> u32 { r#as }\n");
        let input = SourceInput {
            path: format!("src/bounded_{}.rs", seed[3]),
            source,
        };
        let bytes =
            serde_json::to_vec(&input).assured("the bounded current source is serializable");
        let decoded =
            serde_json::from_slice::<SourceInput>(&bytes).assured("the encoded source is valid");
        assert_eq!(decoded, input);
        let result = scan(vec![decoded]).assured("the generator constructs valid Rust syntax");
        assert_eq!(
            result.iter().filter(|site| site.rule == "as_casts").count(),
            casts
        );
        assert_eq!(
            result
                .iter()
                .filter(|site| site.rule == "testing_feature_struct_fields")
                .count(),
            gates
        );
        assert_eq!(result.len(), casts + gates);
        for (index, site) in result.iter().enumerate() {
            assert_eq!(site.path, input.path);
            assert_eq!(site.line, index + 2);
        }
        let bytes = serde_json::to_vec(&result).assured("complete source sites are serializable");
        let decoded = serde_json::from_slice::<Vec<SourceSite>>(&bytes)
            .assured("the encoded source sites are valid");
        assert_eq!(decoded, result);
    });
}

#[test]
fn casts_in_item_and_macro_statement_contexts_preserve_their_owners() {
    let result = sites(
        r#"
const VALUE: u32 = 1u8 as u32;
static SHARED: u32 = 2u8 as u32;
type Array = [u8; { 3u8 as usize }];
union Storage { bytes: [u8; { 4u8 as usize }] }
extern "C" { fn receive(value: [u8; { 5u8 as usize }]); }
trait Alias = Send;
struct Value;
impl Value {
    const WIDTH: usize = 6u8 as usize;
    type Data = u8;
    #[cfg(test)] const PROBE: usize = 7u8 as usize;
    item_tokens! { const DEPTH: usize = 8u8 as usize; }
}
trait Contract {
    const WIDTH: usize = 9u8 as usize;
    type Data;
    #[cfg(test)] const PROBE: usize = 10u8 as usize;
    item_tokens! { const DEPTH: usize = 11u8 as usize; }
}
statement_tokens! { let value = 12u8 as usize; consume(value); }
"#,
    );
    assert_eq!(result.len(), 10);
    assert!(result.iter().all(|site| site.rule == "as_casts"));
    assert_eq!(
        result.iter().map(|site| site.line).collect::<Vec<_>>(),
        [2, 3, 4, 5, 6, 10, 13, 16, 19, 21]
    );
}
