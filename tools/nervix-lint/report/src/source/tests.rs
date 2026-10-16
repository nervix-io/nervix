//! Bounded source syntax, macro fragment and complete current representation qualification.

use meticulous::{OptionExt as _, ResultExt as _};

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
    let result: Vec<_> = result
        .into_iter()
        .filter(|site| site.rule == "as_casts")
        .collect();
    assert_eq!(result.len(), 10);
    assert!(result.iter().all(|site| site.rule == "as_casts"));
    assert_eq!(
        result.iter().map(|site| site.line).collect::<Vec<_>>(),
        [2, 3, 4, 5, 6, 10, 13, 16, 19, 21]
    );
}

#[test]
fn bolero_hidden_tunables_preserve_every_policy_site() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
        let path = format!("src/policy_{}.rs", seed[0]);
        let mut source =
            String::from("// bounded Unicode π policy\nuse std::time::Duration as Budget;\n");
        let mut expected = Vec::new();
        let count = usize::from(seed[1] % 8);
        for index in 0..count {
            let line = match seed[index + 2] % 4 {
                0 => format!("#[cfg(any())] const P_{index}: Budget = Budget::from_secs(5);\n"),
                1 => format!("macro_rules! p_{index} {{ () => {{ const P: usize = 7; }}; }}\n"),
                2 => format!("fn p_{index}() {{ mpsc::channel(8); }}\n"),
                _ => format!("fn p_{index}() {{ Vec::with_capacity(16); }}\n"),
            };
            let column = match seed[index + 2] % 4 {
                0 | 1 => line
                    .find("const")
                    .assured("the generated constant has a const token"),
                2 => line
                    .find("mpsc")
                    .assured("the generated call starts with mpsc"),
                _ => line
                    .find("Vec")
                    .assured("the generated call starts with Vec"),
            };
            expected.push(SourceSite {
                path: path.clone(),
                line: index + 3,
                column,
                rule: "hidden_tunables".into(),
            });
            source.push_str(&line);
        }
        source.push_str(
            "#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = \"wire tag width\"))] const \
             TAG: u8 = 4;\n",
        );
        source.push_str("#[cfg(test)] mod checks { const PROBE: u32 = 16; }\n");
        source.push_str(
            "fn data(capacity: usize) { Vec::with_capacity(capacity); let text = \
             \"Duration::from_secs(3)\"; }\n",
        );
        let input = SourceInput { path, source };
        let encoded =
            serde_json::to_vec(&input).assured("current source input has a JSON representation");
        let decoded: SourceInput =
            serde_json::from_slice(&encoded).assured("the encoded complete input is valid JSON");
        assert_eq!(decoded, input);
        let result = scan(vec![decoded])
            .assured("the generator supplies valid current Rust syntax and contracts");
        assert_eq!(result, expected);
        let encoded =
            serde_json::to_vec(&result).assured("complete sites have a JSON representation");
        let decoded: Vec<SourceSite> =
            serde_json::from_slice(&encoded).assured("the complete sites were just serialized");
        assert_eq!(decoded, result);
    });
}

#[test]
fn setting_defaults_fixed_contracts_and_lexical_aliases_keep_their_owners() {
    let result = sites(
        r#"
use std::time::{Duration as Budget};
type Wait = Budget;
const DEADLINE: Wait = Budget::from_secs(5);
struct Settings { budget: Setting<BudgetSpec> }
impl Default for Settings { fn default() -> Self { Budget::from_secs(6); make() } }
impl SettingDefinition for BudgetSpec {
    fn default_value() -> Budget { Budget::from_secs(7) }
    fn validate() { Budget::from_secs(8); }
    #[cfg(test)] fn probe() { Budget::from_secs(9); }
}
struct Ordinary;
impl Default for Ordinary { fn default() -> Self { Budget::from_secs(10); Self } }
mod labels { type Budget = String; const TEXT: Budget = make(); }
fn configure() {
    use std::time::Duration as Local;
    Local::from_millis(11);
    Semaphore::new(12);
    ArrayQueue::new(13);
    queue.add_permits(14);
    builder.with_capacity(15);
    sync_channel(16);
    bounded(17);
    watch::channel(18);
}
#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame tag is one byte"))]
const TAG: u8 = 1;
#[test] fn probe() { Budget::from_secs(19); }
trait Contract { #[cfg(test)] const PROBE: u64 = 20; const WIDTH: NonZeroU32 = make(); }
impl Settings { #[cfg(test)] const PROBE: u32 = 21; const WIDTH: ByteUnit = make(); }
type Left = Right;
type Right = Left;
const CYCLIC: Left = make();
fn cycle() { Left::from_secs(22); }
const PAIR: (u8, u8) = (1, 2);
"#,
    );
    assert_eq!(
        result.iter().map(|site| site.line).collect::<Vec<_>>(),
        [4, 9, 13, 17, 18, 19, 20, 21, 22, 23, 29, 30]
    );
    assert!(result.iter().all(|site| site.rule == "hidden_tunables"));
}

#[test]
fn invalid_constant_contracts_fail_with_the_authored_location() {
    for annotation in [
        "#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = \"\"))] const C: u8 = 1;",
        "#[cfg_attr(nervix_lint, nervix::constant(variable, reason = \"wire\"))] const C: u8 = 1;",
        "#[cfg_attr(nervix_lint, nervix::constant(fixed))] const C: u8 = 1;",
        "#[nervix::constant(fixed, reason = \"wire\")] const C: u8 = 1;",
        "#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = \"wire\"))] fn probe() {}",
    ] {
        let error = scan(vec![SourceInput {
            path: "src/invalid.rs".into(),
            source: annotation.into(),
        }])
        .expect_err("an invalid current source contract cannot authorize an exclusion");
        assert!(
            matches!(error.current_context(), SourceError::ConstantContract {path, line: 1, ..} if path == "src/invalid.rs")
        );
    }
    let result = scan(vec![SourceInput {
        path: "crates/model-harness/src/probe.rs".into(),
        source: "const TEST_BUDGET: usize = 8;".into(),
    }])
    .assured("a harness constant is valid Rust");
    assert!(result.is_empty());
}
