use std::num::NonZeroUsize;

use arrow_array::{Array, ListArray, StringArray};
use rstest::rstest;
use url::ParseError;

use super::{UrlComponent, component, decode, is_url, port, query_value, query_values};
use crate::{
    RowErrors, SideErrorReason,
    error::{TextOperation, UrlOperation},
    operand::Operand,
    program::Span,
};

const SPAN: Span = Span { start: 0, end: 1 };

/// The component one URL text yields, or the reason it yields none.
fn component_of(text: &str, part: UrlComponent) -> Result<Option<String>, SideErrorReason> {
    let input = StringArray::from(vec![text]);
    let mut errors = RowErrors::new(1);
    let output = component(&input, part, NonZeroUsize::MIN, &mut errors, SPAN);
    if let Some(error) = errors.row(0).first() {
        return Err(error.reason.clone());
    }
    if output.is_null(0) {
        return Ok(None);
    }
    Ok(Some(output.value(0).to_string()))
}

fn present(text: &str) -> Result<Option<String>, SideErrorReason> {
    Ok(Some(text.to_string()))
}

#[rstest]
#[case(
    "HTTPS://Example.COM:8443/a/./b/../caf%C3%A9?q=1#Frag",
    UrlComponent::Scheme,
    present("https")
)]
#[case(
    "HTTPS://Example.COM:8443/a/./b/../caf%C3%A9?q=1#Frag",
    UrlComponent::Host,
    present("example.com")
)]
#[case(
    "HTTPS://Example.COM:8443/a/./b/../caf%C3%A9?q=1#Frag",
    UrlComponent::Path,
    present("/a/caf%C3%A9")
)]
#[case(
    "HTTPS://Example.COM:8443/a/./b/../caf%C3%A9?q=1#Frag",
    UrlComponent::Query,
    present("q=1")
)]
#[case(
    "HTTPS://Example.COM:8443/a/./b/../caf%C3%A9?q=1#Frag",
    UrlComponent::Fragment,
    present("Frag")
)]
#[case(
    "http://bücher.example/",
    UrlComponent::Host,
    present("xn--bcher-kva.example")
)]
#[case(
    "http://[2001:DB8::1]:8080/",
    UrlComponent::Host,
    present("2001:db8::1")
)]
#[case(
    "http://[::ffff:192.0.2.1]/",
    UrlComponent::Host,
    present("::ffff:c000:201")
)]
#[case("http://0x7f.1/", UrlComponent::Host, present("127.0.0.1"))]
#[case("http://example.com/a b", UrlComponent::Path, present("/a%20b"))]
#[case("http://example.com", UrlComponent::Path, present("/"))]
#[case("http://example.com/?", UrlComponent::Query, present(""))]
#[case("http://example.com/#", UrlComponent::Fragment, present(""))]
#[case("http://example.com/", UrlComponent::Query, Ok(None))]
#[case("http://example.com/", UrlComponent::Fragment, Ok(None))]
#[case("mailto:ops@example.com", UrlComponent::Scheme, present("mailto"))]
#[case("mailto:ops@example.com", UrlComponent::Host, Ok(None))]
#[case(
    "mailto:ops@example.com",
    UrlComponent::Path,
    present("ops@example.com")
)]
#[case("urn:isbn:0451450523", UrlComponent::Path, present("isbn:0451450523"))]
#[case("file:///tmp/report.txt", UrlComponent::Host, Ok(None))]
#[case(
    "file:///tmp/report.txt",
    UrlComponent::Path,
    present("/tmp/report.txt")
)]
#[case(
    "  https://example.com/\t  ",
    UrlComponent::Host,
    present("example.com")
)]
fn components_follow_the_url_standard(
    #[case] text: &str,
    #[case] part: UrlComponent,
    #[case] expected: Result<Option<String>, SideErrorReason>,
) {
    assert_eq!(component_of(text, part), expected);
}

#[rstest]
#[case("/search?q=x", ParseError::RelativeUrlWithoutBase)]
#[case("", ParseError::RelativeUrlWithoutBase)]
#[case("example.com", ParseError::RelativeUrlWithoutBase)]
#[case("http://example.com:99999/", ParseError::InvalidPort)]
#[case("http://[::1/", ParseError::InvalidIpv6Address)]
#[case("https://", ParseError::EmptyHost)]
#[case("http://exa mple.com/", ParseError::IdnaError)]
#[case("custom://exa<mple/", ParseError::InvalidDomainCharacter)]
fn text_that_is_not_an_absolute_url_fails_its_row(#[case] text: &str, #[case] defect: ParseError) {
    assert_eq!(
        component_of(text, UrlComponent::Host),
        Err(SideErrorReason::InvalidUrl {
            operation: UrlOperation::UrlHost,
            defect,
        })
    );
}

#[test]
fn invalid_urls_name_the_function_and_the_standard_s_reason() {
    let reason = SideErrorReason::InvalidUrl {
        operation: UrlOperation::UrlPath,
        defect: ParseError::RelativeUrlWithoutBase,
    };
    assert_eq!(
        reason.to_string(),
        "url_path input is not an absolute URL: relative URL without a base"
    );
    assert_eq!(
        SideErrorReason::InvalidPercentEncoding(UrlOperation::UrlDecode).to_string(),
        "url_decode input is not valid percent-encoded UTF-8"
    );
    assert_eq!(
        SideErrorReason::QueryValuesTooLong.to_string(),
        "url_query_values result exceeds what one VEC<STRING> column holds"
    );
}

#[test]
fn ports_are_the_written_port_or_the_scheme_s_default() {
    let input = StringArray::from(vec![
        Some("https://example.com:8443/"),
        Some("https://example.com/"),
        Some("http://example.com:80/"),
        Some("ws://example.com/"),
        Some("wss://example.com/"),
        Some("ftp://example.com/"),
        Some("custom://example.com:7/"),
        Some("custom://example.com/"),
        Some("mailto:ops@example.com"),
        Some("http://example.com:0/"),
        Some("not a url"),
        None,
    ]);
    let mut errors = RowErrors::new(input.len());
    let ports = port(&input, &mut errors, SPAN);
    assert_eq!(
        ports.iter().collect::<Vec<_>>(),
        [
            Some(8443),
            Some(443),
            Some(80),
            Some(80),
            Some(443),
            Some(21),
            Some(7),
            None,
            None,
            Some(0),
            None,
            None
        ]
    );
    assert_eq!(
        errors.row(10)[0].reason,
        SideErrorReason::InvalidUrl {
            operation: UrlOperation::UrlPort,
            defect: ParseError::RelativeUrlWithoutBase,
        }
    );
    assert!(errors.row(11).is_empty());
}

#[rstest]
#[case("a%20b", Some("a b"))]
#[case("a+b", Some("a+b"))]
#[case("caf%c3%A9", Some("café"))]
#[case("100%25", Some("100%"))]
#[case("plain", Some("plain"))]
#[case("", Some(""))]
#[case("100%", None)]
#[case("%4", None)]
#[case("%zz", None)]
#[case("%ff", None)]
#[case("%C3", None)]
fn decoding_is_strict_about_escapes_and_utf8(#[case] text: &str, #[case] expected: Option<&str>) {
    let input = StringArray::from(vec![text]);
    let mut errors = RowErrors::new(1);
    let decoded = decode(&input, NonZeroUsize::MIN, &mut errors, SPAN);
    match expected {
        Some(expected) => {
            assert!(errors.is_error_free(), "{text}");
            assert_eq!(decoded.value(0), expected);
        }
        None => {
            assert!(decoded.is_null(0), "{text}");
            assert_eq!(
                errors.row(0)[0].reason,
                SideErrorReason::InvalidPercentEncoding(UrlOperation::UrlDecode)
            );
        }
    }
}

/// The first value each URL carries for `name`, read as a column of URLs.
fn first_values(urls: &[Option<&str>], name: &str) -> (StringArray, RowErrors) {
    let urls = StringArray::from(urls.to_vec());
    let names = StringArray::from(vec![name]);
    let mut errors = RowErrors::new(urls.len());
    let values = query_value(
        Operand::Column(&urls),
        Operand::Scalar(&names),
        urls.len(),
        NonZeroUsize::MIN,
        &mut errors,
        SPAN,
    );
    (values, errors)
}

/// Every value each URL carries for `name`, read as a column of URLs.
fn all_values(urls: &[Option<&str>], name: &str) -> (ListArray, RowErrors) {
    let urls = StringArray::from(urls.to_vec());
    let names = StringArray::from(vec![name]);
    let mut errors = RowErrors::new(urls.len());
    let values = query_values(
        Operand::Column(&urls),
        Operand::Scalar(&names),
        urls.len(),
        NonZeroUsize::MIN,
        &mut errors,
        SPAN,
    );
    (values, errors)
}

/// The values of one row of a `url_query_values` result.
fn list_row(values: &ListArray, row: usize) -> Vec<String> {
    let row = values.value(row);
    let row = row
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("url_query_values lists STRING values");
    row.iter()
        .map(|value| value.expect("a query value is never null").to_string())
        .collect()
}

#[test]
fn query_values_decode_as_a_form_and_repeat_in_order() {
    let urls = [
        Some("https://example.com/?q=hello+world%21&tag=x&tag=y%26z"),
        Some("https://example.com/?tag=&tag=a+b&&tag"),
        Some("https://example.com/?ta%67=escaped-name&other=1"),
        Some("https://example.com/?%zz=bad-name&tag=kept"),
        Some("https://example.com/?q=1"),
        Some("https://example.com/"),
        None,
    ];
    let (first, errors) = first_values(&urls, "tag");
    assert!(errors.is_error_free());
    assert_eq!(
        first.iter().collect::<Vec<_>>(),
        [
            Some("x"),
            Some(""),
            Some("escaped-name"),
            Some("kept"),
            None,
            None,
            None
        ]
    );

    let (all, errors) = all_values(&urls, "tag");
    assert!(errors.is_error_free());
    assert_eq!(list_row(&all, 0), ["x", "y&z"]);
    assert_eq!(list_row(&all, 1), ["", "a b", ""]);
    assert_eq!(list_row(&all, 2), ["escaped-name"]);
    assert_eq!(list_row(&all, 3), ["kept"]);
    assert!(list_row(&all, 4).is_empty());
    assert!(list_row(&all, 5).is_empty());
    assert!(all.is_null(6));

    let (first, _) = first_values(&urls, "q");
    assert_eq!(first.value(0), "hello world!");
}

#[test]
fn a_value_that_does_not_decode_fails_only_the_call_that_reads_it() {
    let urls = [Some("https://example.com/?bad=%E0%A4&good=1")];
    let (first, errors) = first_values(&urls, "good");
    assert!(errors.is_error_free());
    assert_eq!(first.value(0), "1");

    let (first, errors) = first_values(&urls, "bad");
    assert!(first.is_null(0));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::InvalidPercentEncoding(UrlOperation::UrlQueryValue)
    );

    let (all, errors) = all_values(&urls, "bad");
    assert!(all.is_null(0));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::InvalidPercentEncoding(UrlOperation::UrlQueryValues)
    );

    let (first, errors) = first_values(&[Some("/relative?good=1")], "good");
    assert!(first.is_null(0));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::InvalidUrl {
            operation: UrlOperation::UrlQueryValue,
            defect: ParseError::RelativeUrlWithoutBase,
        }
    );
}

#[test]
fn one_url_every_row_shares_answers_each_row_s_name() {
    let url = StringArray::from(vec!["https://example.com/?a=1&b=2&a=3"]);
    let names = StringArray::from(vec![Some("a"), Some("b"), Some("c"), None]);
    let mut errors = RowErrors::new(names.len());
    let first = query_value(
        Operand::Scalar(&url),
        Operand::Column(&names),
        names.len(),
        NonZeroUsize::MIN,
        &mut errors,
        SPAN,
    );
    assert_eq!(
        first.iter().collect::<Vec<_>>(),
        [Some("1"), Some("2"), None, None]
    );
    let all = query_values(
        Operand::Scalar(&url),
        Operand::Column(&names),
        names.len(),
        NonZeroUsize::MIN,
        &mut errors,
        SPAN,
    );
    assert!(errors.is_error_free());
    assert_eq!(list_row(&all, 0), ["1", "3"]);
    assert_eq!(list_row(&all, 1), ["2"]);
    assert!(list_row(&all, 2).is_empty());
    assert!(all.is_null(3));
}

#[test]
fn results_that_every_row_shares_are_charged_for_every_row() {
    let input = StringArray::from(vec!["https://example.com/path?q=1#top"]);
    for (part, operation) in [
        (UrlComponent::Scheme, TextOperation::UrlScheme),
        (UrlComponent::Host, TextOperation::UrlHost),
        (UrlComponent::Path, TextOperation::UrlPath),
        (UrlComponent::Query, TextOperation::UrlQuery),
        (UrlComponent::Fragment, TextOperation::UrlFragment),
    ] {
        let mut errors = RowErrors::new(1);
        let output = component(&input, part, NonZeroUsize::MAX, &mut errors, SPAN);
        assert!(output.is_null(0));
        assert_eq!(
            errors.row(0)[0].reason,
            SideErrorReason::TextTooLong(operation)
        );
    }

    let mut errors = RowErrors::new(1);
    let decoded = decode(&input, NonZeroUsize::MAX, &mut errors, SPAN);
    assert!(decoded.is_null(0));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::TextTooLong(TextOperation::UrlDecode)
    );

    let names = StringArray::from(vec!["q"]);
    let mut errors = RowErrors::new(1);
    let first = query_value(
        Operand::Column(&input),
        Operand::Column(&names),
        1,
        NonZeroUsize::MAX,
        &mut errors,
        SPAN,
    );
    assert!(first.is_null(0));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::TextTooLong(TextOperation::UrlQueryValue)
    );

    let mut errors = RowErrors::new(1);
    let all = query_values(
        Operand::Column(&input),
        Operand::Column(&names),
        1,
        NonZeroUsize::MAX,
        &mut errors,
        SPAN,
    );
    assert!(all.is_null(0));
    assert_eq!(errors.row(0)[0].reason, SideErrorReason::QueryValuesTooLong);

    // An empty list stands for no value and no text, however many rows share it.
    let mut errors = RowErrors::new(1);
    let none = query_values(
        Operand::Column(&input),
        Operand::Column(&StringArray::from(vec!["absent"])),
        1,
        NonZeroUsize::MAX,
        &mut errors,
        SPAN,
    );
    assert!(errors.is_error_free());
    assert!(list_row(&none, 0).is_empty());
}

#[test]
fn url_validity_is_what_the_url_builtins_read() {
    let input = StringArray::from(vec![
        Some("https://example.com/"),
        Some("mailto:ops@example.com"),
        Some("/relative"),
        Some("http://exa mple.com/"),
        None,
    ]);
    assert_eq!(
        is_url(&input).iter().collect::<Vec<_>>(),
        [Some(true), Some(true), Some(false), Some(false), None]
    );
}

#[test]
fn null_urls_and_texts_produce_nulls_without_errors() {
    let input = StringArray::from(vec![None::<&str>]);
    let mut errors = RowErrors::new(1);
    assert!(
        component(
            &input,
            UrlComponent::Host,
            NonZeroUsize::MIN,
            &mut errors,
            SPAN
        )
        .is_null(0)
    );
    assert!(port(&input, &mut errors, SPAN).is_null(0));
    assert!(decode(&input, NonZeroUsize::MIN, &mut errors, SPAN).is_null(0));
    assert!(errors.is_error_free());
}
