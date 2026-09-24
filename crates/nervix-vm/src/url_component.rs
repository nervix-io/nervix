//! URL components read under the URL Standard, and strict percent-decoding.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Parsing absolute URLs with the `url` crate, the components builtins extract from
//!   them, reading `application/x-www-form-urlencoded` query parameters by name, and
//!   percent-decoding that rejects a malformed escape or text that is not UTF-8.
//! - **Depends on.** The `url` crate, Arrow string, integer, Boolean and list columns, and the VM's
//!   row errors.
//! - **Must not know.** NSPL syntax, routes, schemas or connectors. It never resolves a host,
//!   fetches a URL or consults a public-suffix list.

use std::num::NonZeroUsize;

use arrow_array::{
    Array, BooleanArray, Int64Array, ListArray, StringArray,
    builder::{BooleanBuilder, Int64Builder, ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field};
use meticulous::OptionExt as _;
use url::{Host, ParseError, Url};

use crate::{
    RowErrors, SideError, SideErrorReason,
    error::{TextOperation, UrlOperation},
    operand::Operand,
    program::Span,
    text_column::TextColumnBuilder,
};

/// A component of a URL that a builtin returns as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UrlComponent {
    /// `url_scheme`: the scheme, lowercased, without its `:`.
    Scheme,
    /// `url_host`: the host, without the brackets around an IPv6 address.
    Host,
    /// `url_path`: the path, percent-encoded as the URL Standard serializes it.
    Path,
    /// `url_query`: the query without its `?`.
    Query,
    /// `url_fragment`: the fragment without its `#`.
    Fragment,
}

impl UrlComponent {
    const fn operation(self) -> UrlOperation {
        match self {
            Self::Scheme => UrlOperation::UrlScheme,
            Self::Host => UrlOperation::UrlHost,
            Self::Path => UrlOperation::UrlPath,
            Self::Query => UrlOperation::UrlQuery,
            Self::Fragment => UrlOperation::UrlFragment,
        }
    }

    const fn text_operation(self) -> TextOperation {
        match self {
            Self::Scheme => TextOperation::UrlScheme,
            Self::Host => TextOperation::UrlHost,
            Self::Path => TextOperation::UrlPath,
            Self::Query => TextOperation::UrlQuery,
            Self::Fragment => TextOperation::UrlFragment,
        }
    }

    /// The component of `url`, or `None` when the URL has none.
    fn of(self, url: &Url) -> Option<&str> {
        match self {
            Self::Scheme => Some(url.scheme()),
            Self::Host => {
                let host = url.host_str()?;
                // A URL writes an IPv6 host in brackets, which are not part of the address.
                if let Some(Host::Ipv6(_)) = url.host() {
                    let unbracketed = host.strip_prefix('[')?;
                    return unbracketed.strip_suffix(']');
                }
                Some(host)
            }
            Self::Path => Some(url.path()),
            Self::Query => url.query(),
            Self::Fragment => url.fragment(),
        }
    }
}

/// Records that `row` of a URL builtin read text that is not an absolute URL.
fn invalid_url(
    errors: &mut RowErrors,
    row: usize,
    operation: UrlOperation,
    defect: ParseError,
    span: Span,
) {
    errors.push(
        row,
        SideError {
            reason: SideErrorReason::InvalidUrl { operation, defect },
            span,
        },
    );
}

/// Records that `row` of a builtin returning text had no room left in its column.
fn text_too_long(errors: &mut RowErrors, row: usize, operation: TextOperation, span: Span) {
    errors.push(
        row,
        SideError {
            reason: SideErrorReason::TextTooLong(operation),
            span,
        },
    );
}

/// Records that `row` of a URL builtin read text that is not percent-encoded UTF-8.
fn invalid_percent_encoding(
    errors: &mut RowErrors,
    row: usize,
    operation: UrlOperation,
    span: Span,
) {
    errors.push(
        row,
        SideError {
            reason: SideErrorReason::InvalidPercentEncoding(operation),
            span,
        },
    );
}

/// `url_scheme`, `url_host`, `url_path`, `url_query` and `url_fragment`: one component of each
/// URL, or null where the URL has none.
pub(crate) fn component(
    input: &StringArray,
    component: UrlComponent,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let url = match Url::parse(input.value(row)) {
            Ok(url) => url,
            Err(defect) => {
                output.append_null();
                invalid_url(errors, row, component.operation(), defect, span);
                continue;
            }
        };
        let Some(text) = component.of(&url) else {
            output.append_null();
            continue;
        };
        if !output.append_value(text) {
            output.append_null();
            text_too_long(errors, row, component.text_operation(), span);
        }
    }
    output.finish()
}

/// `url_port`: the port each URL names, or its scheme's default port when it names none.
pub(crate) fn port(input: &StringArray, errors: &mut RowErrors, span: Span) -> Int64Array {
    let mut output = Int64Builder::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        match Url::parse(input.value(row)) {
            Ok(url) => match url.port_or_known_default() {
                Some(port) => output.append_value(i64::from(port)),
                None => output.append_null(),
            },
            Err(defect) => {
                output.append_null();
                invalid_url(errors, row, UrlOperation::UrlPort, defect, span);
            }
        }
    }
    output.finish()
}

/// Whether `text` is an absolute URL the other URL builtins read. Folding `is_url` over a literal
/// and executing it over a column both answer through this test.
pub(crate) fn is_url_text(text: &str) -> bool {
    Url::parse(text).is_ok()
}

/// `is_url`: whether each text is an absolute URL the other URL builtins read.
pub(crate) fn is_url(input: &StringArray) -> BooleanArray {
    let mut output = BooleanBuilder::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
        } else {
            output.append_value(is_url_text(input.value(row)));
        }
    }
    output.finish()
}

/// `url_decode`: each text with its percent escapes decoded. A `+` stays a `+`.
pub(crate) fn decode(
    input: &StringArray,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    let mut decoder = PercentDecoder::default();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let Some(decoded) = decoder.decode(input.value(row), PlusSign::Literal) else {
            output.append_null();
            invalid_percent_encoding(errors, row, UrlOperation::UrlDecode, span);
            continue;
        };
        if !output.append_value(decoded) {
            output.append_null();
            text_too_long(errors, row, TextOperation::UrlDecode, span);
        }
    }
    output.finish()
}

/// How a percent-decoder reads a `+`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlusSign {
    /// A `+` is itself, as in a path or any other percent-encoded text.
    Literal,
    /// A `+` is a space, as in an `application/x-www-form-urlencoded` query.
    Space,
}

/// Strict percent-decoding into a buffer reused across values.
#[derive(Debug, Default)]
struct PercentDecoder {
    decoded: Vec<u8>,
}

impl PercentDecoder {
    /// Decodes `encoded`: a `%` followed by two hexadecimal digits writes the octet they name, a
    /// `+` writes a space when `plus` says so, and every other character writes itself. Answers
    /// `None` when a `%` is not followed by two hexadecimal digits or the decoded octets are not
    /// UTF-8. Text without an escape is answered as it is, without copying it.
    fn decode<'a>(&'a mut self, encoded: &'a str, plus: PlusSign) -> Option<&'a str> {
        let has_escape = encoded
            .bytes()
            .any(|byte| byte == b'%' || (byte == b'+' && plus == PlusSign::Space));
        if !has_escape {
            return Some(encoded);
        }
        self.decoded.clear();
        let mut bytes = encoded.bytes();
        while let Some(byte) = bytes.next() {
            let octet = match byte {
                b'%' => {
                    let high = Self::hex_digit(bytes.next()?)?;
                    let low = Self::hex_digit(bytes.next()?)?;
                    (high << 4) | low
                }
                b'+' if plus == PlusSign::Space => b' ',
                other => other,
            };
            self.decoded.push(octet);
        }
        std::str::from_utf8(&self.decoded).ok()
    }

    /// The value of one hexadecimal digit of either letter case.
    fn hex_digit(digit: u8) -> Option<u8> {
        let value = char::from(digit).to_digit(16)?;
        u8::try_from(value).ok()
    }
}

/// The parameters of an `application/x-www-form-urlencoded` query that carry one name.
///
/// A query is split into pairs at every `&`, an empty pair is skipped, and a pair is split into its
/// name and value at its first `=`, a pair without one having an empty value. Names and values are
/// percent-decoded with `+` read as a space. A pair whose name does not decode names no parameter;
/// the value of a pair that carries the name must decode.
struct QueryParameters {
    names: PercentDecoder,
    values: PercentDecoder,
}

/// Why a query parameter could not be read.
struct UndecodableValue;

impl QueryParameters {
    fn new() -> Self {
        Self {
            names: PercentDecoder::default(),
            values: PercentDecoder::default(),
        }
    }

    /// Calls `found` with the decoded value of every pair of `query` named `name`, in query
    /// order, and stops at the first value that does not decode.
    fn each_value(
        &mut self,
        query: &str,
        name: &str,
        mut found: impl FnMut(&str) -> Continue,
    ) -> Result<(), UndecodableValue> {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (raw_name, raw_value) = match pair.split_once('=') {
                Some((raw_name, raw_value)) => (raw_name, raw_value),
                None => (pair, ""),
            };
            let Some(decoded_name) = self.names.decode(raw_name, PlusSign::Space) else {
                continue;
            };
            if decoded_name != name {
                continue;
            }
            let Some(value) = self.values.decode(raw_value, PlusSign::Space) else {
                return Err(UndecodableValue);
            };
            if let Continue::Stop = found(value) {
                return Ok(());
            }
        }
        Ok(())
    }
}

/// Whether a scan over query parameters goes on after a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Continue {
    Next,
    Stop,
}

/// The URL one row of a two-argument URL builtin reads, parsed once for as long as the rows read
/// the same URL, which they do when the URL is one value every row shares.
struct RowUrls<'a> {
    urls: Operand<'a, StringArray>,
    parsed: Option<ParsedUrl>,
}

struct ParsedUrl {
    index: usize,
    url: Result<Url, ParseError>,
}

impl<'a> RowUrls<'a> {
    fn new(urls: Operand<'a, StringArray>) -> Self {
        Self { urls, parsed: None }
    }

    /// Whether the URL of `row` is null.
    fn is_null(&self, row: usize) -> bool {
        self.urls.is_null(row)
    }

    /// The URL of `row`, which must not be null.
    fn url(&mut self, row: usize) -> &Result<Url, ParseError> {
        let index = self.urls.index(row);
        let reusable = match &self.parsed {
            Some(parsed) => parsed.index == index,
            None => false,
        };
        if !reusable {
            let url = Url::parse(self.urls.array().value(index));
            self.parsed = Some(ParsedUrl { index, url });
        }
        let parsed = self
            .parsed
            .as_ref()
            .verified("the URL of this row was parsed above when it was not already");
        &parsed.url
    }
}

/// `url_query_value`: the first value each URL's query carries for its row's name, or null where
/// the URL has no query or its query carries no such name.
pub(crate) fn query_value(
    urls: Operand<'_, StringArray>,
    names: Operand<'_, StringArray>,
    rows: usize,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    let mut urls = RowUrls::new(urls);
    let mut parameters = QueryParameters::new();
    let mut value = String::new();
    for row in 0..rows {
        if urls.is_null(row) || names.is_null(row) {
            output.append_null();
            continue;
        }
        let url = match urls.url(row) {
            Ok(url) => url,
            Err(defect) => {
                output.append_null();
                invalid_url(errors, row, UrlOperation::UrlQueryValue, *defect, span);
                continue;
            }
        };
        let Some(query) = url.query() else {
            output.append_null();
            continue;
        };
        let name = names.array().value(names.index(row));
        let mut found = false;
        let scanned = parameters.each_value(query, name, |first| {
            value.clear();
            value.push_str(first);
            found = true;
            Continue::Stop
        });
        if let Err(UndecodableValue) = scanned {
            output.append_null();
            invalid_percent_encoding(errors, row, UrlOperation::UrlQueryValue, span);
            continue;
        }
        if !found {
            output.append_null();
            continue;
        }
        if !output.append_value(&value) {
            output.append_null();
            text_too_long(errors, row, TextOperation::UrlQueryValue, span);
        }
    }
    output.finish()
}

/// The values one row of `url_query_values` found, decoded into one text with the end of each.
#[derive(Default)]
struct FoundValues {
    text: String,
    ends: Vec<usize>,
}

/// The `VEC<STRING>` column `url_query_values` builds, which never holds more values or text than
/// an Arrow list of strings addresses.
struct ValueListColumn {
    builder: ListBuilder<StringBuilder>,
    /// How many rows of the finished column each list appended here stands for.
    rows_per_value: NonZeroUsize,
    /// How many values the lists appended so far hold.
    values: usize,
}

impl ValueListColumn {
    fn new(rows_per_value: NonZeroUsize) -> Self {
        let item = Field::new("item", DataType::Utf8, false);
        Self {
            builder: ListBuilder::new(StringBuilder::new()).with_field(item),
            rows_per_value,
            values: 0,
        }
    }

    /// Appends one list holding `found`, or answers false without appending anything when its
    /// values or their text do not fit in what the column has left. A list that every row shares
    /// is charged once for every row it stands for.
    #[must_use]
    fn append(&mut self, found: &FoundValues) -> bool {
        let rows = self.rows_per_value.get();
        let Some(values) = found.ends.len().checked_mul(rows) else {
            return false;
        };
        let Some(text) = found.text.len().checked_mul(rows) else {
            return false;
        };
        let Some(total_values) = self.values.checked_add(values) else {
            return false;
        };
        let Some(total_text) = self
            .builder
            .values_ref()
            .values_slice()
            .len()
            .checked_add(text)
        else {
            return false;
        };
        // A list array records where each list ends, and a string array where each value ends,
        // as `i32` offsets.
        if i32::try_from(total_values).is_err() || i32::try_from(total_text).is_err() {
            return false;
        }
        let mut start = 0;
        for &end in &found.ends {
            self.builder.values().append_value(&found.text[start..end]);
            start = end;
        }
        self.builder.append(true);
        self.values = total_values;
        true
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(mut self) -> ListArray {
        self.builder.finish()
    }
}

/// `url_query_values`: every value each URL's query carries for its row's name, in query order,
/// and an empty list where the URL has no query or its query carries no such name.
pub(crate) fn query_values(
    urls: Operand<'_, StringArray>,
    names: Operand<'_, StringArray>,
    rows: usize,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> ListArray {
    let mut output = ValueListColumn::new(rows_per_value);
    let mut urls = RowUrls::new(urls);
    let mut parameters = QueryParameters::new();
    let mut found = FoundValues::default();
    for row in 0..rows {
        if urls.is_null(row) || names.is_null(row) {
            output.append_null();
            continue;
        }
        let url = match urls.url(row) {
            Ok(url) => url,
            Err(defect) => {
                output.append_null();
                invalid_url(errors, row, UrlOperation::UrlQueryValues, *defect, span);
                continue;
            }
        };
        found.text.clear();
        found.ends.clear();
        if let Some(query) = url.query() {
            let name = names.array().value(names.index(row));
            let scanned = parameters.each_value(query, name, |value| {
                found.text.push_str(value);
                found.ends.push(found.text.len());
                Continue::Next
            });
            if let Err(UndecodableValue) = scanned {
                output.append_null();
                invalid_percent_encoding(errors, row, UrlOperation::UrlQueryValues, span);
                continue;
            }
        }
        if !output.append(&found) {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::QueryValuesTooLong,
                    span,
                },
            );
        }
    }
    output.finish()
}

#[cfg(test)]
#[path = "url_component_tests.rs"]
mod tests;
