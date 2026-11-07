//! Repository tooling, outside the product layer order.
//! Owns: the current audit's sorted JSON and stable identity preimages.
//! Depends on: Serde JSON and its maintained configurable formatter for every JSON byte.
//! Must not know: candidate attribution or compiler policy.

use std::io::{self, Write};

use error_stack::ResultExt as _;
use meticulous::ResultExt as _;
use serde::Serialize;
use serde_json::ser::{CharEscape, Formatter};
use serde_json_fmt::{JsonFormat, JsonFormatter};

use crate::error::{Error, Result};

pub(super) struct Encoding {
    identity: JsonFormat,
    report: JsonFormat,
}

impl Encoding {
    pub fn new() -> Self {
        let identity = JsonFormat::new()
            .ascii(true)
            .comma(", ")
            .assured("a comma followed by a space is a valid separator")
            .colon(": ")
            .assured("a colon followed by a space is a valid separator");
        Self {
            identity,
            report: JsonFormat::new().ascii(true),
        }
    }

    pub fn identity(&self, value: &impl Serialize) -> Result<String> {
        let bytes = self.encode(value, &self.identity)?;
        Ok(String::from_utf8(bytes).assured("the maintained JSON formatter writes UTF-8"))
    }

    pub fn report(&self, value: &impl Serialize) -> Result<Vec<u8>> {
        let mut bytes = self.encode(value, &self.report)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    fn encode(&self, value: &impl Serialize, format: &JsonFormat) -> Result<Vec<u8>> {
        let mut value = serde_json::to_value(value).change_context(Error::Json {
            path: "audit encoding".into(),
        })?;
        value.sort_all_objects();
        let mut bytes = Vec::new();
        let formatter = AuditFormatter {
            formatter: format.clone().build(),
        };
        let mut serializer = serde_json::Serializer::with_formatter(&mut bytes, formatter);
        value
            .serialize(&mut serializer)
            .change_context(Error::Json {
                path: "audit encoding".into(),
            })?;
        Ok(bytes)
    }
}

/// The identity contract escapes DEL as well as non-ASCII. Both escaping operations remain
/// with maintained formatters; this adapter selects the existing ASCII-control operation.
struct AuditFormatter {
    formatter: JsonFormatter,
}

impl Formatter for AuditFormatter {
    fn begin_array<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.formatter.begin_array(writer)
    }
    fn end_array<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.formatter.end_array(writer)
    }
    fn begin_array_value<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.formatter.begin_array_value(writer, first)
    }
    fn end_array_value<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.formatter.end_array_value(writer)
    }
    fn begin_object<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.formatter.begin_object(writer)
    }
    fn end_object<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.formatter.end_object(writer)
    }
    fn begin_object_key<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.formatter.begin_object_key(writer, first)
    }
    fn end_object_key<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.formatter.end_object_key(writer)
    }
    fn begin_object_value<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.formatter.begin_object_value(writer)
    }
    fn end_object_value<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.formatter.end_object_value(writer)
    }

    fn write_string_fragment<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        let mut fragments = fragment.split('\u{7f}');
        if let Some(first) = fragments.next() {
            self.formatter.write_string_fragment(writer, first)?;
        }
        for next in fragments {
            self.formatter
                .write_char_escape(writer, CharEscape::AsciiControl(0x7f))?;
            self.formatter.write_string_fragment(writer, next)?;
        }
        Ok(())
    }
}
