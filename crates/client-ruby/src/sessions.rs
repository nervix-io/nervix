//! Ruby sessions, prepared commands, outcomes and completion pages.
//!
//! Layer: edges.
//! - **Owns.** Connection options, Ruby cancellation and command objects, and released-GVL waits.
//! - **Depends on.** The host session runtime, typed outcomes and magnus keyword conversion.
//! - **Must not know.** Server state, routing, request correlation or recovery decisions.

use std::{cell::RefCell, time::Duration};

use magnus::{
    DataTypeFunctions, Error, RArray, RHash, RModule, RString, Ruby, TypedData, Value, function,
    method,
    prelude::*,
    scan_args::{get_kwargs, scan_args},
    typed_data::Obj,
};
use meticulous::ResultExt as _;
use nervix_client_core::{ConnectOptions, DomainName, SourceSpan, TlsRequirement};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;
use url::Url;

use crate::{
    clocks::{ClockEvent, DomainClock},
    enums, errors, gvl,
    schema::Schema,
    subscriptions::{Event, Subscription},
    values,
};

pub(crate) fn seconds(argument: &'static str, value: f64) -> Result<Duration, Error> {
    Duration::try_from_secs_f64(value).map_err(|error| {
        errors::invalid(
            argument,
            &format!("must be finite non-negative seconds: {error}"),
        )
    })
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Cancel", free_immediately)]
pub(crate) struct Cancel {
    cancel: host::Cancel,
}

impl Cancel {
    fn new(args: &[Value]) -> Result<Self, Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let kwargs =
            get_kwargs::<_, (), (Option<Option<f64>>,), ()>(args.keywords, &[], &["deadline"])?;
        let deadline = kwargs.optional.0.flatten();
        let cancel = match deadline {
            Some(deadline) => host::Cancel::with_deadline(seconds("deadline", deadline)?)
                .map_err(errors::raise)?,
            None => host::Cancel::default(),
        };
        Ok(Self { cancel })
    }
    fn trigger(&self) {
        self.cancel.trigger();
    }
    pub(crate) fn child(&self) -> host::Cancel {
        // Finish every borrow into Ruby-owned typed data before releasing the GVL. The call
        // keeps this native child even if an ephemeral keyword token becomes collectible.
        self.cancel.child()
    }
    pub(crate) fn keyword(keywords: RHash) -> Result<Option<host::Cancel>, Error> {
        let kwargs =
            get_kwargs::<_, (), (Option<Option<Obj<Self>>>,), ()>(keywords, &[], &["cancel"])?;
        Ok(kwargs.optional.0.flatten().map(|cancel| cancel.child()))
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Execution", free_immediately)]
pub(crate) struct Execution {
    // A released-GVL call holds a second native owner until the execution attempt returns.
    execution: Arc<host::Execution>,
}

impl Execution {
    fn reference(&self) -> &str {
        self.execution.reference()
    }
}

struct Connection {
    server: String,
    domain: Option<String>,
    options: ConnectOptions,
    cancel: Option<host::Cancel>,
}

impl Connection {
    fn parse(args: &[Value]) -> Result<Self, Error> {
        let args = scan_args::<(String,), (), (), (), RHash, ()>(args)?;
        let (server,) = args.required;
        // Magnus's fixed keyword-scanner interface uses a tuple; native connection state is
        // named and no Ruby value is retained in the options handed to the host runtime.
        let kwargs = get_kwargs::<
            _,
            (),
            (
                Option<Option<String>>,
                Option<Option<String>>,
                Option<Option<String>>,
                Option<Option<i32>>,
                Option<Option<RString>>,
                Option<Option<Vec<String>>>,
                Option<Option<f64>>,
                Option<Option<f64>>,
                Option<Option<f64>>,
            ),
            RHash,
        >(
            args.keywords,
            &[],
            &[
                "domain",
                "username",
                "password",
                "tls",
                "ca_certificate",
                "seeds",
                "connect_timeout",
                "request_timeout",
                "retry_timeout",
            ],
        )?;
        let (
            domain,
            username,
            password,
            tls,
            ca_certificate,
            seeds,
            connect_timeout,
            request_timeout,
            retry_timeout,
        ) = kwargs.optional;
        let domain = domain.flatten();
        let username = username.flatten();
        let password = password.flatten();
        let tls = tls.flatten();
        let ca_certificate = ca_certificate.flatten();
        let seeds = seeds.flatten();
        let connect_timeout = connect_timeout.flatten();
        let request_timeout = request_timeout.flatten();
        let retry_timeout = retry_timeout.flatten();
        let more = get_kwargs::<_, (), (Option<Option<f64>>, Option<Option<Obj<Cancel>>>), ()>(
            kwargs.splat,
            &[],
            &["backup_wait_timeout", "cancel"],
        )?;
        let (backup_wait_timeout, cancel) = more.optional;
        let backup_wait_timeout = backup_wait_timeout.flatten();
        let cancel = cancel.flatten().map(|cancel| cancel.child());
        let mut options = ConnectOptions::default();
        match (username, password) {
            (Some(username), Some(password)) => {
                options = options.with_basic_auth(username, password)
            }
            (None, None) => {}
            _ => {
                return Err(errors::invalid(
                    "username",
                    "and password must both be supplied or both be absent",
                ));
            }
        }
        if let Some(tls) = tls {
            let requirement = match tls {
                1 => TlsRequirement::Preferred,
                2 => TlsRequirement::Required,
                _ => {
                    return Err(errors::invalid(
                        "tls",
                        "must be Tls::PREFERRED or Tls::REQUIRED",
                    ));
                }
            };
            options.tls_requirement = Some(requirement);
        }
        if let Some(certificate) = ca_certificate {
            options.ca_certificate_pem = Some(values::bytes(certificate));
        }
        if let Some(seeds) = seeds {
            let mut parsed = Vec::with_capacity(seeds.len());
            for seed in seeds {
                let seed = Url::parse(&seed).map_err(|error| {
                    errors::invalid("seeds", &format!("contains an invalid URL: {error}"))
                })?;
                parsed.push(seed);
            }
            options.seed_servers = parsed;
        }
        if let Some(timeout) = connect_timeout {
            options.connect_timeout = seconds("connect_timeout", timeout)?;
        }
        if let Some(timeout) = request_timeout {
            options.request_timeout = seconds("request_timeout", timeout)?;
        }
        if let Some(timeout) = retry_timeout {
            options.retry_timeout = seconds("retry_timeout", timeout)?;
        }
        if let Some(timeout) = backup_wait_timeout {
            options.backup_wait_timeout = seconds("backup_wait_timeout", timeout)?;
        }
        Ok(Self {
            server,
            domain,
            options,
            cancel,
        })
    }

    fn connect(self) -> Result<Session, Error> {
        let token = self.cancel;
        let session = gvl::without_gvl(token, |cancel| {
            host::Session::connect(
                &self.server,
                self.domain.as_deref(),
                self.options,
                Some(cancel),
            )
        })?;
        Ok(Session {
            session: RefCell::new(Some(session)),
        })
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Session", free_immediately)]
pub(crate) struct Session {
    session: RefCell<Option<host::Session>>,
}

impl Session {
    fn connect(args: &[Value]) -> Result<Self, Error> {
        Connection::parse(args)?.connect()
    }
    pub(crate) fn held(&self) -> Result<host::Session, Error> {
        let session = self
            .session
            .try_borrow()
            .map_err(|_| errors::borrowed("session"))?;
        match session.as_ref() {
            Some(session) => Ok(session.clone()),
            None => Err(errors::closed("session")),
        }
    }
    fn close(&self) -> Result<(), Error> {
        let session = self
            .session
            .try_borrow_mut()
            .map_err(|_| errors::borrowed("session"))?
            .take();
        drop(session);
        Ok(())
    }
    fn prepare(&self, args: &[Value]) -> Result<Execution, Error> {
        let args = scan_args::<(String,), (), (), (), RHash, ()>(args)?;
        let (query,) = args.required;
        let token = Cancel::keyword(args.keywords)?;
        let session = self.held()?;
        let execution = gvl::without_gvl(token, |cancel| session.prepare(&query, Some(cancel)))?;
        Ok(Execution {
            execution: Arc::new(execution),
        })
    }
    fn execute(&self, args: &[Value]) -> Result<Outcome, Error> {
        let args = scan_args::<(Obj<Execution>,), (), (), (), RHash, ()>(args)?;
        let (execution,) = args.required;
        let execution = execution.execution.clone();
        let token = Cancel::keyword(args.keywords)?;
        let session = self.held()?;
        let outcome = gvl::without_gvl(token, |cancel| session.execute(&execution, Some(cancel)))?;
        Ok(Outcome { outcome })
    }
    fn next_event(&self, args: &[Value]) -> Result<Event, Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let token = Cancel::keyword(args.keywords)?;
        let session = self.held()?;
        let event = gvl::without_gvl(token, |cancel| session.next_event(Some(cancel)))?;
        Ok(Event::new(event))
    }
    fn next_clock_event(&self, args: &[Value]) -> Result<ClockEvent, Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let token = Cancel::keyword(args.keywords)?;
        let session = self.held()?;
        let event = gvl::without_gvl(token, |cancel| session.next_clock_event(Some(cancel)))?;
        Ok(ClockEvent::new(event))
    }
    fn domain_clock(&self, domain: String) -> Result<Option<DomainClock>, Error> {
        let domain = DomainName::try_from(domain)
            .map_err(|error| errors::invalid("domain", &error.to_string()))?;
        Ok(self.held()?.domain_clock(&domain).map(DomainClock::new))
    }
    fn suggest(&self, args: &[Value]) -> Result<Suggestions, Error> {
        let args =
            scan_args::<(String, usize, u16), (Option<Option<String>>,), (), (), RHash, ()>(args)?;
        let (input, cursor, page_size) = args.required;
        let continuation = args.optional.0.flatten();
        let token = Cancel::keyword(args.keywords)?;
        let session = self.held()?;
        let page = gvl::without_gvl(token, |cancel| {
            session.suggest_page(
                &input,
                cursor,
                page_size,
                continuation.as_deref(),
                Some(cancel),
            )
        })?;
        Ok(Suggestions {
            page: host::Suggestions::new(page),
        })
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Outcome", free_immediately)]
struct Outcome {
    outcome: host::Outcome,
}

impl Outcome {
    fn disposition(&self) -> i32 {
        enums::Disposition::code(self.outcome.disposition())
    }
    fn message(&self) -> &str {
        &self.outcome.command().message
    }
    fn execution_reference(&self) -> Option<&str> {
        Some(
            self.outcome
                .command()
                .execution_reference
                .as_ref()?
                .as_str(),
        )
    }
    fn upload_failure(&self) -> Option<i32> {
        self.outcome
            .upload_failure()
            .map(enums::UploadFailure::code)
    }
    fn diagnostics(&self) -> RArray {
        let ruby = Ruby::get().assured("a native Ruby method holds the GVL");
        ruby.ary_from_iter(
            self.outcome
                .command()
                .diagnostics
                .iter()
                .map(|diagnostic| Diagnostic {
                    message: diagnostic.message.clone(),
                    span: diagnostic.span,
                }),
        )
    }
    fn subscription(&self) -> Option<Subscription> {
        Some(Subscription::new(
            &self.outcome.command().subscription.as_ref()?.subscription,
        ))
    }
    fn schema(&self) -> Option<Schema> {
        Some(Schema::new(self.outcome.opened_schema()?.clone()))
    }
    fn backup(&self) -> Option<Backup> {
        let backup = self.outcome.command().backup.as_ref()?;
        Some(Backup {
            total_bytes: backup.total_bytes.get(),
            digest: *backup.digest.as_bytes(),
        })
    }
    fn restore(&self) -> Option<Restore> {
        Some(Restore {
            summary: self.outcome.restore_summary()?,
        })
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Diagnostic", free_immediately)]
struct Diagnostic {
    message: String,
    span: Option<SourceSpan>,
}
impl Diagnostic {
    fn message(&self) -> &str {
        &self.message
    }
    fn span(&self) -> Option<(u32, u32)> {
        let span = self.span?;
        Some((span.start(), span.end()))
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Backup", free_immediately)]
struct Backup {
    total_bytes: u64,
    digest: [u8; 32],
}
impl Backup {
    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
    fn digest(&self) -> RString {
        Ruby::get()
            .assured("a native Ruby method holds the GVL")
            .str_from_slice(&self.digest)
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Restore", free_immediately)]
struct Restore {
    summary: host::RestoreSummary,
}
impl Restore {
    fn dry_run(&self) -> bool {
        self.summary.dry_run
    }
    fn domains(&self) -> u64 {
        self.summary.domains
    }
    fn resource_versions(&self) -> u64 {
        self.summary.resource_versions
    }
    fn models(&self) -> u64 {
        self.summary.models
    }
    fn failed(&self) -> bool {
        self.summary.failed
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Suggestions", free_immediately)]
struct Suggestions {
    page: host::Suggestions,
}
impl Suggestions {
    fn status(&self) -> i32 {
        enums::CompletionStatus::code(self.page.outcome().status.into())
    }
    fn continuation(&self) -> Option<&str> {
        self.page.outcome().continuation.as_deref()
    }
    fn suggestions(&self) -> RArray {
        let ruby = Ruby::get().assured("a native Ruby method holds the GVL");
        ruby.ary_from_iter(
            self.page
                .outcome()
                .suggestions
                .iter()
                .map(|item| Suggestion {
                    value: item.value.clone(),
                    kind: item.kind.into(),
                    start: item.edit.start,
                    end: item.edit.end,
                    replacement: item.edit.replacement.clone(),
                }),
        )
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Suggestion", free_immediately)]
struct Suggestion {
    value: String,
    kind: host::CompletionKind,
    start: u32,
    end: u32,
    replacement: String,
}
impl Suggestion {
    fn value(&self) -> &str {
        &self.value
    }
    fn kind(&self) -> i32 {
        enums::CompletionKind::code(self.kind)
    }
    fn span(&self) -> (u32, u32) {
        (self.start, self.end)
    }
    fn replacement(&self) -> &str {
        &self.replacement
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    Cancel::register(ruby, module)?;
    Execution::register(ruby, module)?;
    Session::register(ruby, module)?;
    Outcome::register(ruby, module)?;
    Diagnostic::register(ruby, module)?;
    Backup::register(ruby, module)?;
    Restore::register(ruby, module)?;
    Suggestions::register(ruby, module)?;
    Suggestion::register(ruby, module)?;
    Ok(())
}

impl Cancel {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let cancel = crate::class(ruby, module, "Cancel")?;
        cancel.define_singleton_method("new", function!(Cancel::new, -1))?;
        cancel.define_method("trigger", method!(Cancel::trigger, 0))?;
        Ok(())
    }
}

impl Execution {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let execution = crate::class(ruby, module, "Execution")?;
        execution.define_method("reference", method!(Execution::reference, 0))?;
        Ok(())
    }
}

impl Session {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let session = crate::class(ruby, module, "Session")?;
        session.define_singleton_method("connect", function!(Session::connect, -1))?;
        session.define_method("close", method!(Session::close, 0))?;
        session.define_method("prepare", method!(Session::prepare, -1))?;
        session.define_method("execute", method!(Session::execute, -1))?;
        session.define_method("next_event", method!(Session::next_event, -1))?;
        session.define_method("next_clock_event", method!(Session::next_clock_event, -1))?;
        session.define_method("domain_clock", method!(Session::domain_clock, 1))?;
        session.define_method("suggest", method!(Session::suggest, -1))?;
        Ok(())
    }
}

impl Outcome {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let outcome = crate::class(ruby, module, "Outcome")?;
        outcome.define_method("disposition", method!(Outcome::disposition, 0))?;
        outcome.define_method("message", method!(Outcome::message, 0))?;
        outcome.define_method(
            "execution_reference",
            method!(Outcome::execution_reference, 0),
        )?;
        outcome.define_method("upload_failure", method!(Outcome::upload_failure, 0))?;
        outcome.define_method("diagnostics", method!(Outcome::diagnostics, 0))?;
        outcome.define_method("subscription", method!(Outcome::subscription, 0))?;
        outcome.define_method("schema", method!(Outcome::schema, 0))?;
        outcome.define_method("backup", method!(Outcome::backup, 0))?;
        outcome.define_method("restore", method!(Outcome::restore, 0))?;
        Ok(())
    }
}

impl Diagnostic {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let diagnostic = crate::class(ruby, module, "Diagnostic")?;
        diagnostic.define_method("message", method!(Diagnostic::message, 0))?;
        diagnostic.define_method("span", method!(Diagnostic::span, 0))?;
        Ok(())
    }
}

impl Backup {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let backup = crate::class(ruby, module, "Backup")?;
        backup.define_method("total_bytes", method!(Backup::total_bytes, 0))?;
        backup.define_method("digest", method!(Backup::digest, 0))?;
        Ok(())
    }
}

impl Restore {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let restore = crate::class(ruby, module, "Restore")?;
        restore.define_method("dry_run", method!(Restore::dry_run, 0))?;
        restore.define_method("domains", method!(Restore::domains, 0))?;
        restore.define_method("resource_versions", method!(Restore::resource_versions, 0))?;
        restore.define_method("models", method!(Restore::models, 0))?;
        restore.define_method("failed", method!(Restore::failed, 0))?;
        Ok(())
    }
}

impl Suggestions {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let suggestions = crate::class(ruby, module, "Suggestions")?;
        suggestions.define_method("status", method!(Suggestions::status, 0))?;
        suggestions.define_method("suggestions", method!(Suggestions::suggestions, 0))?;
        suggestions.define_method("continuation", method!(Suggestions::continuation, 0))?;
        Ok(())
    }
}

impl Suggestion {
    fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
        let suggestion = crate::class(ruby, module, "Suggestion")?;
        suggestion.define_method("value", method!(Suggestion::value, 0))?;
        suggestion.define_method("kind", method!(Suggestion::kind, 0))?;
        suggestion.define_method("span", method!(Suggestion::span, 0))?;
        suggestion.define_method("replacement", method!(Suggestion::replacement, 0))?;
        Ok(())
    }
}
