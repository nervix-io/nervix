//! Java array, string and scalar representations of the host vocabulary.
//!
//! Layer: edges.
//! - **Owns.** Checked JNI lengths, bulk copies and the Java encodings of host enums.
//! - **Depends on.** JNI and typed host descriptions.
//! - **Must not know.** Session state, retries or graph configuration.

use jni::{
    Env, jni_sig, jni_str,
    objects::{JLongArray, JObject, JObjectArray, JString, JValue},
    refs::Auto,
    sys::{jint, jlong},
};
use nervix_client_host as host;

use crate::error::Error;

pub(crate) fn size(value: jint, argument: &'static str) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| Error::argument(argument, "must not be negative"))
}

pub(crate) fn count(value: usize) -> Result<jint, Error> {
    jint::try_from(value).map_err(|_| Error::argument("length", "exceeds the JVM array range"))
}

pub(crate) fn part(value: jint) -> Result<host::Part, Error> {
    host::Part::from_repr(value)
        .ok_or_else(|| Error::argument("part", "does not name rows or the branch key"))
}

pub(crate) fn field_type(value: jint) -> Result<host::FieldType, Error> {
    host::FieldType::from_repr(value)
        .ok_or_else(|| Error::argument("type", "does not name a field type"))
}

pub(crate) fn optional_string(env: &Env<'_>, value: &JString<'_>) -> Result<Option<String>, Error> {
    if value.is_null() {
        Ok(None)
    } else {
        Ok(Some(value.try_to_string(env)?))
    }
}

pub(crate) fn string<'local>(
    env: &mut Env<'local>,
    value: Option<&str>,
) -> Result<JString<'local>, Error> {
    match value {
        Some(value) => Ok(env.new_string(value)?),
        None => Ok(JString::null()),
    }
}

pub(crate) fn longs<'local>(
    env: &mut Env<'local>,
    values: &[jlong],
) -> Result<JLongArray<'local>, Error> {
    count(values.len())?;
    let array = env.new_long_array(values.len())?;
    array.set_region(env, 0, values)?;
    Ok(array)
}

pub(crate) fn unsigned_longs<'local>(
    env: &mut Env<'local>,
    values: &[u64],
) -> Result<JLongArray<'local>, Error> {
    let signed: Vec<_> = values.iter().map(|value| value.cast_signed()).collect();
    longs(env, &signed)
}

pub(crate) fn offsets(env: &Env<'_>, values: &JLongArray<'_>) -> Result<Vec<u64>, Error> {
    let length = values.len(env)?;
    let mut signed = vec![0_i64; length];
    values.get_region(env, 0, &mut signed)?;
    Ok(signed.into_iter().map(jlong::cast_unsigned).collect())
}

/// The host's stable enum values are projected explicitly, so adding a variant requires a
/// Java projection instead of silently selecting a different discriminant.
macro_rules! codes {
    ($name:ident($ty:ty) { $($variant:ident = $value:literal),* $(,)? }) => {
        pub(crate) fn $name(value: $ty) -> jint {
            match value { $(<$ty>::$variant => $value,)* }
        }
    };
}

codes!(open_refusal(host::OpenRefusal) {
    DomainNotFound=1, DomainStopped=2, EndpointNotFound=3, NotClientEndpoint=4,
    EndpointUnavailable=5, SchemaMismatch=6, TooManyEndpoints=7, SessionCapacityExhausted=8,
    NodeCapacityExhausted=9, InvalidLimits=10, InTransaction=11,
});
codes!(field_kind(host::FieldType) {
    U8=1,I8=2,U16=3,I16=4,U32=5,I32=6,U64=7,I64=8,F32=9,F64=10,Bool=11,String=12,
    Bytes=13,Datetime=14,FixedList=15,List=16,
});
codes!(disposition(host::Disposition) {
    Completed=1,Failed=2,NotLeader=3,TransactionDetached=4,TransactionTakenOver=5,OutcomeUnknown=6,
    ExecutionReferenceConflict=7,ExecutionReferenceExpired=8,PreviewStale=9,
});
codes!(upload_failure(host::UploadFailure) {
    InvalidStream=1,ResourceNotDeclared=2,SizeMismatch=3,QuotaExceeded=4,
    InstallationFailed=5,IdentityConflict=6,
});
codes!(event_kind(host::EventKind) {
    Rows=1,DeliveryLost=2,RowsSkipped=3,Ended=4,Interrupted=5,ConsumerOverflow=6,RestorationFailed=7,
});
codes!(clock_kind(host::ClockEventKind) {State=1,Tick=2,Ended=3,Interrupted=4,RestorationFailed=5});
codes!(clock_state(host::ClockState) {Stopped=1,Uninstalled=2,Unpaced=3,Paced=4});
codes!(clock_end(host::ClockEndReason) {DomainRemoved=1});
codes!(endpoint_state(host::EndpointState) {Active=1,Interrupted=2,Restoring=3,ReopenRequired=4,Closed=5});
codes!(admission(host::Admission) {Open=1,Suspended=2});
codes!(window(host::WindowKind) {Sequential=1,Parallel=2});
codes!(settlement(host::Settlement) {Confirmed=1,StaleReference=2,WrongConsumer=3,InvalidReason=4,ConsumerEnded=5});
codes!(reopen_reason(host::ReopenReason) {
    DomainStopped=1,EndpointRemoved=2,SchemaChanged=3,ContractChanged=4,GenerationChanged=5,ProtocolViolated=6,Refused=7,
});
codes!(submission_result(host::SubmissionResult) {NotAdmitted=1,Completed=2,ProcessingFailed=3,OutcomeUnknown=4});

pub(crate) fn fields<'local>(
    env: &mut Env<'local>,
    schema: &host::Schema,
    part: host::Part,
) -> Result<JObjectArray<'local>, Error> {
    let fields = schema.fields(part);
    count(fields.len())?;
    let array = JObjectArray::<JObject>::new(env, fields.len(), JObject::null())?;
    for (index, field) in fields.iter().enumerate() {
        let name = Auto::new(env.new_string(field.name.as_str())?);
        let levels = host::TypeLevels::of(&field.ty);
        let mut kinds = Vec::with_capacity(levels.count());
        let mut lengths = Vec::with_capacity(levels.count());
        for index in 0..levels.count() {
            let ty = levels.level(index)?;
            kinds.push(field_kind(host::FieldType::from(ty)));
            let length = match ty {
                nervix_models::ParseAsType::Array { len, .. } => i64::from(len.get()),
                _ => 0,
            };
            lengths.push(length);
        }
        let java_kinds = Auto::new(env.new_int_array(kinds.len())?);
        java_kinds.set_region(env, 0, &kinds)?;
        let java_lengths = Auto::new(longs(env, &lengths)?);
        let value = Auto::new(env.new_object(
            jni_str!("io/nervix/client/Nervix$Field"),
            jni_sig!("(Ljava/lang/String;[I[JZZ)V"),
            &[
                JValue::Object(name.as_ref()),
                JValue::Object(java_kinds.as_ref()),
                JValue::Object(java_lengths.as_ref()),
                JValue::Bool(field.optional),
                JValue::Bool(field.sensitive),
            ],
        )?);
        array.set_element(env, index, value.as_ref())?;
    }
    Ok(array)
}

codes!(submission_refusal(host::SubmissionRefusal) {InvalidBatch=1,Suspended=2,Busy=3,Draining=4,ProducerEnded=5,CreditExceeded=6});
codes!(batch_defect(host::BatchDefect) {Malformed=1,UnexpectedMessage=2,Compressed=3,SchemaMismatch=4,NotOneBatch=5,TooManyRows=6,TooLarge=7,InvalidData=8});
codes!(processing_failure(host::ProcessingFailure) {AckTimedOut=1,Rejected=2});
codes!(uncertainty(host::Uncertainty) {Interrupted=1,OwnerLost=2,SessionLost=3});
codes!(completion_status(host::CompletionStatus) {Ready=1,MissingContext=2,StaleContext=3,LookupFailed=4});
codes!(completion_kind(host::CompletionKind) {Text=1,LocalDirectoryLookup=2});
