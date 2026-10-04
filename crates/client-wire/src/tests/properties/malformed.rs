//! Hostile bytes use the production verifier and decoders under small fixed allocation budgets.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::FlatBufferBuilder;
use meticulous::ResultExt as _;

use crate::{
    tests::fixtures::{checked, finish_raw, settings, size},
    *,
};

fn limits() -> SessionLimits {
    let mut settings = settings();
    settings.frame_bytes = size(16 * 1024);
    settings.transfer_bytes = size(32 * 1024);
    settings.nesting_depth = size(16);
    settings.collection_entries = size(64);
    settings.string_bytes = size(2048);
    checked(settings)
}

fn decode<R: FrameRoot, T>(
    bytes: Bytes,
    read: fn(&VerifiedFrame<R>) -> Result<T, Report<WireDecodeError>>,
) {
    let limits = limits();
    match VerifiedFrame::<R>::verify(bytes, &limits) {
        Ok(frame) => {
            let detached = frame.detached();
            assert_eq!(detached.bytes(), frame.bytes());
            drop(frame);
            // Error outcomes are expected in this target only. Both paths must stay bounded and
            // deterministic; an accepted frame is always actually decoded, including Rows.
            match read(&detached) {
                Ok(_) => assert!(read(&detached).is_ok()),
                Err(error) => {
                    let repeated = match read(&detached) {
                        Err(error) => error,
                        Ok(_) => panic!("decoding changed its outcome"),
                    };
                    assert_eq!(error.current_context(), repeated.current_context());
                }
            }
        }
        Err(error) => match error.current_context() {
            FrameError::TooLarge { actual, limit, .. } => assert!(*actual > *limit),
            FrameError::Truncated { actual, .. } => assert!(*actual < 8),
            FrameError::WrongIdentifier { .. } | FrameError::Invalid { .. } => {}
        },
    }
}

fn all_roots(bytes: &[u8]) {
    let bytes = Bytes::copy_from_slice(bytes);
    decode::<ClientFrame, _>(bytes.clone(), ClientMessage::decode);
    decode::<ServerFrame, _>(bytes.clone(), ServerMessage::decode);
    decode::<UploadFrame, _>(bytes.clone(), UploadMessage::decode);
    decode::<UploadReplyFrame, _>(bytes.clone(), UploadReply::decode);
    decode::<BackupDownloadRequestFrame, _>(bytes.clone(), BackupDownloadRequest::decode);
    decode::<BackupDownloadFrame, _>(bytes.clone(), BackupDownloadMessage::decode);
    decode::<RestoreFrame, _>(bytes.clone(), RestoreMessage::decode);
    decode::<RestoreReplyFrame, _>(bytes, RestoreReply::decode);
}

#[test]
fn bolero_arbitrary_frames_verify_and_decode_within_limits() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(16 * 1024 + 1)
        .for_each(|bytes: &[u8]| {
            all_roots(bytes);
        });
}

const CURRENT_FRAMES: &[&[u8]] = &[
    include_bytes!("../../../conformance/client_command.nxcm"),
    include_bytes!("../../../conformance/client_submit_batch.nxcm"),
    include_bytes!("../../../conformance/server_command_failed.nxsm"),
    include_bytes!("../../../conformance/server_rows.nxsm"),
    include_bytes!("../../../conformance/server_domain_clock_attached.nxsm"),
    include_bytes!("../../../conformance/backup_download_chunk.nxbd"),
    include_bytes!("../../../conformance/backup_download_request.nxbq"),
    include_bytes!("../../../conformance/restore_chunk.nxrm"),
    include_bytes!("../../../conformance/restore_outcome.nxrr"),
];

fn current_frame(index: usize) -> Vec<u8> {
    if let Some(frame) = CURRENT_FRAMES.get(index) {
        return frame.to_vec();
    }
    let limits = limits();
    if index == CURRENT_FRAMES.len() {
        UploadStart {
            request_id: crate::tests::fixtures::request(1),
            domain: crate::tests::fixtures::name("tenant"),
            resource: crate::tests::fixtures::name("resource"),
            upload_identity: nervix_models::ResourceUploadIdentity::parse("upload")
                .assured("a literal identity"),
            total_bytes: std::num::NonZeroU64::MIN,
        }
        .encode(&limits)
        .assured("a bounded current upload")
        .bytes()
        .to_vec()
    } else {
        UploadReply {
            request_id: Some(crate::tests::fixtures::request(1)),
            disposition: UploadDisposition::Installed {
                upload_identity: nervix_models::ResourceUploadIdentity::parse("upload")
                    .assured("a literal identity"),
                version: std::num::NonZeroU64::MIN,
                origin: OutcomeOrigin::Recovered,
            },
            message: "installed".to_owned(),
            diagnostics: Vec::new(),
        }
        .encode(&limits)
        .assured("a bounded current upload reply")
        .bytes()
        .to_vec()
    }
}

fn mutations(bytes: &[u8]) {
    let mut entropy = nervix_arbitrary::Entropy::new(bytes);
    let index = entropy.count(CURRENT_FRAMES.len() + 1);
    let mut frame = current_frame(index);
    match entropy.byte() % 5 {
        0 => {
            let len = entropy.count(frame.len());
            frame.truncate(len);
        }
        1 => {
            let offset = entropy.count(frame.len() - 1);
            frame[offset] ^= entropy.byte();
        }
        2 => {
            let offset = entropy.count(frame.len() - 4);
            frame[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        }
        3 => frame.resize(entropy.count(16 * 1024 + 1), entropy.byte()),
        _ => frame[..4].copy_from_slice(&u32::MAX.to_le_bytes()),
    }
    all_roots(&frame);
}

#[test]
fn bolero_current_frame_corruption_is_bounded() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            mutations(bytes);
        });
}

fn discriminators(bytes: &[u8]) {
    let mut entropy = nervix_arbitrary::Entropy::new(bytes);
    let tag = entropy.byte();
    let mut builder = FlatBufferBuilder::new();
    let request = wire::ListDomainsRequest::create(&mut builder, &wire::ListDomainsRequestArgs {});
    let root = wire::ClientMessage::create(
        &mut builder,
        &wire::ClientMessageArgs {
            request_id: 1,
            request_type: wire::ClientRequest(tag),
            request: Some(request.as_union_value()),
        },
    );
    let encoded = finish_raw(builder, root, ClientFrame::IDENTIFIER);
    let frame = VerifiedFrame::<ClientFrame>::verify(encoded.clone(), &limits());
    if tag > wire::ClientRequest::ENUM_MAX {
        let frame =
            frame.assured("an unknown tag is structurally verified without following its member");
        let error = ClientMessage::decode(&frame)
            .expect_err("an undeclared current discriminant is invalid");
        assert_eq!(
            error.current_context(),
            &WireDecodeError::UnknownUnionVariant {
                field: "ClientMessage.request",
                discriminant: tag
            }
        );
    } else {
        all_roots(&encoded);
    }
}

#[test]
fn bolero_discriminators_are_validated_before_use() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            discriminators(bytes);
        });
}

#[test]
fn malformed_boundaries_reach_limits_and_current_valid_frames() {
    for index in 0..CURRENT_FRAMES.len() + 2 {
        all_roots(&current_frame(index));
    }
    for len in [0, 1, 7, 8, 1023, 1024, 16383, 16384, 16385] {
        all_roots(&vec![0xff; len]);
    }
    for tag in 0..=u8::MAX {
        discriminators(&[tag]);
    }
    for seed in 0..32 {
        mutations(&[seed; 64]);
    }
}
