//! Current restore reply fixtures shared by real gRPC owner and public-client tests.
//!
//! Layer: test harness.
//! - **Owns.** Raw gRPC encoding and a verified reply whose present request identity is zero.
//! - **Depends on.** The current session schema, FlatBuffers, tonic and the wire verifier.
//! - **Must not know.** Client recovery, server admission or restore execution.

use bytes::BufMut as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    FrameRoot, RestoreReply, RestoreReplyFrame, SessionLimits, VerifiedFrame, grpc::FrameCodec,
};
use tonic::{
    Status,
    codec::{Codec, EncodeBuf, Encoder},
};

/// Which validation a current restore reply fails.
#[derive(Debug, Clone, Copy)]
pub enum ReplyFault {
    Undecodable,
    AnotherRequest,
}

impl ReplyFault {
    pub fn substitute(self, reply: RestoreReply) -> Vec<u8> {
        match self {
            Self::AnotherRequest => {
                let mut reply = reply;
                reply.request_id = Some(nervix_client_wire::RequestId::new(
                    std::num::NonZeroU64::new(2).assured("the fixture identity is non-zero"),
                ));
                reply
                    .encode(&SessionLimits::DEFAULT)
                    .assured("changing only a request identity preserves the reply's size")
                    .into_bytes()
                    .to_vec()
            }
            Self::Undecodable => undecodable_reply(),
        }
    }
}

/// Writes the current RestoreUploadFailed and RestoreReply tables, deliberately setting the
/// optional request_id to a present zero. FlatBuffers verifies the tables; semantic decoding
/// refuses the identity before it can interpret the disposition.
fn undecodable_reply() -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let message = builder.create_string("the scripted peer received the complete restore");
    let failed = builder.start_table();
    // RestoreUploadFailed.failure = InvalidStream (0), message = slot 6.
    builder.push_slot_always::<u8>(4, 0);
    builder.push_slot_always(6, message);
    let failed = builder.end_table(failed);
    let reply = builder.start_table();
    builder.push_slot_always::<u64>(4, 0);
    // RestoreDisposition::RestoreUploadFailed is the current union's second variant.
    builder.push_slot_always::<u8>(6, 2);
    builder.push_slot_always(8, failed.as_union_value());
    let reply = builder.end_table(reply);
    builder.finish(reply, Some("NXRR"));
    let bytes = builder.finished_data().to_vec();
    let verified =
        VerifiedFrame::<RestoreReplyFrame>::verify(bytes.clone().into(), &SessionLimits::DEFAULT)
            .assured("the current malformed semantic fixture passes structural verification");
    assert!(RestoreReply::decode(&verified).is_err());
    bytes
}

/// The normal wire decoder with a raw-byte encoder for a scripted peer's exact current frame.
pub struct RawCodec<Inbound: FrameRoot> {
    inner: FrameCodec<Inbound, Inbound>,
}

impl<Inbound: FrameRoot> Default for RawCodec<Inbound> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Inbound: FrameRoot> RawCodec<Inbound> {
    pub fn new() -> Self {
        Self {
            inner: FrameCodec::new(SessionLimits::DEFAULT),
        }
    }
}

impl<Inbound: FrameRoot> Codec for RawCodec<Inbound> {
    type Encode = Vec<u8>;
    type Decode = VerifiedFrame<Inbound>;
    type Encoder = RawEncoder;
    type Decoder = <FrameCodec<Inbound, Inbound> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        self.inner.decoder()
    }
}

pub struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn encode(&mut self, frame: Self::Item, output: &mut EncodeBuf<'_>) -> Result<(), Status> {
        output.put_slice(&frame);
        Ok(())
    }
}
