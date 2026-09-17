//! Unsolicited server messages about the server and the session itself: notices, leadership and
//! the end of the session.

use error_stack::Report;
use nervix_models::ClusterNodeName;

use crate::{
    codec::{DecodeError, Decoder, EncodeError, EncodedUnion, Encoder, wire_enum},
    common::{LeaderEndpoints, LeaderRedirect},
    frame::{EncodedFrame, ServerFrame},
    limits::SessionLimits,
    server::finish_server_message,
    wire,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NoticeLevel {
    Info,
    Warning,
    Error,
}

wire_enum!(ALL_NOTICE_LEVELS: NoticeLevel => wire::NoticeLevel { Info, Warning, Error });

/// A server condition worth an operator's attention that belongs to no request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerNotice {
    pub level: NoticeLevel,
    pub message: String,
}

impl ServerNotice {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let message = encoder.text("ServerNotice.message", &self.message)?;
        let notice = wire::ServerNotice::create(
            encoder.fbb(),
            &wire::ServerNoticeArgs {
                level: Some(self.level.into()),
                message: Some(message),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::ServerNotice, notice),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        notice: wire::ServerNotice<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let level = decoder.required_enumeration("ServerNotice.level", notice.level())?;
        let message = decoder.text("ServerNotice.message", notice.message())?;
        Ok(Self { level, message })
    }
}

/// Which node leads the cluster, as the serving node sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Leadership {
    /// The node serving this session leads.
    ServingNode(ClusterNodeName),
    /// Another node leads.
    Remote(LeaderEndpoints),
    /// No leader is known, for example during an election.
    Unknown,
}

/// Leadership as the serving node observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipObserved {
    pub leadership: Leadership,
}

impl LeadershipObserved {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let leadership = match &self.leadership {
            Leadership::ServingNode(node) => {
                let node = encoder.text("LeaderIsServingNode.node", node.as_str())?;
                let serving = wire::LeaderIsServingNode::create(
                    encoder.fbb(),
                    &wire::LeaderIsServingNodeArgs { node: Some(node) },
                );
                EncodedUnion::new(wire::Leadership::LeaderIsServingNode, serving)
            }
            Leadership::Remote(leader) => {
                let leader = leader.encode(&mut encoder)?;
                let remote = wire::LeaderIsRemote::create(
                    encoder.fbb(),
                    &wire::LeaderIsRemoteArgs {
                        leader: Some(leader),
                    },
                );
                EncodedUnion::new(wire::Leadership::LeaderIsRemote, remote)
            }
            Leadership::Unknown => EncodedUnion::new(
                wire::Leadership::LeaderUnknown,
                wire::LeaderUnknown::create(encoder.fbb(), &wire::LeaderUnknownArgs {}),
            ),
        };
        let observed = wire::LeadershipObserved::create(
            encoder.fbb(),
            &wire::LeadershipObservedArgs {
                leadership_type: leadership.discriminant,
                leadership: Some(leadership.value),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::LeadershipObserved, observed),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        observed: wire::LeadershipObserved<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        if let Some(serving) = observed.leadership_as_leader_is_serving_node() {
            let node = decoder.name("LeaderIsServingNode.node", serving.node())?;
            return Ok(Self {
                leadership: Leadership::ServingNode(node),
            });
        }
        if let Some(remote) = observed.leadership_as_leader_is_remote() {
            let leader = LeaderEndpoints::decode(decoder, remote.leader())?;
            return Ok(Self {
                leadership: Leadership::Remote(leader),
            });
        }
        match observed.leadership_type() {
            wire::Leadership::LeaderUnknown => Ok(Self {
                leadership: Leadership::Unknown,
            }),
            undeclared => Err(decoder.unknown_union("LeadershipObserved.leadership", undeclared.0)),
        }
    }
}

/// Why the server ends a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEndReason {
    ServerShuttingDown,
    /// The session needs the cluster leader, which is not the serving node.
    LeaderRedirect(LeaderRedirect),
    /// The client broke the protocol.
    ProtocolViolated {
        message: String,
    },
}

/// The server is ending the session. No reply follows for any request still in flight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEnding {
    pub reason: SessionEndReason,
}

impl SessionEnding {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let reason = match &self.reason {
            SessionEndReason::ServerShuttingDown => EncodedUnion::new(
                wire::SessionEndReason::ServerShuttingDown,
                wire::ServerShuttingDown::create(encoder.fbb(), &wire::ServerShuttingDownArgs {}),
            ),
            SessionEndReason::LeaderRedirect(redirect) => {
                let redirect = redirect.encode(&mut encoder)?;
                EncodedUnion::new(wire::SessionEndReason::LeaderRedirect, redirect)
            }
            SessionEndReason::ProtocolViolated { message } => {
                let message = encoder.text("ProtocolViolated.message", message)?;
                let violated = wire::ProtocolViolated::create(
                    encoder.fbb(),
                    &wire::ProtocolViolatedArgs {
                        message: Some(message),
                    },
                );
                EncodedUnion::new(wire::SessionEndReason::ProtocolViolated, violated)
            }
        };
        let ending = wire::SessionEnding::create(
            encoder.fbb(),
            &wire::SessionEndingArgs {
                reason_type: reason.discriminant,
                reason: Some(reason.value),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::SessionEnding, ending),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        ending: wire::SessionEnding<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        if let Some(redirect) = ending.reason_as_leader_redirect() {
            let redirect = LeaderRedirect::decode(decoder, redirect)?;
            return Ok(Self {
                reason: SessionEndReason::LeaderRedirect(redirect),
            });
        }
        if let Some(violated) = ending.reason_as_protocol_violated() {
            let message = decoder.text("ProtocolViolated.message", violated.message())?;
            return Ok(Self {
                reason: SessionEndReason::ProtocolViolated { message },
            });
        }
        match ending.reason_type() {
            wire::SessionEndReason::ServerShuttingDown => Ok(Self {
                reason: SessionEndReason::ServerShuttingDown,
            }),
            undeclared => Err(decoder.unknown_union("SessionEnding.reason", undeclared.0)),
        }
    }
}
