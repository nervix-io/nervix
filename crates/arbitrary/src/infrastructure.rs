//! The Models other entities stand on: relays, branches, virtual hosts, endpoints, signaling
//! protocols, lookups and placements.

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BranchEviction, CreateBranch, CreateEndpoint, CreateLookup, CreatePlacement, CreateRelay,
    CreateSignalingProtocol, CreateVhost, EndpointType, MaterializedRelayState, ModelName,
    PlacementPolicy, RelayBranching, RequestedResourceVersion, SignalingProtobufConfig,
    SignalingProtocolOnConnect, SignalingStep, SignalingWaitStep, SignalingWireFormat,
    VhostTlsResource,
};

use crate::Arbitrary;

/// The most items a generated list of hostnames, members, steps or programs holds.
const ITEMS: usize = 3;

/// The keywords that begin an `ALTER` operation. Inside an `ALTER`, a comma before one of them
/// written bare begins the next operation.
const ALTER_OPERATION_WORDS: [&str; 6] = ["add", "drop", "alter", "set", "replace", "rename"];

impl Arbitrary<'_> {
    /// A relay of either branching, with its capacity and optional materialized state.
    pub fn create_relay(&mut self) -> CreateRelay {
        let name = self.name();
        let schema = self.name();
        let capacity = self.entropy.boundary_biased(1..=u64::MAX);
        let capacity = usize::try_from(capacity).assured("supported targets address 64 bits");
        let branching = if self.entropy.flag() {
            RelayBranching::BranchedBy {
                branch: self.name(),
            }
        } else {
            RelayBranching::Unbranched
        };
        let materialized_state = if self.entropy.flag() {
            Some(MaterializedRelayState::LastByTimestamp)
        } else {
            None
        };
        CreateRelay {
            name,
            schema,
            buffer: NonZeroUsize::new(capacity).verified("the range above starts at one"),
            branching,
            materialized_state,
        }
    }

    /// A branch declaration, with or without an LRU bound on its live instances.
    pub fn create_branch(&mut self) -> CreateBranch {
        let name = self.name();
        let schema = self.name();
        let ttl = self.duration();
        let eviction = if self.entropy.flag() {
            Some(BranchEviction::Lru {
                max_instances: self.positive_u64(),
            })
        } else {
            None
        };
        CreateBranch {
            name,
            schema,
            ttl,
            eviction,
        }
    }

    /// A virtual host answering one or more hostnames, optionally with a TLS resource.
    pub fn create_vhost(&mut self) -> CreateVhost<RequestedResourceVersion> {
        let name = self.name();
        let count = self.entropy.positive_count(
            NonZeroUsize::new(ITEMS).assured("a vhost answers at least one hostname"),
        );
        let mut hostnames = Vec::with_capacity(count);
        for _ in 0..count {
            hostnames.push(self.hostname());
        }
        let tls = if self.entropy.flag() {
            Some(VhostTlsResource {
                resource: self.name(),
                version: self.requested_version(),
            })
        } else {
            None
        };
        CreateVhost {
            name,
            hostnames,
            tls,
        }
    }

    /// A hostname: dot-separated labels, each of identifiers and whole numbers joined by hyphens.
    pub fn hostname(&mut self) -> String {
        self.hostname_within(usize::MAX)
    }

    /// A hostname whose identifier parts are each at most `part_bytes` long.
    pub(crate) fn hostname_within(&mut self, part_bytes: usize) -> String {
        let labels = self.entropy.positive_count(
            NonZeroUsize::new(ITEMS).assured("a hostname holds at least one label"),
        );
        let mut hostname = String::new();
        for label in 0..labels {
            if label > 0 {
                hostname.push('.');
            }
            let parts = self
                .entropy
                .positive_count(NonZeroUsize::new(2).assured("a label holds at least one part"));
            for part in 0..parts {
                if part > 0 {
                    hostname.push('-');
                }
                if self.entropy.flag() {
                    let number = self.entropy.boundary_biased(1..=u64::from(u16::MAX));
                    hostname.push_str(&number.to_string());
                } else {
                    let text = self.name_text();
                    let end = text.len().min(part_bytes);
                    hostname.push_str(&text[..end]);
                }
            }
        }
        hostname
    }

    /// An endpoint on a virtual host, of either type, optionally speaking a signaling protocol.
    pub fn create_endpoint(&mut self) -> CreateEndpoint {
        let name = self.name();
        let on_vhost = self.name();
        let path = format!("/{}", self.string());
        let endpoint_type = self
            .entropy
            .pick([EndpointType::Websockets, EndpointType::Http]);
        let signaling_protocol = if endpoint_type == EndpointType::Websockets && self.entropy.flag()
        {
            Some(self.name())
        } else {
            None
        };
        CreateEndpoint {
            name,
            on_vhost,
            path,
            endpoint_type,
            signaling_protocol,
        }
    }

    /// A signaling protocol: its wire format and the handshake it runs when a connection opens.
    pub fn create_signaling_protocol(
        &mut self,
    ) -> CreateSignalingProtocol<RequestedResourceVersion> {
        let name = self.name();
        let format = match self.entropy.byte() % 7 {
            0 => SignalingWireFormat::Json,
            1 => SignalingWireFormat::Yaml,
            2 => SignalingWireFormat::Toml,
            3 => SignalingWireFormat::Xml,
            4 => SignalingWireFormat::Cbor,
            5 => SignalingWireFormat::Raw,
            _ => SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                resource: self.name(),
                resource_version: self.requested_version(),
                config: self.config_entries(),
                send_message: self.string(),
                wait_message: self.string(),
            }),
        };
        let accept_data = self.entropy.flag();
        let count = self.entropy.positive_count(
            NonZeroUsize::new(ITEMS).assured("a handshake takes at least one step"),
        );
        let mut steps = Vec::with_capacity(count);
        for _ in 0..count {
            let step = if self.entropy.flag() {
                SignalingStep::Send(self.programs(1))
            } else {
                let matchers = self.programs(1);
                let capture = if matchers.len() == 1 && self.entropy.flag() {
                    Some(self.string())
                } else {
                    None
                };
                SignalingStep::Wait(SignalingWaitStep {
                    matchers,
                    capture,
                    fail_matchers: self.programs(0),
                    accept_data: self.entropy.flag(),
                })
            };
            steps.push(step);
        }
        let fail_matchers = self.programs(0);
        let timeout = self.duration();
        CreateSignalingProtocol {
            name,
            format,
            on_connect: SignalingProtocolOnConnect {
                accept_data,
                steps,
                fail_matchers,
                timeout,
            },
        }
    }

    /// At least `minimum` JAQ programs, each any string.
    fn programs(&mut self, minimum: usize) -> Vec<String> {
        let extra = ITEMS
            .checked_sub(minimum)
            .assured("a program list's minimum is within its bound");
        let count = minimum
            .checked_add(self.entropy.count(extra))
            .verified("the extra count is at most the bound minus the minimum");
        let mut programs = Vec::with_capacity(count);
        for _ in 0..count {
            programs.push(self.string());
        }
        programs
    }

    /// A lookup keyed by one field of the records a resource version holds.
    pub fn create_lookup(&mut self) -> CreateLookup<RequestedResourceVersion> {
        CreateLookup {
            name: self.name(),
            key_field: self.name(),
            resource: self.name(),
            resource_version: self.requested_version(),
            path: self.string(),
            decode_using_codec: self.name(),
        }
    }

    /// A placement between two non-empty groups of runtime nodes, each member named once.
    ///
    /// `ALTER PLACEMENT` sets the same groups, and reads a comma before a bare operation keyword as
    /// the next operation, so in the NSPL domain no member is named like one.
    pub fn create_placement(&mut self) -> CreatePlacement {
        let name = self.name();
        let from = self.distinct_names_refusing::<ModelName>(1, ITEMS, &ALTER_OPERATION_WORDS);
        let to = self.distinct_names_refusing::<ModelName>(1, ITEMS, &ALTER_OPERATION_WORDS);
        let policy = self.entropy.pick([
            PlacementPolicy::RequireColocation,
            PlacementPolicy::PreferColocation,
            PlacementPolicy::Neutral,
            PlacementPolicy::SuggestSeparation,
        ]);
        let rank = if self.entropy.flag() {
            Some(self.positive_u64())
        } else {
            None
        };
        CreatePlacement::new(name, from, to, policy, rank)
            .verified("both member lists are non-empty and hold each member once")
    }
}
