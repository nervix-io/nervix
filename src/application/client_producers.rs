//! Attaching application producers to the node that executes their client ingestor.
//!
//! Layer: edges.
//!
//! - **Owns.** Attaching a producer to a client ingestor this node executes, or through the one
//!   ordered link this node keeps to each other node to a client ingestor that node executes; one
//!   handle a session submits, closes and detaches through wherever the ingestor runs; and the
//!   events a forwarded producer receives, which mirror those of a local one.
//! - **Depends on.** The runtime's client ingestor endpoints, the interconnect's ordered duplex
//!   streams, and the client producer vocabulary.
//! - **Must not know.** Sessions, the session wire, NSPL, or how a batch is decoded or admitted.
//!
//! A local producer is attached to this node's endpoint directly. A forwarded one is attached by
//! the owning node on behalf of this one, which counts the producer's granted bytes against the
//! owning node's budget as well as this node's. Either way the session sees the same handle and
//! the same events: one outcome per batch, admission changes, and at most one end, after which the
//! events close. Losing a link ends every producer forwarded over it with `OwnerLost`, after its
//! batches the owning node cannot have admitted are answered as not admitted.

mod link;

use std::num::NonZeroU64;

use bytes::Bytes;
use nervix_interconnect::{HandlerRegistrationError, Transport};
use nervix_models::{
    ClientProducerDescription, ClientProducerLimits, ClientProducerRefusal, ClusterNodeName,
    DomainName, IngestorName, SchemaField,
};

use self::link::{ForwardedProducer, ProducerLinks};
use crate::runtime::{
    ClientProducerEvents, ClientProducerHandle, ClientProducerOpenRequest, ClientProducerRetention,
    ClientSubmissionId, OpenedClientProducer, Runtime,
};

/// What a session asks for when it opens a producer.
pub(in crate::application) struct ProducerOpen {
    pub(in crate::application) domain: DomainName,
    pub(in crate::application) ingestor: IngestorName,
    pub(in crate::application) expected_fields: Vec<SchemaField>,
    pub(in crate::application) limits: ClientProducerLimits,
    /// The largest batch one submission frame of the session carries.
    pub(in crate::application) max_batch_bytes: NonZeroU64,
}

/// An attached producer: what its open established, the route its batches take, and the events
/// that answer it.
pub(in crate::application) struct OpenedRoute {
    pub(in crate::application) description: ClientProducerDescription,
    pub(in crate::application) route: ProducerRoute,
    pub(in crate::application) events: ClientProducerEvents,
}

/// Where one producer's batches go. Dropping it detaches the producer without answering what it
/// still has outstanding.
pub(in crate::application) enum ProducerRoute {
    /// To this node's endpoint.
    Local(ClientProducerHandle),
    /// Over the link to the node that executes the ingestor.
    Forwarded(ForwardedProducer),
}

/// A route that took no batch: the link to the node that executes the ingestor already ended, so
/// the batch never left this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) struct RouteEnded;

impl ProducerRoute {
    /// Hands one batch to the endpoint, which answers it with exactly one outcome once it took the
    /// batch.
    pub(in crate::application) fn submit(
        &self,
        submission: ClientSubmissionId,
        batch: Bytes,
    ) -> Result<(), RouteEnded> {
        match self {
            Self::Local(handle) => {
                handle.submit(submission, batch);
                Ok(())
            }
            Self::Forwarded(forwarded) => forwarded.submit(submission, batch),
        }
    }

    /// Stops admission. The events close once every batch has its outcome.
    pub(in crate::application) fn close(self) {
        match self {
            Self::Local(handle) => handle.close(),
            Self::Forwarded(forwarded) => forwarded.close(),
        }
    }
}

/// This node's producer attachments to client ingestors, local and forwarded. Cloning it shares
/// the links.
#[derive(Clone)]
pub(in crate::application) struct ClientProducerRouter {
    runtime: Runtime,
    local_node: ClusterNodeName,
    links: ProducerLinks,
}

impl ClientProducerRouter {
    pub(in crate::application) fn new(
        runtime: Runtime,
        interconnect: Transport,
        local_node: ClusterNodeName,
    ) -> Self {
        let links = ProducerLinks::new(interconnect, local_node.clone());
        Self {
            runtime,
            local_node,
            links,
        }
    }

    /// Serves the links other nodes open to attach producers to the client ingestors this node
    /// executes.
    pub(in crate::application) fn serve_links(
        &self,
        interconnect: &Transport,
    ) -> error_stack::Result<(), HandlerRegistrationError> {
        link::serve_owner_links(interconnect, self.runtime.clone())
    }

    /// Attaches a producer to the client ingestor `owner` executes.
    pub(in crate::application) async fn open(
        &self,
        owner: &ClusterNodeName,
        open: ProducerOpen,
    ) -> Result<OpenedRoute, ClientProducerRefusal> {
        if *owner != self.local_node {
            return self.links.open(owner, open).await;
        }
        let ProducerOpen {
            domain,
            ingestor,
            expected_fields,
            limits,
            max_batch_bytes,
        } = open;
        let request = ClientProducerOpenRequest {
            domain,
            ingestor,
            expected_fields,
            limits,
            max_batch_bytes,
            retention: ClientProducerRetention::Local,
        };
        let OpenedClientProducer {
            description,
            handle,
            events,
        } = self.runtime.open_client_producer(request).await?;
        Ok(OpenedRoute {
            description,
            route: ProducerRoute::Local(handle),
            events,
        })
    }
}
