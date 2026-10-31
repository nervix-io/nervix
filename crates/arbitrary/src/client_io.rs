//! Native client producers and consumers: the limits they ask for, what an opened producer is
//! told, the outcomes of its batches, and the physical durations their policies carry.

use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClientAttachmentId, ClientConsumerLimits, ClientEndpointContract, ClientProducerDescription,
    ClientProducerGrant, ClientProducerLimits, ClientProducerPolicy, ClientSubmissionOutcome,
    ClientSubmissionRefusal, DEFAULT_CLIENT_BATCH_ROWS, DEFAULT_CLIENT_CONSUMER_SESSION_BYTES,
    DEFAULT_CLIENT_PRODUCER_SESSION_BYTES, MAX_CLIENT_CONSUMER_BATCHES,
    MAX_CLIENT_PRODUCER_BATCHES,
};
use strum::IntoEnumIterator;

use crate::Arbitrary;

/// The nanoseconds below one second, which is every subsecond part a duration holds.
const SUBSECOND_NANOS: u64 = 999_999_999;

impl Arbitrary<'_> {
    /// Any duration the standard library's `Duration` holds, landing on zero, one nanosecond and
    /// the longest duration as often as anywhere else.
    pub fn physical_duration(&mut self) -> Duration {
        match self.entropy.byte() % 5 {
            0 => Duration::ZERO,
            1 => Duration::from_nanos(1),
            2 => Duration::MAX,
            3 => Duration::from_secs(self.entropy.any_u64()),
            _ => {
                let seconds = self.entropy.any_u64();
                let nanos = self.entropy.boundary_biased(0..=SUBSECOND_NANOS);
                let nanos = u32::try_from(nanos).verified("a subsecond part is below one second");
                // A subsecond part below one second carries nothing into the seconds.
                Duration::new(seconds, nanos)
            }
        }
    }

    /// One variant of an enum that lists its own variants, each as likely as any other.
    pub fn variant<T: IntoEnumIterator>(&mut self) -> T {
        let count = NonZeroUsize::new(T::iter().len())
            .assured("a vocabulary enum a generator draws from has variants");
        let chosen = self.entropy.index(count);
        T::iter()
            .nth(chosen)
            .verified("the index is below the number of variants")
    }

    /// A positive count a `u32` holds, landing on `declared`, the bound the vocabulary declares
    /// for it, as often as on either end of the range.
    fn positive_u32_around(&mut self, declared: u32) -> NonZeroU32 {
        let value = match self.entropy.byte() % 4 {
            0 => u64::from(declared),
            _ => self.entropy.boundary_biased(1..=u64::from(u32::MAX)),
        };
        let value = u32::try_from(value).verified("both arms stay within a u32");
        NonZeroU32::new(value).assured("a declared bound and the range above are at least one")
    }

    /// A positive count a `u64` holds, landing on `declared`, the bound the vocabulary declares
    /// for it, as often as on either end of the range.
    fn positive_u64_around(&mut self, declared: u64) -> NonZeroU64 {
        let value = match self.entropy.byte() % 4 {
            0 => declared,
            _ => self.entropy.boundary_biased(1..=u64::MAX),
        };
        NonZeroU64::new(value).assured("a declared bound and the range above are at least one")
    }

    /// The window a producer asks for: any positive batch and byte counts, the most one producer
    /// may ask for included, and counts beyond it, which a frame carries and an open refuses.
    pub fn client_producer_limits(&mut self) -> ClientProducerLimits {
        ClientProducerLimits {
            batches: self.positive_u32_around(MAX_CLIENT_PRODUCER_BATCHES),
            bytes: self.positive_u64_around(DEFAULT_CLIENT_PRODUCER_SESSION_BYTES),
        }
    }

    /// The window a consumer asks for: any positive batch and byte counts, the most one consumer
    /// may ask for included, and counts beyond it, which a frame carries and an open refuses.
    pub fn client_consumer_limits(&mut self) -> ClientConsumerLimits {
        ClientConsumerLimits {
            batches: self.positive_u32_around(MAX_CLIENT_CONSUMER_BATCHES),
            bytes: self.positive_u64_around(DEFAULT_CLIENT_CONSUMER_SESSION_BYTES),
        }
    }

    /// What an opened producer was granted, with every count anywhere in its range.
    pub fn client_producer_grant(&mut self) -> ClientProducerGrant {
        ClientProducerGrant {
            batches: self.positive_u32_around(MAX_CLIENT_PRODUCER_BATCHES),
            bytes: self.positive_u64_around(DEFAULT_CLIENT_PRODUCER_SESSION_BYTES),
            max_batch_bytes: self.positive_u64_around(DEFAULT_CLIENT_PRODUCER_SESSION_BYTES),
            max_batch_rows: self.positive_u32_around(DEFAULT_CLIENT_BATCH_ROWS),
        }
    }

    /// The acknowledgement policy the producers of one client ingestor share.
    pub fn client_producer_policy(&mut self) -> ClientProducerPolicy {
        ClientProducerPolicy {
            window: self.ack_window(),
            ack_timeout: self.physical_duration(),
            retry_backoff: self.physical_duration(),
            retry_max_backoff: self.physical_duration(),
        }
    }

    /// The identity of one producer attachment: any 128 bits.
    pub fn client_attachment(&mut self) -> ClientAttachmentId {
        let high = u128::from(self.entropy.any_u64());
        let low = u128::from(self.entropy.any_u64());
        ClientAttachmentId::from_u128((high << 64) | low)
    }

    /// Everything an opened producer is told, with a schema of any declared field types.
    pub fn client_producer_description(&mut self) -> ClientProducerDescription {
        ClientProducerDescription {
            attachment: self.client_attachment(),
            fields: self.schema_fields(0),
            generation: self.entropy.any_u64(),
            contract: ClientEndpointContract::from_digest(self.digest()),
            policy: self.client_producer_policy(),
            grant: self.client_producer_grant(),
            admission: self.variant(),
        }
    }

    /// Why a batch was not admitted, with every defect an invalid batch names.
    pub fn client_submission_refusal(&mut self) -> ClientSubmissionRefusal {
        match self.entropy.byte() % 6 {
            0 => ClientSubmissionRefusal::Suspended,
            1 => ClientSubmissionRefusal::Busy,
            2 => ClientSubmissionRefusal::Draining,
            3 => ClientSubmissionRefusal::ProducerEnded,
            4 => ClientSubmissionRefusal::CreditExceeded,
            _ => ClientSubmissionRefusal::InvalidBatch(self.variant()),
        }
    }

    /// The terminal outcome of one submitted batch, with every cause each outcome names.
    pub fn client_submission_outcome(&mut self) -> ClientSubmissionOutcome {
        match self.entropy.byte() % 4 {
            0 => ClientSubmissionOutcome::Completed,
            1 => ClientSubmissionOutcome::NotAdmitted(self.client_submission_refusal()),
            2 => ClientSubmissionOutcome::ProcessingFailed(self.variant()),
            _ => ClientSubmissionOutcome::OutcomeUnknown(self.variant()),
        }
    }
}
