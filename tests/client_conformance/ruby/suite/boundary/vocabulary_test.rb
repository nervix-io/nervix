# frozen_string_literal: true

module RubySuite
  class VocabularyTest < Boundary
    def test_every_declared_enum_member_has_its_stable_value_and_integer_semantics
      expected = {
        NX::FailureKind => {
          NX::FailureKind::INVALID_ARGUMENT => 1,
          NX::FailureKind::CONNECT => 2,
          NX::FailureKind::TRANSPORT => 3,
          NX::FailureKind::UNCERTAIN => 4,
          NX::FailureKind::REJECTED => 5,
          NX::FailureKind::DEADLINE => 6,
          NX::FailureKind::CANCELLED => 7,
          NX::FailureKind::OVERFLOW => 8,
          NX::FailureKind::PROTOCOL => 9,
          NX::FailureKind::TYPE => 10,
          NX::FailureKind::CLOSED => 11,
          NX::FailureKind::INTERRUPTED => 12,
          NX::FailureKind::REOPEN_REQUIRED => 13,
        },
        NX::Disposition => {
          NX::Disposition::COMPLETED => 1,
          NX::Disposition::FAILED => 2,
          NX::Disposition::NOT_LEADER => 3,
          NX::Disposition::TRANSACTION_DETACHED => 4,
          NX::Disposition::TRANSACTION_TAKEN_OVER => 5,
          NX::Disposition::OUTCOME_UNKNOWN => 6,
          NX::Disposition::EXECUTION_REFERENCE_CONFLICT => 7,
          NX::Disposition::EXECUTION_REFERENCE_EXPIRED => 8,
          NX::Disposition::PREVIEW_STALE => 9,
        },
        NX::UploadFailure => {
          NX::UploadFailure::INVALID_STREAM => 1,
          NX::UploadFailure::RESOURCE_NOT_DECLARED => 2,
          NX::UploadFailure::SIZE_MISMATCH => 3,
          NX::UploadFailure::QUOTA_EXCEEDED => 4,
          NX::UploadFailure::INSTALLATION_FAILED => 5,
          NX::UploadFailure::IDENTITY_CONFLICT => 6,
        },
        NX::EventKind => {
          NX::EventKind::ROWS => 1,
          NX::EventKind::DELIVERY_LOST => 2,
          NX::EventKind::ROWS_SKIPPED => 3,
          NX::EventKind::ENDED => 4,
          NX::EventKind::INTERRUPTED => 5,
          NX::EventKind::CONSUMER_OVERFLOW => 6,
          NX::EventKind::RESTORATION_FAILED => 7,
        },
        NX::EndReason => {
          NX::EndReason::RELAY_REMOVED => 1,
          NX::EndReason::RELAY_CHANGED => 2,
        },
        NX::RestorationCause => {
          NX::RestorationCause::IN_TRANSACTION => 1,
          NX::RestorationCause::INVALID_STATEMENT => 2,
          NX::RestorationCause::NAME_IN_USE => 3,
          NX::RestorationCause::INVALID_FILTER => 4,
          NX::RestorationCause::UNAVAILABLE => 5,
          NX::RestorationCause::REJECTED => 6,
          NX::RestorationCause::FAILED => 7,
        },
        NX::CellState => {
          NX::CellState::VALUE => 1,
          NX::CellState::NULL => 2,
          NX::CellState::REDACTED => 3,
        },
        NX::FieldType => {
          NX::FieldType::U8 => 1,
          NX::FieldType::I8 => 2,
          NX::FieldType::U16 => 3,
          NX::FieldType::I16 => 4,
          NX::FieldType::U32 => 5,
          NX::FieldType::I32 => 6,
          NX::FieldType::U64 => 7,
          NX::FieldType::I64 => 8,
          NX::FieldType::F32 => 9,
          NX::FieldType::F64 => 10,
          NX::FieldType::BOOL => 11,
          NX::FieldType::STRING => 12,
          NX::FieldType::BYTES => 13,
          NX::FieldType::DATETIME => 14,
          NX::FieldType::FIXED_LIST => 15,
          NX::FieldType::LIST => 16,
        },
        NX::Part => {
          NX::Part::ROWS => 1,
          NX::Part::BRANCH_KEY => 2,
        },
        NX::ClockEventKind => {
          NX::ClockEventKind::STATE => 1,
          NX::ClockEventKind::TICK => 2,
          NX::ClockEventKind::ENDED => 3,
          NX::ClockEventKind::INTERRUPTED => 4,
          NX::ClockEventKind::RESTORATION_FAILED => 5,
        },
        NX::ClockState => {
          NX::ClockState::STOPPED => 1,
          NX::ClockState::UNINSTALLED => 2,
          NX::ClockState::UNPACED => 3,
          NX::ClockState::PACED => 4,
        },
        NX::ClockEndReason => {
          NX::ClockEndReason::DOMAIN_REMOVED => 1,
        },
        NX::EndpointState => {
          NX::EndpointState::ACTIVE => 1,
          NX::EndpointState::INTERRUPTED => 2,
          NX::EndpointState::RESTORING => 3,
          NX::EndpointState::REOPEN_REQUIRED => 4,
          NX::EndpointState::CLOSED => 5,
        },
        NX::OpenRefusal => {
          NX::OpenRefusal::DOMAIN_NOT_FOUND => 1,
          NX::OpenRefusal::DOMAIN_STOPPED => 2,
          NX::OpenRefusal::ENDPOINT_NOT_FOUND => 3,
          NX::OpenRefusal::NOT_CLIENT_ENDPOINT => 4,
          NX::OpenRefusal::ENDPOINT_UNAVAILABLE => 5,
          NX::OpenRefusal::SCHEMA_MISMATCH => 6,
          NX::OpenRefusal::TOO_MANY_ENDPOINTS => 7,
          NX::OpenRefusal::SESSION_CAPACITY_EXHAUSTED => 8,
          NX::OpenRefusal::NODE_CAPACITY_EXHAUSTED => 9,
          NX::OpenRefusal::INVALID_LIMITS => 10,
          NX::OpenRefusal::IN_TRANSACTION => 11,
        },
        NX::ReopenReason => {
          NX::ReopenReason::DOMAIN_STOPPED => 1,
          NX::ReopenReason::ENDPOINT_REMOVED => 2,
          NX::ReopenReason::SCHEMA_CHANGED => 3,
          NX::ReopenReason::CONTRACT_CHANGED => 4,
          NX::ReopenReason::GENERATION_CHANGED => 5,
          NX::ReopenReason::PROTOCOL_VIOLATED => 6,
          NX::ReopenReason::REFUSED => 7,
        },
        NX::WindowKind => {
          NX::WindowKind::SEQUENTIAL => 1,
          NX::WindowKind::PARALLEL => 2,
        },
        NX::Admission => {
          NX::Admission::OPEN => 1,
          NX::Admission::SUSPENDED => 2,
        },
        NX::Settlement => {
          NX::Settlement::CONFIRMED => 1,
          NX::Settlement::STALE_REFERENCE => 2,
          NX::Settlement::WRONG_CONSUMER => 3,
          NX::Settlement::INVALID_REASON => 4,
          NX::Settlement::CONSUMER_ENDED => 5,
        },
        NX::SubmissionResult => {
          NX::SubmissionResult::NOT_ADMITTED => 1,
          NX::SubmissionResult::COMPLETED => 2,
          NX::SubmissionResult::PROCESSING_FAILED => 3,
          NX::SubmissionResult::OUTCOME_UNKNOWN => 4,
        },
        NX::SubmissionRefusal => {
          NX::SubmissionRefusal::INVALID_BATCH => 1,
          NX::SubmissionRefusal::SUSPENDED => 2,
          NX::SubmissionRefusal::BUSY => 3,
          NX::SubmissionRefusal::DRAINING => 4,
          NX::SubmissionRefusal::PRODUCER_ENDED => 5,
          NX::SubmissionRefusal::CREDIT_EXCEEDED => 6,
        },
        NX::BatchDefect => {
          NX::BatchDefect::MALFORMED => 1,
          NX::BatchDefect::UNEXPECTED_MESSAGE => 2,
          NX::BatchDefect::COMPRESSED => 3,
          NX::BatchDefect::SCHEMA_MISMATCH => 4,
          NX::BatchDefect::NOT_ONE_BATCH => 5,
          NX::BatchDefect::TOO_MANY_ROWS => 6,
          NX::BatchDefect::TOO_LARGE => 7,
          NX::BatchDefect::INVALID_DATA => 8,
        },
        NX::ProcessingFailure => {
          NX::ProcessingFailure::ACK_TIMED_OUT => 1,
          NX::ProcessingFailure::REJECTED => 2,
        },
        NX::Uncertainty => {
          NX::Uncertainty::INTERRUPTED => 1,
          NX::Uncertainty::OWNER_LOST => 2,
          NX::Uncertainty::SESSION_LOST => 3,
        },
        NX::CompletionStatus => {
          NX::CompletionStatus::READY => 1,
          NX::CompletionStatus::MISSING_CONTEXT => 2,
          NX::CompletionStatus::STALE_CONTEXT => 3,
          NX::CompletionStatus::LOOKUP_FAILED => 4,
        },
        NX::CompletionKind => {
          NX::CompletionKind::TEXT => 1,
          NX::CompletionKind::LOCAL_DIRECTORY_LOOKUP => 2,
        },
        NX::Tls => { NX::Tls::PREFERRED => 1, NX::Tls::REQUIRED => 2 }
      }
      expected.each do |namespace, members|
        assert_instance_of Module, namespace
        assert_equal namespace.constants(false).length, members.length
        members.each do |actual, number|
          assert_equal number, actual
          assert_equal number.hash, actual.hash
          assert_equal number.inspect, actual.inspect
          assert_equal number, { actual => number }.fetch(actual)
        end
      end
      assert_equal :redacted, NX::REDACTED
      refute_empty NX::VERSION
      assert_equal RbConfig::CONFIG.fetch('ruby_version'), NX::RUBY_ABI
    end

    def test_every_exception_class_is_under_the_shared_base_class
      classes = [
        NX::InvalidArgumentError,
        NX::ConnectError,
        NX::TransportError,
        NX::UncertainOutcomeError,
        NX::RejectedError,
        NX::DeadlineError,
        NX::CancelledError,
        NX::EventOverflowError,
        NX::ProtocolError,
        NX::TypeMismatchError,
        NX::ClosedError,
        NX::ConsumerInterruptedError,
        NX::ReopenRequiredError,
      ]
      classes.each { |klass| assert_operator klass, :<, NX::ClientError }
    end
  end
end
