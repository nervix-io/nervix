# frozen_string_literal: true

module RubySuite
  class EndpointsTest < ClusterTest
    def test_endpoint_descriptions_and_value_objects
      assert_instance_of NX::Session, session
      # ANCHOR: open_producer
      writing = producer
      assert_instance_of NX::Producer, writing
      assert_equal NX::EndpointState::ACTIVE, writing.state
      assert_equal NX::Admission::OPEN, writing.admission
      assert_operator writing.generation, :>=, 1
      assert_equal 32, writing.contract.bytesize
      assert_nil writing.reopen
      grant = writing.grant
      assert_instance_of NX::Grant, grant
      assert_equal 2, grant.batches
      assert_equal 1 << 20, grant.bytes
      assert_operator grant.max_batch_rows, :>, 0
      assert_operator grant.max_batch_bytes, :>, 0
      policy = writing.policy
      assert_instance_of NX::Policy, policy
      assert_equal NX::WindowKind::SEQUENTIAL, policy.window
      assert_equal 1, policy.outstanding
      assert_equal 60_000_000_000, policy.ack_timeout_nanos
      assert_equal 100_000_000, policy.retry_backoff_nanos
      assert_equal 1_000_000_000, policy.retry_max_backoff_nanos
      # ANCHOR_END: open_producer
      # ANCHOR: value_objects
      assert_equal grant, writing.grant
      assert grant == writing.grant
      refute grant == Object.new
      assert grant.eql?(writing.grant)
      assert_equal grant.hash, writing.grant.hash
      assert_equal policy, writing.policy
      assert policy == writing.policy
      refute policy == grant
      assert policy.eql?(writing.policy)
      assert_equal policy.hash, writing.policy.hash
      assert_includes grant.inspect, 'batches'
      assert_includes policy.inspect, 'ack_timeout'
      # ANCHOR_END: value_objects
      reading = consumer
      assert_instance_of NX::Consumer, reading
      assert_equal NX::EndpointState::ACTIVE, reading.state
      assert_operator reading.generation, :>=, 1
      assert_equal 32, reading.contract.bytesize
      assert_nil reading.reopen
      assert_equal 4, reading.grant.batches
      refute grant == reading.grant
      assert_equal 16, reading.grant.max_batch_rows
      assert_equal NX::WindowKind::SEQUENTIAL, reading.policy.window
      assert_equal 'echo', reading.schema.fields.last.name
    end

    def test_open_refusals_keep_their_typed_cause
      assert_instance_of NX::Session, session
      # ANCHOR: open_refusal
      failure = assert_raises(NX::RejectedError) do
        session.open_ingestor(cluster.domain, 'missing', input_fields,
                              batches: 2, bytes: 1 << 20, cancel: RubySuite.deadline)
      end
      assert_equal NX::FailureKind::REJECTED, failure.kind
      assert_equal NX::OpenRefusal::ENDPOINT_NOT_FOUND, failure.open_refusal
      refute_empty failure.message
      # ANCHOR_END: open_refusal
      assert_raises(NX::InvalidArgumentError) do
        session.open_ingestor(cluster.domain, 'typed_in', input_fields,
                              batches: 0, bytes: 1024, cancel: RubySuite.deadline)
      end
      failure = assert_raises(NX::RejectedError) do
        session.open_ingestor("#{cluster.domain}_missing", 'typed_in', input_fields,
                              batches: 2, bytes: 1024, cancel: RubySuite.deadline)
      end
      assert_equal NX::OpenRefusal::DOMAIN_NOT_FOUND, failure.open_refusal
      failure = assert_raises(NX::RejectedError) do
        session.open_ingestor(cluster.domain, 'typed_in', output_fields,
                              batches: 2, bytes: 1024, cancel: RubySuite.deadline)
      end
      assert_equal NX::OpenRefusal::SCHEMA_MISMATCH, failure.open_refusal
    end

    def test_built_batches_delivery_metadata_retry_and_ack
      reading = consumer
      writing = producer
      assert_instance_of NX::Consumer, reading
      assert_instance_of NX::Producer, writing
      # ANCHOR: submit
      submission = writing.submit(build(writing.schema, 101), cancel: RubySuite.deadline)
      pending = writing.pending.fetch(0)
      assert_instance_of NX::PendingSubmission, pending
      assert_equal submission, pending.id
      refute pending.resolved
      # ANCHOR_END: submit
      # ANCHOR: consume
      delivery = reading.next(cancel: RubySuite.deadline)
      assert_instance_of NX::Delivery, delivery
      assert_equal 16, delivery.identity.bytesize
      assert_equal 16, delivery.reference.bytesize
      assert_equal 'typed_rows', delivery.source_relay
      assert_equal 32, delivery.branch_fingerprint.bytesize
      assert_equal 1, delivery.members
      assert_operator delivery.execution_now, :>, 0
      batch = delivery.batch
      assert_equal [101], batch.column(0)
      assert_equal [101], batch.column(11)
      assert_equal [['a', 'é']], batch.column(8)
      assert_equal [[-32_768, 32_767]], batch.column(9)
      assert_equal ['secret'], batch.column(10)
      assert_equal delivery.ipc.get_string, batch.ipc.get_string
      # ANCHOR_END: consume
      # ANCHOR: retry
      assert_equal NX::Settlement::CONFIRMED, delivery.retry(cancel: RubySuite.deadline)
      retried = reading.next(cancel: RubySuite.deadline)
      assert_equal delivery.identity, retried.identity
      refute_equal delivery.reference, retried.reference
      assert_equal NX::Settlement::STALE_REFERENCE, delivery.ack(cancel: RubySuite.deadline)
      assert_equal NX::Settlement::CONFIRMED, retried.ack(cancel: RubySuite.deadline)
      # ANCHOR_END: retry
      # ANCHOR: rejoin
      outcome = writing.rejoin(submission, cancel: RubySuite.deadline)
      assert_instance_of NX::SubmissionOutcome, outcome
      assert_equal NX::SubmissionResult::COMPLETED, outcome.result
      assert_empty outcome.message
      assert_empty writing.pending
      # ANCHOR_END: rejoin
    end

    def test_ipc_submissions_rejections_and_every_outcome_cause
      reading = consumer
      writing = producer
      assert_instance_of NX::Consumer, reading
      assert_instance_of NX::Producer, writing
      # ANCHOR: submit_ipc
      bytes = build(writing.schema, 111).ipc.get_string
      submission = writing.submit_ipc(bytes, cancel: RubySuite.deadline)
      first = reading.next(cancel: RubySuite.deadline)
      assert_equal [111], first.batch.column(0)
      assert_equal NX::Settlement::CONFIRMED, first.ack(cancel: RubySuite.deadline)
      # ANCHOR_END: submit_ipc
      assert_equal NX::SubmissionResult::COMPLETED, writing.rejoin(submission, cancel: RubySuite.deadline).result
      # ANCHOR: reject
      rejected = writing.submit(build(writing.schema, 112), cancel: RubySuite.deadline)
      delivery = reading.next(cancel: RubySuite.deadline)
      assert_instance_of NX::Delivery, delivery
      assert_equal NX::Settlement::CONFIRMED, delivery.reject('the application refused this row', cancel: RubySuite.deadline)
      outcome = writing.rejoin(rejected, cancel: RubySuite.deadline)
      assert_instance_of NX::SubmissionOutcome, outcome
      assert_equal NX::SubmissionResult::PROCESSING_FAILED, outcome.result
      assert_equal NX::ProcessingFailure::REJECTED, outcome.failure
      # ANCHOR_END: reject
      assert_raises(NX::TypeMismatchError) { outcome.refusal }
      assert_raises(NX::TypeMismatchError) { outcome.uncertainty }
      # ANCHOR: refused_batch
      foreign = build(reading.schema, 113).ipc.get_string
      refused = writing.rejoin(writing.submit_ipc(foreign), cancel: RubySuite.deadline)
      assert_instance_of NX::SubmissionOutcome, refused
      assert_equal NX::SubmissionResult::NOT_ADMITTED, refused.result
      assert_equal NX::SubmissionRefusal::INVALID_BATCH, refused.refusal
      assert_equal NX::BatchDefect::SCHEMA_MISMATCH, refused.defect
      refute_empty refused.message
      # ANCHOR_END: refused_batch
      assert_raises(NX::TypeMismatchError) { refused.failure }
      assert_raises(NX::TypeMismatchError) { refused.uncertainty }
    end

    def test_pending_release_and_credit_waits
      reading = consumer
      writing = producer
      assert_instance_of NX::Producer, writing
      submission = writing.submit(build(writing.schema, 121), cancel: RubySuite.deadline)
      # ANCHOR: release
      assert_nil writing.release(submission)
      assert_empty writing.pending
      assert_raises(NX::InvalidArgumentError) { writing.rejoin(submission, cancel: RubySuite.deadline) }
      # ANCHOR_END: release
      reading.next(cancel: RubySuite.deadline).ack(cancel: RubySuite.deadline)
      known = writing.submit(build(writing.schema, 122), cancel: RubySuite.deadline)
      reading.next(cancel: RubySuite.deadline).ack(cancel: RubySuite.deadline)
      RubySuite.wait_until('the known outcome did not arrive') { writing.pending.all?(&:resolved) }
      assert_equal NX::SubmissionResult::COMPLETED, writing.release(known).result
      limited = session.open_ingestor(cluster.domain, 'typed_in', input_fields,
                                     batches: 1, bytes: 1 << 20, cancel: RubySuite.deadline)
      @owned << limited
      first = limited.submit(build(limited.schema, 123), cancel: RubySuite.deadline)
      assert_raises(NX::DeadlineError) do
        limited.submit(build(limited.schema, 124), cancel: NX::Cancel.new(deadline: 0.2))
      end
      reading.next(cancel: RubySuite.deadline).ack(cancel: RubySuite.deadline)
      assert_equal NX::SubmissionResult::COMPLETED, limited.rejoin(first, cancel: RubySuite.deadline).result
    end

    def test_a_session_gap_keeps_a_submission_unknown_and_restores_the_producer
      reading = consumer
      relay = Relay.new(cluster.leader)
      publishing = cluster.connect(server: relay.uri, seeds: [])
      writing = producer(publishing)
      assert_instance_of NX::Producer, writing
      begin
        submission = writing.submit(build(writing.schema, 131), cancel: RubySuite.deadline)
        held = reading.next(cancel: RubySuite.deadline)
        relay.cut
        # ANCHOR: outcome_unknown
        outcome = writing.rejoin(submission, cancel: RubySuite.deadline)
        assert_instance_of NX::SubmissionOutcome, outcome
        assert_equal NX::SubmissionResult::OUTCOME_UNKNOWN, outcome.result
        assert_equal NX::Uncertainty::SESSION_LOST, outcome.uncertainty
        # ANCHOR_END: outcome_unknown
        held.ack(cancel: RubySuite.deadline)
        relay.restore
        again = writing.submit(build(writing.schema, 132), cancel: RubySuite.deadline)
        delivered = reading.next(cancel: RubySuite.deadline)
        assert_equal [132], delivered.batch.column(0)
        delivered.ack(cancel: RubySuite.deadline)
        assert_equal NX::SubmissionResult::COMPLETED, writing.rejoin(again, cancel: RubySuite.deadline).result
        assert_equal NX::EndpointState::ACTIVE, writing.state
      ensure
        relay.restore
        writing.close(cancel: RubySuite.deadline)
        @owned.delete(writing)
        publishing.close
        relay.close
      end
    end

    def test_contract_changes_require_reopen
      assert_instance_of NX::Session, session
      ingestor = unique('changing')
      body = 'FROM SCHEMA suite_row MODE ACK PARALLEL MAX 4 ACK TIMEOUT %s ' \
             'RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND'
      completed("CREATE CLIENT PRODUCER INGESTOR #{ingestor} #{body % '60s'} TIMESTAMP NOW " \
                'TO watched_rows INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant ' \
                'FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;')
      writing = producer(session, ingestor)
      completed("ALTER CLIENT PRODUCER INGESTOR #{ingestor} SET #{body % '30s'};")
      RubySuite.wait_until('the contract alteration did not require reopening') do
        writing.state == NX::EndpointState::REOPEN_REQUIRED
      end
      # ANCHOR: reopen
      reopen = writing.reopen
      assert_instance_of NX::Reopen, reopen
      assert_equal NX::ReopenReason::CONTRACT_CHANGED, reopen.reason
      assert_nil reopen.refusal
      assert_raises(NX::ReopenRequiredError) { writing.submit(build(writing.schema), cancel: RubySuite.deadline) }
      replacement = producer(session, ingestor)
      assert_equal 30_000_000_000, replacement.policy.ack_timeout_nanos
      # ANCHOR_END: reopen
    end

    def test_endpoint_blocks_close_on_normal_return_and_exception
      assert_instance_of NX::Session, session
      # ANCHOR: endpoint_block
      closed = nil
      result = session.open_ingestor(cluster.domain, 'typed_in', input_fields,
                                     batches: 2, bytes: 1 << 20, cancel: RubySuite.deadline) do |handle|
        closed = handle
        assert_equal NX::EndpointState::ACTIVE, handle.state
        :written
      end
      assert_equal :written, result
      assert_equal NX::EndpointState::CLOSED, closed.state
      # ANCHOR_END: endpoint_block
      failure = Class.new(StandardError)
      assert_raises(failure) do
        session.subscribe_emitter(cluster.domain, 'typed_out', output_fields,
                                  batches: 4, bytes: 1 << 20, cancel: RubySuite.deadline) do |handle|
          closed = handle
          raise failure
        end
      end
      assert_equal NX::EndpointState::CLOSED, closed.state
      result = session.open_ingestor(cluster.domain, 'typed_in', input_fields,
                                     batches: 2, bytes: 1 << 20, cancel: RubySuite.deadline) do |handle|
        closed = handle
        break :broken
      end
      assert_equal :broken, result
      assert_equal NX::EndpointState::CLOSED, closed.state
    ensure
      closed&.close(cancel: RubySuite.deadline)
    end

    def test_a_consumer_gap_invalidates_delivery_references_and_recovers_identity
      writing = producer
      relay = Relay.new(cluster.leader)
      consuming = cluster.connect(server: relay.uri, seeds: [])
      reading = consumer(consuming)
      assert_instance_of NX::Consumer, reading
      begin
        submission = writing.submit(build(writing.schema, 141), cancel: RubySuite.deadline)
        held = reading.next(cancel: RubySuite.deadline)
        relay.cut
        # ANCHOR: consumer_interrupted
        assert_raises(NX::ConsumerInterruptedError) { reading.next(cancel: RubySuite.deadline) }
        assert_raises(NX::RejectedError) { held.ack(cancel: RubySuite.deadline) }
        relay.restore
        delivery = reading.next(cancel: RubySuite.deadline)
        assert_equal held.identity, delivery.identity
        assert_equal NX::Settlement::CONFIRMED, delivery.ack(cancel: RubySuite.deadline)
        # ANCHOR_END: consumer_interrupted
        assert_equal NX::SubmissionResult::COMPLETED, writing.rejoin(submission, cancel: RubySuite.deadline).result
        assert_equal NX::EndpointState::ACTIVE, reading.state
        assert_raises(NX::DeadlineError) { reading.next(cancel: NX::Cancel.new(deadline: 0.2)) }
      ensure
        relay.restore
        reading.close(cancel: RubySuite.deadline)
        @owned.delete(reading)
        consuming.close
        relay.close
      end
    end

    def test_suspended_admission_and_an_admitted_command_cancelled_from_another_thread
      ingestor = unique('draining')
      body = 'FROM SCHEMA suite_row MODE ACK PARALLEL MAX 4 ACK TIMEOUT %s ' \
             'RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND'
      completed("CREATE CLIENT PRODUCER INGESTOR #{ingestor} #{body % '60s'} TIMESTAMP NOW " \
                'TO typed_rows INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant ' \
                'FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;')
      reading = consumer
      writing = producer(session, ingestor)
      submission = writing.submit(build(writing.schema, 151), cancel: RubySuite.deadline)
      delivery = reading.next(cancel: RubySuite.deadline)
      execution = session.prepare("ALTER CLIENT PRODUCER INGESTOR #{ingestor} SET #{body % '30s'};")
      token = NX::Cancel.new(deadline: 120)
      worker = Thread.new { assert_raises(NX::CancelledError) { session.execute(execution, cancel: token) } }
      # ANCHOR: suspended_admission
      RubySuite.wait_until('the admitted alteration never suspended the producer') do
        writing.admission == NX::Admission::SUSPENDED
      end
      token.trigger
      assert worker.join(WAIT), 'the admitted command ignored cancellation'
      assert_equal execution.reference, worker.value.execution_reference
      # The server still drains the admitted batch. Cancellation ends this wait, not its effect.
      assert_equal NX::Settlement::CONFIRMED, delivery.ack(cancel: RubySuite.deadline)
      assert_equal NX::SubmissionResult::COMPLETED, writing.rejoin(submission, cancel: RubySuite.deadline).result
      recovered = session.execute(execution, cancel: RubySuite.deadline)
      assert_equal NX::Disposition::COMPLETED, recovered.disposition, recovered.message
      assert_equal execution.reference, recovered.execution_reference
      # ANCHOR_END: suspended_admission
    ensure
      token&.trigger
      delivery&.ack(cancel: RubySuite.deadline) if worker&.alive?
      worker&.join(WAIT)
    end
  end
end
