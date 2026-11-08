# frozen_string_literal: true

module RubySuite
  class LifetimesTest < ClusterTest
    def test_a_producer_outlives_its_ruby_session_and_keeps_native_runtime
      connected = cluster.connect
      writing = producer(connected, 'watched_in')
      execution = connected.prepare('LIST DOMAINS;')
      reference = execution.reference
      # ANCHOR: session_lifetime
      weak = WeakRef.new(connected)
      connected.close
      assert_raises(NX::ClosedError) { connected.prepare('LIST DOMAINS;') }
      connected = nil
      assert_collected weak
      assert_equal reference, execution.reference
      assert_equal NX::EndpointState::ACTIVE, writing.state
      submission = writing.submit(build(writing.schema, 301), cancel: RubySuite.deadline)
      assert_equal NX::SubmissionResult::COMPLETED, writing.rejoin(submission, cancel: RubySuite.deadline).result
      # ANCHOR_END: session_lifetime
    ensure
      connected&.close
    end

    def test_a_delivery_outlives_session_and_consumer_references
      writing = producer
      connected = cluster.connect
      reading = consumer(connected)
      submission = writing.submit(build(writing.schema, 302), cancel: RubySuite.deadline)
      delivery = reading.next(cancel: RubySuite.deadline)
      weak_session = WeakRef.new(connected)
      weak_consumer = WeakRef.new(reading)
      connected.close
      connected = nil
      @owned.delete(reading)
      reading = nil
      assert_collected weak_session
      assert_collected weak_consumer
      assert_equal [302], delivery.batch.column(0)
      # Collection closes the consumer's attachment, but retained delivery data remains valid.
      # A replacement consumer observes redelivery before we judge the expired reference.
      replacement = consumer
      redelivered = replacement.next(cancel: RubySuite.deadline)
      assert_equal delivery.identity, redelivered.identity
      assert_raises(NX::RejectedError) { delivery.ack(cancel: RubySuite.deadline) }
      assert_equal NX::Settlement::CONFIRMED, redelivered.ack(cancel: RubySuite.deadline)
      assert_equal NX::SubmissionResult::COMPLETED, writing.rejoin(submission, cancel: RubySuite.deadline).result
    ensure
      connected&.close
    end

    def test_an_endpoint_open_takes_the_complete_field_snapshot
      fields = input_fields
      token = NX::Cancel.new(deadline: 120)
      started = Queue.new
      worker = Thread.new do
        started << true
        session.open_ingestor(cluster.domain, 'watched_in', fields,
                              batches: 2, bytes: 1 << 20, cancel: token)
      end
      started.pop
      writing = worker.value
      @owned << writing
      fields.add('later', NX::FieldType::U8)
      assert_equal 'secret', writing.schema.fields.last.name
      assert_equal 'later', fields.schema.fields.last.name
      assert_equal 11, writing.schema.fields.length
    ensure
      token&.trigger
      worker&.join(WAIT)
    end

    def test_an_application_publishes_and_settles_its_output_with_ensure
      # ANCHOR: application
      connected = cluster.connect
      writing = nil
      reading = nil
      assert_instance_of NX::Session, connected
      begin
        reading = connected.subscribe_emitter(cluster.domain, 'typed_out', output_fields,
                                              batches: 4, bytes: 1 << 20, cancel: RubySuite.deadline)
        writing = connected.open_ingestor(cluster.domain, 'typed_in', input_fields,
                                           batches: 2, bytes: 1 << 20, cancel: RubySuite.deadline)
        submission = writing.submit(build(writing.schema, 303), cancel: RubySuite.deadline)
        delivery = reading.next(cancel: RubySuite.deadline)
        assert_equal [303], delivery.batch.column(11)
        assert_equal NX::Settlement::CONFIRMED, delivery.ack(cancel: RubySuite.deadline)
        assert_equal NX::SubmissionResult::COMPLETED, writing.rejoin(submission, cancel: RubySuite.deadline).result
      ensure
        writing&.close(cancel: RubySuite.deadline)
        reading&.close(cancel: RubySuite.deadline)
        connected.close
      end
      # ANCHOR_END: application
    end
  end
end
