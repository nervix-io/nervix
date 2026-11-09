# frozen_string_literal: true

module RubySuite
  class SubscriptionsTest < ClusterTest
    def publish(id, tenant = 'acme')
      writing = producer(session, 'watched_in')
      submission = writing.submit(build(writing.schema, id, tenant), cancel: RubySuite.deadline)
      outcome = writing.rejoin(submission, cancel: RubySuite.deadline)
      assert_equal NX::SubmissionResult::COMPLETED, outcome.result, outcome.message
      writing.close(cancel: RubySuite.deadline)
      @owned.delete(writing)
    end

    def test_rows_have_typed_values_bulk_columns_and_retained_frames
      subscription = unique('seen')
      # ANCHOR: subscribe
      opened = completed("CREATE SUBSCRIPTION #{subscription} TO watched_rows;")
      handle = opened.subscription
      assert_instance_of NX::Subscription, handle
      assert_equal subscription, handle.name
      assert_operator handle.generation, :>, 0
      assert_equal 'by_tenant', opened.schema.branch
      assert_equal ['tenant'], opened.schema.key_fields.map(&:name)
      # ANCHOR_END: subscribe
      publish(201)
      # ANCHOR: rows
      event = next_event
      assert_instance_of NX::Event, event
      assert_equal NX::EventKind::ROWS, event.kind
      assert_equal 1, event.row_count
      assert_equal handle.name, event.subscription.name
      assert_equal handle.generation, event.subscription.generation
      assert_equal [201], event.column(0)
      assert_equal [nil], event.column(5)
      assert_equal [NX::CellState::NULL], event.states(5)
      assert_equal [NX::REDACTED], event.column(10)
      assert_equal [NX::CellState::REDACTED], event.states(10)
      assert_equal ['acme'], event.column(0, NX::Part::BRANCH_KEY)
      # ANCHOR_END: rows
      # ANCHOR: row_buffers
      assert_equal [-(1 << 63)], event.fixed(2).unpack('q*')
      offsets, data = event.varlen(1, NX::Part::ROWS)
      assert_equal [[0, 4], 'acme'.b], [offsets, data]
      assert_equal 'id', event.schema.fields.first.name
      assert_nil event.restoration_failure
      frame = event.frame
      assert_instance_of IO::Buffer, frame
      assert frame.readonly?
      assert_equal 'NXSM', frame.get_string(4, 4)
      # ANCHOR_END: row_buffers
      # ANCHOR: event_lifetime
      slice = frame.slice(4, 4)
      weak = WeakRef.new(event)
      event = nil
      frame = nil
      GC.start(full_mark: true, immediate_sweep: true)
      GC.compact
      assert weak.weakref_alive?
      assert_equal 'NXSM', slice.get_string
      slice = nil
      assert_collected weak
      # ANCHOR_END: event_lifetime
      # ANCHOR: delete_subscription
      completed("DELETE SUBSCRIPTION #{subscription};")
      again = completed("CREATE SUBSCRIPTION #{subscription} TO watched_rows;").subscription
      refute_equal handle.generation, again.generation
      completed("DELETE SUBSCRIPTION #{subscription};")
      # ANCHOR_END: delete_subscription
    end

    def test_refused_restoration_is_observable_and_the_subscription_recovers
      relay = Relay.new(cluster.leader)
      watching = cluster.connect(server: relay.uri, seeds: [])
      subscription = unique('restored')
      armed = false
      begin
        completed("CREATE SUBSCRIPTION #{subscription} TO watched_rows;", watching)
        publish(211)
        assert_equal [211], next_event(watching).column(0)
        relay.cut
        Actions.request('refuse-next-subscription-opening')
        armed = true
        relay.restore
        # ANCHOR: subscription_restoration
        interrupted = next_event(watching)
        assert_equal NX::EventKind::INTERRUPTED, interrupted.kind
        refused = next_event(watching)
        assert_equal NX::EventKind::RESTORATION_FAILED, refused.kind
        failure = refused.restoration_failure
        assert_instance_of NX::RestorationFailure, failure
        assert_equal NX::RestorationCause::UNAVAILABLE, failure.cause
        refute_empty failure.message
        assert_operator failure.retry_after_nanos, :>, 0
        # Keep the subscription and let a later opening succeed.
        after = nil
        RubySuite.wait_until('the restored subscription delivered no rows') do
          publish(212)
          begin
            candidate = watching.next_event(cancel: NX::Cancel.new(deadline: 2))
          rescue NX::DeadlineError
            next false
          end
          if candidate.kind == NX::EventKind::RESTORATION_FAILED
            assert_equal NX::RestorationCause::UNAVAILABLE, candidate.restoration_failure.cause
            next false
          end
          after = candidate
          true
        end
        assert_equal NX::EventKind::ROWS, after.kind
        assert_equal [212], after.column(0)
        assert_equal subscription, after.subscription.name
        # ANCHOR_END: subscription_restoration
        completed("DELETE SUBSCRIPTION #{subscription};", watching)
      ensure
        Actions.request('clear-subscription-opening-refusals') if armed
        relay.restore
        watching.close
        relay.close
      end
    end

    def test_dropping_a_relay_ends_its_subscription
      relay = unique('doomed')
      subscription = unique('ended')
      completed("CREATE RELAY #{relay} SCHEMA suite_row BRANCHED BY by_tenant;")
      completed("CREATE SUBSCRIPTION #{subscription} TO #{relay};")
      completed("DROP RELAY #{relay};")
      # ANCHOR: subscription_ended
      event = next_event
      assert_instance_of NX::Event, event
      assert_equal NX::EventKind::ENDED, event.kind
      assert_equal NX::EndReason::RELAY_REMOVED, event.end_reason
      assert_equal subscription, event.subscription.name
      assert_nil event.restoration_failure
      # The event carries no rows or frame.
      assert_raises(NX::TypeMismatchError) { event.schema }
      assert_raises(NX::TypeMismatchError) { event.frame }
      # ANCHOR_END: subscription_ended
    end
  end
end
