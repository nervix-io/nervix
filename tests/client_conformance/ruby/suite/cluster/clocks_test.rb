# frozen_string_literal: true

module RubySuite
  class ClocksTest < ClusterTest
    def test_attached_clock_projections_ticks_generations_and_detach
      domain = "#{cluster.domain}_#{unique('clock')}"
      completed("CREATE PACED DOMAIN #{domain} WITH PERIOD 100ms SKEW 10ms;")
      following = cluster.connect(domain: domain)
      assert_instance_of NX::Session, following
      begin
        completed("START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;", following)
        assert_nil following.domain_clock(domain)
        # ANCHOR: clock_attach
        completed('ATTACH DOMAIN CLOCK;', following)
        clock = following.domain_clock(domain)
        assert_instance_of NX::DomainClock, clock
        assert_equal domain, clock.domain
        assert_equal NX::ClockState::PACED, clock.state
        paced = clock.paced
        assert_instance_of NX::PacedClock, paced
        assert_equal clock.generation, paced.generation
        assert_equal 100_000_000, paced.period_nanos
        assert_equal 10_000_000, paced.skew_nanos
        assert_equal 1_893_456_000_000_000_000, paced.origin
        assert_equal 2.0, paced.rate
        # ANCHOR_END: clock_attach
        # ANCHOR: clock_project
        assert_equal paced.origin, clock.logical_time_at(paced.anchor)
        assert_equal 50_000_000, clock.wall_duration_until(paced.anchor, paced.origin + paced.period_nanos)
        assert_equal [paced.origin, paced.origin], clock.admission_window(paced.anchor)
        assert clock.admits(paced.anchor, paced.origin + paced.skew_nanos)
        refute clock.admits(paced.anchor, paced.origin + paced.skew_nanos + 1)
        # ANCHOR_END: clock_project
        # ANCHOR: clock_tick
        event = next_clock(following) { |value| value.kind == NX::ClockEventKind::TICK }
        assert_instance_of NX::ClockEvent, event
        assert_equal NX::ClockEventKind::TICK, event.kind
        assert_equal paced.generation, event.generation
        assert_equal domain, event.domain
        tick = event.tick
        assert_instance_of NX::Tick, tick
        assert_equal paced.generation, tick.generation
        assert_equal paced.origin + (tick.id - 1) * paced.period_nanos, tick.boundary
        assert_operator tick.serving_logical, :>=, paced.origin
        assert_operator tick.authority_utc, :>=, paced.anchor
        # ANCHOR_END: clock_tick
        assert_nil event.restoration_failure
        assert_raises(NX::TypeMismatchError) { event.state }
        assert_raises(NX::TypeMismatchError) { event.paced }
        assert_raises(NX::TypeMismatchError) { event.end_reason }
        held = following.domain_clock(domain)
        assert_instance_of NX::DomainClock, held
        assert_operator held.tick.id, :>=, tick.id
        completed('STOP;', following)
        completed("START AT '2031-01-01T00:00:00Z' TIME RATE 4.0;", following)
        # ANCHOR: clock_generations
        state = next_clock(following) do |value|
          value.kind == NX::ClockEventKind::STATE && value.state == NX::ClockState::PACED && value.generation == 2
        end
        assert_equal 1_924_992_000_000_000_000, state.paced.origin
        assert_equal 4.0, state.paced.rate
        completed('DETACH DOMAIN CLOCK;', following)
        assert_nil following.domain_clock(domain)
        # ANCHOR_END: clock_generations
      ensure
        following.close
      end
      assert_equal paced.origin, clock.logical_time_at(paced.anchor)
    end

    def test_clock_interruption_restores_attachment_and_detach_stops_following
      domain = "#{cluster.domain}_#{unique('clock')}"
      completed("CREATE PACED DOMAIN #{domain} WITH PERIOD 100ms SKEW 10ms;")
      relay = Relay.new(cluster.leader)
      following = cluster.connect(server: relay.uri, domain: domain, seeds: [])
      controller = cluster.connect(domain: domain)
      begin
        completed('START;', controller)
        completed('ATTACH DOMAIN CLOCK;', following)
        next_clock(following) { |event| event.kind == NX::ClockEventKind::TICK }
        relay.cut
        # ANCHOR: clock_interrupted
        interrupted = next_clock(following) { |event| event.kind == NX::ClockEventKind::INTERRUPTED }
        assert_equal domain, interrupted.domain
        assert_equal NX::ClockState::PACED, following.domain_clock(domain).state
        relay.restore
        next_clock(following) { |event| event.kind == NX::ClockEventKind::STATE }
        # ANCHOR_END: clock_interrupted
        completed('STOP;', controller)
        stopped = next_clock(following) do |event|
          event.kind == NX::ClockEventKind::STATE && event.state == NX::ClockState::STOPPED
        end
        assert_equal domain, stopped.domain
        completed('DETACH DOMAIN CLOCK;', following)
        assert_nil following.domain_clock(domain)
      ensure
        relay.restore
        following.close
        controller.close
        relay.close
      end
    end

    def test_clock_waits_have_cancellation_and_deadlines
      assert_instance_of NX::Session, session
      assert_raises(NX::DeadlineError) { session.next_clock_event(cancel: NX::Cancel.new(deadline: 0.1)) }
      token = NX::Cancel.new(deadline: 120)
      worker = Thread.new { assert_raises(NX::CancelledError) { session.next_clock_event(cancel: token) } }
      token.trigger
      assert worker.join(WAIT)
      assert_equal NX::FailureKind::CANCELLED, worker.value.kind
    ensure
      token&.trigger
      worker&.join(WAIT)
    end
  end
end
