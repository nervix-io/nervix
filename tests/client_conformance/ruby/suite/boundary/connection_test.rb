# frozen_string_literal: true

module RubySuite
  class ConnectionTest < Boundary
    def test_invalid_options_fail_before_a_connection_is_opened
      # ANCHOR: typed_error
      error = assert_raises(NX::InvalidArgumentError) do
        NX::Session.connect('http://127.0.0.1:1', username: 'incomplete')
      end
      assert_kind_of NX::ClientError, error
      assert_equal NX::FailureKind::INVALID_ARGUMENT, error.kind
      assert_nil error.execution_reference
      assert_nil error.open_refusal
      assert_match(/password/, error.message)
      # ANCHOR_END: typed_error
      [Float::NAN, Float::INFINITY, -1].each do |invalid|
        %i[connect_timeout request_timeout retry_timeout backup_wait_timeout].each do |option|
          assert_raises(NX::InvalidArgumentError) { NX::Session.connect('http://127.0.0.1:1', **{option => invalid}) }
        end
        assert_raises(NX::InvalidArgumentError) { NX::Cancel.new(deadline: invalid) }
      end
      NX::Cancel.new(deadline: nil)
      assert_raises(NX::InvalidArgumentError) { NX::Session.connect('http://127.0.0.1:1', tls: -1) }
      assert_raises(NX::InvalidArgumentError) { NX::Session.connect('http://127.0.0.1:1', seeds: ['not a URL']) }
      assert_raises(NX::ConnectError) do
        NX::Session.connect('http://127.0.0.1:1', tls: NX::Tls::REQUIRED, cancel: RubySuite.deadline)
      end
    end

    def test_a_silent_connection_releases_the_gvl_and_another_thread_cancels_it
      listener = TCPServer.new('127.0.0.1', 0)
      # ANCHOR: cancel_thread
      token = NX::Cancel.new(deadline: 120)
      assert_instance_of NX::Cancel, token
      waiter = Thread.new do
        assert_raises(NX::CancelledError) do
          NX::Session.connect("http://127.0.0.1:#{listener.addr[1]}", cancel: token)
        end
      end
      assert IO.select([listener], nil, nil, WAIT), 'the native call did not reach its peer'
      peer = listener.accept
      assert IO.select([peer], nil, nil, WAIT), 'the native call sent no bytes'
      refute_empty peer.readpartial(24)
      token.trigger
      assert waiter.join(WAIT), 'the native call held the GVL or ignored cancellation'
      assert_equal NX::FailureKind::CANCELLED, waiter.value.kind
      # ANCHOR_END: cancel_thread
    ensure
      token&.trigger
      peer&.close
      listener&.close
      waiter&.join(WAIT)
    end

    def test_a_silent_connection_ends_at_its_deadline
      listener = TCPServer.new('127.0.0.1', 0)
      failure = Thread.new do
        assert_raises(NX::DeadlineError) do
          NX::Session.connect("http://127.0.0.1:#{listener.addr[1]}", cancel: NX::Cancel.new(deadline: 0.25))
        end
      end
      assert failure.join(WAIT), 'the native deadline did not end the call'
      assert_equal NX::FailureKind::DEADLINE, failure.value.kind
    ensure
      listener&.close
      failure&.kill
      failure&.join(WAIT)
    end
  end
end
