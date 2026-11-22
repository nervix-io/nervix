# frozen_string_literal: true

module RubySuite
  class NoticesTest < ClusterTest
    def test_a_junction_message_error_is_a_classified_runtime_notice
      assert_instance_of NX::Session, session
      prefix = unique('notice')
      scorer = "#{prefix}_scorer"
      completed("CREATE RELAY #{prefix}_readings SCHEMA suite_row UNBRANCHED;")
      completed("CREATE RELAY #{prefix}_ratios SCHEMA suite_row UNBRANCHED;")
      completed("CREATE CLIENT PRODUCER INGESTOR #{prefix}_in FROM SCHEMA suite_row " \
                'MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s ' \
                "ON QUIESCE SUSPEND TIMESTAMP NOW TO #{prefix}_readings INHERIT ALL UNBRANCHED " \
                'FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;')
      # The divisor is always zero, so the junction's route fails every row it receives.
      completed("CREATE JUNCTION #{scorer} FROM #{prefix}_readings UNBRANCHED TO #{prefix}_ratios " \
                'INHERIT ALL SET amount = input.amount / (input.amount - input.amount) ' \
                'FLUSH IMMEDIATE ON MESSAGE ERROR LOG;')
      # A notice reaches only the sessions open when it is published, so this session was opened
      # before the row is submitted.
      writing = producer(session, "#{prefix}_in")
      # ANCHOR: runtime_notice
      submission = writing.submit(build(writing.schema, 401), cancel: RubySuite.deadline)
      settled = writing.rejoin(submission, cancel: RubySuite.deadline)
      # The route's ON MESSAGE ERROR LOG fails the row's submission and reports its error.
      assert_equal NX::SubmissionResult::PROCESSING_FAILED, settled.result
      assert_equal NX::ProcessingFailure::REJECTED, settled.failure
      token = RubySuite.deadline
      notice = nil
      loop do
        notice = session.next_notice(cancel: token)
        assert_instance_of NX::Notice, notice
        # Other notices may come first: select by the typed fields, never by the text.
        break if notice.error_node == scorer
      end
      assert_instance_of NX::Notice, notice
      assert_equal NX::NoticeKind::RUNTIME, notice.kind
      assert_equal NX::NoticeLevel::ERROR, notice.level
      assert_equal 'evaluation', notice.error_class
      assert_equal 'junction', notice.error_node_kind
      # The text is for display; this check only shows what it says.
      assert_includes notice.message, "junction '#{scorer}' message error in domain '#{cluster.domain}'"
      # ANCHOR_END: runtime_notice
    end

    def test_notice_waits_end_at_their_deadline_or_from_another_thread
      assert_instance_of NX::Session, session
      # A wait returns any notice that arrives first, and ends at its deadline when none does.
      RubySuite.wait_until('no notice wait ended at its deadline') do
        session.next_notice(cancel: NX::Cancel.new(deadline: 0.1))
        false
      rescue NX::DeadlineError
        true
      end
      # ANCHOR: notice_thread
      token = NX::Cancel.new(deadline: WAIT)
      reader = Thread.new do
        # Read notices on their own thread until the application cancels the wait.
        loop { session.next_notice(cancel: token) }
      rescue NX::CancelledError => error
        error
      end
      token.trigger
      assert reader.join(WAIT), 'a cancelled notice wait did not end'
      assert_equal NX::FailureKind::CANCELLED, reader.value.kind
      # ANCHOR_END: notice_thread
    ensure
      token&.trigger
      reader&.join(WAIT)
    end
  end
end
