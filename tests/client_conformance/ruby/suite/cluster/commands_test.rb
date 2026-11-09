# frozen_string_literal: true

module RubySuite
  class CommandsTest < ClusterTest
    def test_connection_options_authentication_tls_and_seeds
      # ANCHOR: connect
      connected = NX::Session.connect(cluster.uri, **cluster.options)
      assert_instance_of NX::Session, connected
      begin
        outcome = connected.execute(connected.prepare('LIST DOMAINS;'), cancel: RubySuite.deadline)
        assert_equal NX::Disposition::COMPLETED, outcome.disposition
        assert_includes outcome.message, cluster.domain
      ensure
        connected.close
      end
      # ANCHOR_END: connect
      [NX::Tls::PREFERRED, NX::Tls::REQUIRED].each do |tls|
        if URI(cluster.uri).scheme == 'http' && tls == NX::Tls::REQUIRED
          assert_raises(NX::ConnectError) { cluster.connect(tls: tls) }
        else
          candidate = cluster.connect(tls: tls)
          begin
            completed('LIST DOMAINS;', candidate)
          ensure
            candidate.close
          end
        end
      end
      if URI(cluster.uri).scheme == 'https'
        assert_raises(NX::ConnectError) { cluster.connect(ca_certificate: nil) }
      end
      refused = assert_raises(NX::ConnectError) { cluster.connect(password: 'incorrect password') }
      assert_equal NX::FailureKind::CONNECT, refused.kind
      refute_empty refused.message
      refute_includes refused.message, 'incorrect password'
      # ANCHOR: seeds
      unreachable = URI(cluster.uri)
      unused = TCPServer.new('127.0.0.1', 0)
      unreachable.host = '127.0.0.1'
      unreachable.port = unused.addr[1]
      unused.close
      candidate = cluster.connect(server: unreachable.to_s,
                                  seeds: ENV.fetch('NERVIX_SUITE_SEEDS').split(','),
                                  connect_timeout: 5, request_timeout: 120,
                                  retry_timeout: 120, backup_wait_timeout: 120)
      begin
        completed('LIST DOMAINS;', candidate)
      ensure
        candidate.close
      end
      # ANCHOR_END: seeds
      assert_raises(NX::ConnectError) { cluster.connect(server: unreachable.to_s, seeds: []) }
    end

    def test_commands_diagnostics_cancellation_and_recovery
      assert_instance_of NX::Session, session
      statement = "CREATE SCHEMA #{unique('reading')} ( value I64 );"
      # ANCHOR: execute
      execution = session.prepare(statement, cancel: RubySuite.deadline)
      assert_instance_of NX::Execution, execution
      outcome = session.execute(execution, cancel: RubySuite.deadline)
      assert_instance_of NX::Outcome, outcome
      assert_equal NX::Disposition::COMPLETED, outcome.disposition
      assert_equal execution.reference, outcome.execution_reference
      puts outcome.message
      # ANCHOR_END: execute
      assert_empty outcome.diagnostics
      assert_nil outcome.upload_failure
      assert_nil outcome.subscription
      assert_nil outcome.schema
      assert_nil outcome.backup
      assert_nil outcome.restore
      # ANCHOR: diagnostics
      failed = execute('CREATE RELAY;')
      assert_equal NX::Disposition::FAILED, failed.disposition
      diagnostic = failed.diagnostics.fetch(0)
      assert_instance_of NX::Diagnostic, diagnostic
      assert_equal [12, 12], diagnostic.span
      refute_empty diagnostic.message
      # ANCHOR_END: diagnostics
      # ANCHOR: recover
      execution = session.prepare("CREATE SCHEMA #{unique('cancelled')} ( value I64 );")
      token = NX::Cancel.new
      token.trigger
      cancelled = assert_raises(NX::CancelledError) { session.execute(execution, cancel: token) }
      assert_equal execution.reference, cancelled.execution_reference
      recovered = session.execute(execution, cancel: RubySuite.deadline)
      assert_equal NX::Disposition::COMPLETED, recovered.disposition
      assert_equal recovered.execution_reference, session.execute(execution).execution_reference
      # ANCHOR_END: recover
      expired = session.prepare("CREATE SCHEMA #{unique('expired')} ( value I64 );")
      failure = assert_raises(NX::DeadlineError) { session.execute(expired, cancel: NX::Cancel.new(deadline: 0)) }
      assert_equal expired.reference, failure.execution_reference
      assert_equal NX::Disposition::COMPLETED, session.execute(expired, cancel: RubySuite.deadline).disposition
    end

    def test_completion_spans_are_utf8_bytes_and_pages_continue
      assert_instance_of NX::Session, session
      # ANCHOR: completion
      text = 'CREATE SCH'
      page = session.suggest(text, text.bytesize, 20, nil, cancel: RubySuite.deadline)
      assert_instance_of NX::Suggestions, page
      assert_equal NX::CompletionStatus::READY, page.status
      suggestion = page.suggestions.find { |item| item.value == 'SCHEMA' }
      assert_instance_of NX::Suggestion, suggestion
      assert_equal 'SCHEMA', suggestion.value
      assert_equal NX::CompletionKind::TEXT, suggestion.kind
      from, to = suggestion.span
      completed_text = text.byteslice(0...from) + suggestion.replacement + text.byteslice(to..)
      assert_match(/\ACREATE SCHEMA/, completed_text)
      # ANCHOR_END: completion
      assert_equal text.bytesize, to
      text = "// é\nCREATE SCH"
      suggestion = session.suggest(text, text.bytesize, 20).suggestions.find { |item| item.value == 'SCHEMA' }
      assert_equal text.bytesize, suggestion.span.last
      # ANCHOR: completion_pages
      values = []
      continuation = nil
      64.times do
        page = session.suggest('CREATE ', 7, 2, continuation, cancel: RubySuite.deadline)
        assert_instance_of NX::Suggestions, page
        values.concat(page.suggestions.map(&:value))
        continuation = page.continuation
        break if continuation.nil?
      end
      assert_nil continuation, 'completion did not end within 64 pages'
      assert_operator values.length, :>, 2
      assert_equal values.uniq, values
      # ANCHOR_END: completion_pages
      names = session.suggest('SHOW CREATE RELAY ', 18, 50).suggestions.map(&:value)
      assert_includes names, 'typed_rows'
      assert_includes names, 'watched_rows'
    end

    def test_one_session_serves_threads_while_an_event_wait_is_cancelled
      assert_instance_of NX::Session, session
      # ANCHOR: threads
      token = NX::Cancel.new(deadline: 120)
      waiter = Thread.new do
        assert_raises(NX::CancelledError) { session.next_event(cancel: token) }
      end
      workers = 4.times.map do |index|
        Thread.new { execute("CREATE SCHEMA #{unique('shared')}_#{index} ( value I64 );") }
      end
      workers.each do |worker|
        assert worker.join(WAIT), 'a shared session call kept another Ruby thread from progressing'
        assert_equal NX::Disposition::COMPLETED, worker.value.disposition
      end
      token.trigger
      assert waiter.join(WAIT), 'the event wait ignored cancellation'
      assert_equal NX::FailureKind::CANCELLED, waiter.value.kind
      # ANCHOR_END: threads
      assert_raises(NX::DeadlineError) { session.next_event(cancel: NX::Cancel.new(deadline: 0.1)) }
    ensure
      token&.trigger
      waiter&.join(WAIT)
      workers&.each { |worker| worker.join(WAIT) }
    end

    def test_session_block_closes_on_normal_return_and_exception
      # ANCHOR: session_block
      held = nil
      result = NX::Session.connect(cluster.uri, **cluster.options) do |connected|
        held = connected
        assert_instance_of NX::Session, connected
        assert_equal NX::Disposition::COMPLETED, connected.execute(connected.prepare('LIST DOMAINS;')).disposition
        :finished
      end
      assert_equal :finished, result
      assert_raises(NX::ClosedError) { held.prepare('LIST DOMAINS;') }
      # ANCHOR_END: session_block
      failure = Class.new(StandardError)
      assert_raises(failure) do
        NX::Session.connect(cluster.uri, **cluster.options) do |connected|
          held = connected
          raise failure
        end
      end
      assert_raises(NX::ClosedError) { held.prepare('LIST DOMAINS;') }
      result = NX::Session.connect(cluster.uri, **cluster.options) do |connected|
        held = connected
        break :broken
      end
      assert_equal :broken, result
      assert_raises(NX::ClosedError) { held.prepare('LIST DOMAINS;') }
      result = lambda do
        NX::Session.connect(cluster.uri, **cluster.options) do |connected|
          held = connected
          return :returned
        end
      end.call
      assert_equal :returned, result
      assert_raises(NX::ClosedError) { held.prepare('LIST DOMAINS;') }
    ensure
      held&.close
    end

    def test_backup_restore_and_reexecution_keep_their_outcomes
      archive = File.join(cluster.backups, "#{unique('domain')}.nvxb")
      copy = "#{cluster.domain}_#{unique('copy')}"
      # ANCHOR: backup
      execution = session.prepare("BACKUP DOMAIN #{cluster.domain} TO '#{archive}';")
      token = NX::Cancel.new
      token.trigger
      cancelled = assert_raises(NX::CancelledError) { session.execute(execution, cancel: token) }
      assert_equal execution.reference, cancelled.execution_reference
      outcome = session.execute(execution, cancel: RubySuite.deadline)
      assert_equal NX::Disposition::COMPLETED, outcome.disposition, outcome.message
      backup = outcome.backup
      assert_instance_of NX::Backup, backup
      assert_equal File.size(archive), backup.total_bytes
      assert_equal 32, backup.digest.bytesize
      # ANCHOR_END: backup
      # ANCHOR: restore
      outcome = execute("RESTORE DOMAIN #{cluster.domain} AS #{copy} FROM '#{archive}' DRY RUN;")
      assert_equal NX::Disposition::COMPLETED, outcome.disposition, outcome.message
      restore = outcome.restore
      assert_instance_of NX::Restore, restore
      assert restore.dry_run
      refute restore.failed
      assert_equal 1, restore.domains
      assert_operator restore.models, :>, 0
      assert_equal 0, restore.resource_versions
      execution = session.prepare("RESTORE DOMAIN #{cluster.domain} AS #{copy} FROM '#{archive}';")
      cancelled = assert_raises(NX::CancelledError) { session.execute(execution, cancel: token) }
      assert_equal execution.reference, cancelled.execution_reference
      restored = session.execute(execution, cancel: RubySuite.deadline)
      assert_equal NX::Disposition::COMPLETED, restored.disposition, restored.message
      refute restored.restore.dry_run
      refute restored.restore.failed
      # ANCHOR_END: restore
      assert_includes execute('LIST DOMAINS;').message, copy
    end

    def test_upload_identity_conflicts_are_typed_outcome_fields
      resource = unique('resource')
      directory = File.join(cluster.backups, unique('bundle'))
      FileUtils.mkdir_p(directory)
      payload = File.join(directory, 'payload.txt')
      File.binwrite(payload, "first archive\n")
      completed("CREATE RESOURCE #{resource};")
      # ANCHOR: upload
      execution = session.prepare("UPLOAD RESOURCE #{resource} VERSION '#{directory}';")
      uploaded = session.execute(execution, cancel: RubySuite.deadline)
      assert_equal NX::Disposition::COMPLETED, uploaded.disposition, uploaded.message
      assert_nil uploaded.upload_failure
      # The same execution sends the same archive under the same upload identity, so it completes
      # again instead of installing a second version.
      again = session.execute(execution, cancel: RubySuite.deadline)
      assert_equal NX::Disposition::COMPLETED, again.disposition, again.message
      assert_nil again.upload_failure
      File.binwrite(payload, "another archive\n")
      refused = session.execute(execution, cancel: RubySuite.deadline)
      assert_equal NX::Disposition::FAILED, refused.disposition
      assert_equal NX::UploadFailure::IDENTITY_CONFLICT, refused.upload_failure
      # ANCHOR_END: upload
    end
  end
end
