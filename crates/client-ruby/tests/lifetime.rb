# encoding: UTF-8
# frozen_string_literal: true

# The peer answers the public wire; no Ruby object is ever called from its runtime worker.
module RubyLifetime
  NX = Nervix::Client
  def self.run
    session = NX::Session.connect(URI, domain: nil, username: nil, password: nil, tls: nil,
                                  ca_certificate: nil, seeds: nil, connect_timeout: nil,
                                  request_timeout: 10, retry_timeout: nil, backup_wait_timeout: nil, cancel: nil)
    execution = session.prepare('SHOW DOMAINS;', cancel: nil)
    reference = execution.reference
    fields = NX::Fields.new
    fields.add('id', NX::FieldType::U32)
    token = NX::Cancel.new(deadline: 10)
    producer = session.open_ingestor('ruby_test', 'input', fields, batches: 2, bytes: 4096, cancel: token)
    weak = WeakRef.new(session)
    session.close
    RubyBoundary.failure(NX::ClosedError) { session.prepare('SHOW DOMAINS;', cancel: nil) }
    session = nil
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact
    RubyBoundary.check(!weak.weakref_alive?, 'the producer kept the Ruby session object alive')
    RubyBoundary.check(execution.reference == reference, 'collection changed the prepared execution')
    RubyBoundary.check(producer.state == NX::EndpointState::ACTIVE, 'the producer lost its runtime')
    builder = NX::BatchBuilder.new(producer.schema, 1)
    builder.fixed(0, 0, [4_294_967_295].pack('L'))
    id = producer.submit(builder.finish, cancel: nil)
    result = producer.rejoin(id, cancel: token)
    RubyBoundary.check(result.result == NX::SubmissionResult::COMPLETED, 'an orphaned producer did not submit')
    RubyBoundary.check(result.message.empty?, 'completion acquired a failure message')
    RubyBoundary.failure(NX::TypeMismatchError) { result.refusal }
    producer.close(cancel: token)
    RubyBoundary.check(producer.state == NX::EndpointState::CLOSED, 'the retained producer did not close')
  end
end
RubyLifetime.run
