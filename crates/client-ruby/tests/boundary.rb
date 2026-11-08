# encoding: UTF-8
# frozen_string_literal: true

# Executed inside the embedded Ruby interpreter of the binding's Rust owner test.
require 'socket'
require 'weakref'

module RubyBoundary
  NX = Nervix::Client

  def self.check(condition, message)
    raise message unless condition
  end

  def self.failure(expected)
    yield
    raise "expected #{expected}"
  rescue NX::ClientError => error
    check(error.is_a?(expected), "expected #{expected}, got #{error.class}")
    error
  end

  def self.fields_and_batches
    fields = NX::Fields.new
    fields.add('id', NX::FieldType::U64)
    fields.add('text', NX::FieldType::STRING, nullable: true)
    fields.add('raw', NX::FieldType::BYTES)
    fields.add('nested', NX::FieldType::LIST)
    fields.element(NX::FieldType::FIXED_LIST, 2)
    fields.element(NX::FieldType::I16)
    schema = fields.schema
    check(schema.fields.map(&:name) == %w[id text raw nested], 'field order changed')
    check(schema.fields.last.levels.map(&:type) == [NX::FieldType::LIST, NX::FieldType::FIXED_LIST, NX::FieldType::I16], 'nested type changed')
    failure(NX::InvalidArgumentError) { fields.element(NX::FieldType::U8) }

    builder = NX::BatchBuilder.new(schema, 2)
    ids = [0, (1 << 64) - 1].pack('Q*')
    builder.fixed(0, 0, ids)
    ids.replace("\xff".b * ids.bytesize)
    builder.states(1, [true, false])
    text = "a\0é".encode('UTF-8')
    builder.varlen(1, 0, [0, text.bytesize, text.bytesize], text)
    text.replace('overwritten')
    builder.varlen(2, 0, [0, 2, 2], "\x00\xff".b)
    builder.offsets(3, 0, [0, 1, 2])
    builder.fixed(3, 2, [-32_768, 32_767, -1, 0].pack('s*'))
    batch = builder.finish
    check(batch.column(0) == [0, (1 << 64) - 1], 'unsigned values changed')
    check(batch.column(1) == ["a\0é", nil], 'text, NUL, or null changed')
    check(batch.column(2) == ["\x00\xff".b, ''.b], 'binary values changed')
    check(batch.column(3) == [[[-32_768, 32_767]], [[-1, 0]]], 'nested values changed')
    check(batch.states(1) == [NX::CellState::VALUE, NX::CellState::NULL], 'cell states changed')
    check(batch.fixed(0, 0).unpack('Q*') == [0, (1 << 64) - 1], 'bulk fixed values changed')
    check(batch.varlen(2, 0) == [[0, 2, 2], "\x00\xff".b], 'bulk binary values changed')
    check(batch.offsets(3, 0) == [0, 1, 2], 'list offsets changed')
    failure(NX::TypeMismatchError) { batch.fixed(1, 0) }
    failure(NX::InvalidArgumentError) { builder.fixed(0, 0, ''.b); builder.finish }

    view = batch.ipc
    check(view.is_a?(IO::Buffer) && view.readonly?, 'IPC is not a read-only buffer')
    check(view.locked? && view.frozen?, 'a view can lose its native owner')
    begin
      view.instance_variable_set(:@nervix_owner, nil)
      raise 'a view permitted its owner to be withdrawn'
    rescue FrozenError
      # The owner reference belongs to the view for its entire lifetime.
    end
    begin
      view.transfer
      raise 'a root view transferred its pointer without its owner'
    rescue IO::Buffer::LockedError
      # Derived views retain this root through IO::Buffer's own source reference.
    end
    begin
      view.set_string('x')
      raise 'a root view permitted native storage to change'
    rescue IO::Buffer::AccessError
      # Read-only applies to the external memory as well as Ruby object fields.
    end
    expected = view.get_string
    slice = view.slice(1, view.size - 1)
    begin
      slice.set_string('x')
      raise 'a derived view permitted native storage to change'
    rescue IO::Buffer::AccessError
      # IO::Buffer propagates the read-only flag to slices.
    end
    weak = WeakRef.new(batch)
    batch = nil
    view = nil
    GC.start(full_mark: true, immediate_sweep: true)
    GC.compact
    check(weak.weakref_alive?, 'an IPC slice lost its native owner')
    check(slice.get_string == expected.byteslice(1..), 'an IPC slice changed after collection')
    decoded = NX::Batch.decode(schema, expected, 2)
    check(decoded.column(0) == [0, (1 << 64) - 1], 'IPC lost a complete unsigned value')
    check(decoded.column(3) == [[[-32_768, 32_767]], [[-1, 0]]], 'IPC lost nested values')
  end

  # Accepting the actual HTTP/2 preface proves the worker is already inside its native wait.
  # Cancelling then needs the main Ruby thread to run while that wait has released the GVL.
  def self.waiting_connection(server, cancel)
    waiter = Thread.new do
      failure(NX::CancelledError) do
        NX::Session.connect("http://127.0.0.1:#{server.addr[1]}", cancel: cancel)
      end
    end
    check(IO.select([server], nil, nil, 10), 'the native call did not reach its peer')
    connection = server.accept
    check(IO.select([connection], nil, nil, 10), 'the native call sent no HTTP/2 preface')
    check(!connection.readpartial(24).empty?, 'no native connection was admitted')
    [waiter, connection]
  end

  def self.cancellation
    server = TCPServer.new('127.0.0.1', 0)
    token = NX::Cancel.new(deadline: 10)
    waiter, connection = waiting_connection(server, token)
    token.trigger
    check(waiter.join(10), 'a native connect kept the GVL or ignored cancellation')
    check(waiter.value.kind == NX::FailureKind::CANCELLED, 'cancellation kind changed')
    connection.close

    # An interrupt ends only the interrupted call; reusing its parent token still reaches the peer.
    shared = NX::Cancel.new(deadline: 10)
    waiter, connection = waiting_connection(server, shared)
    waiter.kill
    check(waiter.join(10), 'a Ruby interrupt did not end the native call')
    connection.close
    waiter, connection = waiting_connection(server, shared)
    shared.trigger
    check(waiter.join(10), 'the parent cancellation did not reach its next call')
    waiter.value
    connection.close
    server.close

    NX::Cancel.new(deadline: nil)
    NX::Cancel.new(deadline: 0)
    failure(NX::InvalidArgumentError) { NX::Cancel.new(deadline: -1) }
    failure(NX::InvalidArgumentError) { NX::Cancel.new(deadline: Float::INFINITY) }
  end

  def self.run
    fields_and_batches
    cancellation
  end
end

RubyBoundary.run
