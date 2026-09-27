# frozen_string_literal: true

# The Ruby probe of the shared Rust binding, through Fiddle.
#
# It prints the same conformance report as every other probe. Every event reference is a
# Fiddle::Pointer whose free function is the binding's release, so the garbage collector releases a
# reference nothing reaches; the probe also releases references explicitly. Frames are read in
# place through the pointer the binding lends, and columns are copied into Fiddle buffers one call
# per column.

require 'fiddle'

module Probe
  LIBRARY = Fiddle.dlopen(ENV.fetch('NERVIX_CLIENT_LIBRARY'))
  VOIDP = Fiddle::TYPE_VOIDP
  SIZE = Fiddle::TYPE_SIZE_T
  INT = Fiddle::TYPE_INT32_T
  BOOL = Fiddle::TYPE_BOOL
  U64 = Fiddle::TYPE_UINT64_T
  POINTER_BYTES = Fiddle::SIZEOF_VOIDP
  SIZE_BYTES = Fiddle::SIZEOF_SIZE_T

  ERROR_DEADLINE = 6
  ERROR_CANCELLED = 7
  PART_ROWS = 1
  PART_BRANCH_KEY = 2
  CELL_VALUE = 1
  CELL_NULL = 2
  CELL_REDACTED = 3
  EVENT_ROWS = 1
  DISPOSITIONS = [nil, 'completed', 'failed', 'not_leader', 'transaction_detached',
                  'transaction_taken_over', 'outcome_unknown', 'execution_reference_conflict',
                  'execution_reference_expired', 'preview_stale'].freeze
  TYPES = [nil, 'U8', 'I8', 'U16', 'I16', 'U32', 'I32', 'U64', 'I64', 'F32', 'F64', 'BOOL',
           'STRING', 'BYTES', 'DATETIME', 'FIXED_LIST', 'LIST'].freeze
  # The width of each fixed-width type, and the `unpack` directive that reads it in native byte
  # order. Floats are read as unsigned integers of their width, because the report prints bits.
  FIXED = {
    'U8' => [1, 'C', 'u8'], 'I8' => [1, 'c', 'i8'], 'U16' => [2, 'S', 'u16'],
    'I16' => [2, 's', 'i16'], 'U32' => [4, 'L', 'u32'], 'I32' => [4, 'l', 'i32'],
    'U64' => [8, 'Q', 'u64'], 'I64' => [8, 'q', 'i64'], 'F32' => [4, 'L', 'f32'],
    'F64' => [8, 'Q', 'f64'], 'BOOL' => [1, 'C', 'bool'], 'DATETIME' => [8, 'q', 'datetime']
  }.freeze

  def self.function(name, arguments, result)
    Fiddle::Function.new(LIBRARY[name], arguments, result)
  end

  F = {
    error_kind: function('nx_error_kind_of', [VOIDP], INT),
    error_message: function('nx_error_message', [VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    error_reference: function('nx_error_execution_reference', [VOIDP, VOIDP, VOIDP], BOOL),
    error_free: function('nx_error_free', [VOIDP], Fiddle::TYPE_VOID),
    cancel_new: function('nx_cancel_new', [], VOIDP),
    cancel_with_deadline: function('nx_cancel_with_deadline', [U64, VOIDP], VOIDP),
    cancel_trigger: function('nx_cancel_trigger', [VOIDP], Fiddle::TYPE_VOID),
    session_connect: function('nx_session_connect',
                              [VOIDP, SIZE, VOIDP, SIZE, VOIDP, SIZE, VOIDP, SIZE, VOIDP, VOIDP], VOIDP),
    session_free: function('nx_session_free', [VOIDP], Fiddle::TYPE_VOID),
    session_prepare: function('nx_session_prepare', [VOIDP, VOIDP, SIZE, VOIDP, VOIDP], VOIDP),
    execution_free: function('nx_execution_free', [VOIDP], Fiddle::TYPE_VOID),
    session_execute: function('nx_session_execute', [VOIDP, VOIDP, VOIDP, VOIDP], VOIDP),
    session_next_event: function('nx_session_next_event', [VOIDP, VOIDP, VOIDP], VOIDP),
    outcome_disposition: function('nx_outcome_disposition', [VOIDP], INT),
    outcome_diagnostic_count: function('nx_outcome_diagnostic_count', [VOIDP], SIZE),
    outcome_diagnostic: function('nx_outcome_diagnostic',
                                 [VOIDP, SIZE, VOIDP, VOIDP, VOIDP, VOIDP, VOIDP], VOIDP),
    outcome_subscription: function('nx_outcome_subscription', [VOIDP, VOIDP, VOIDP, VOIDP], BOOL),
    outcome_schema: function('nx_outcome_schema', [VOIDP, VOIDP], VOIDP),
    schema_field_count: function('nx_schema_field_count', [VOIDP, INT, VOIDP], VOIDP),
    schema_field: function('nx_schema_field',
                           [VOIDP, INT, SIZE, VOIDP, VOIDP, VOIDP, VOIDP, VOIDP], VOIDP),
    schema_branch: function('nx_schema_branch', [VOIDP, VOIDP, VOIDP], BOOL),
    schema_free: function('nx_schema_free', [VOIDP], Fiddle::TYPE_VOID),
    event_kind: function('nx_event_kind_of', [VOIDP], INT),
    event_subscription: function('nx_event_subscription', [VOIDP, VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    event_row_count: function('nx_event_row_count', [VOIDP], U64),
    event_retain: function('nx_event_retain', [VOIDP], VOIDP),
    event_frame: function('nx_event_frame', [VOIDP, VOIDP, VOIDP], VOIDP),
    event_column_states: function('nx_event_column_states', [VOIDP, INT, SIZE, VOIDP, SIZE], VOIDP),
    event_column_fixed: function('nx_event_column_fixed', [VOIDP, INT, SIZE, VOIDP, SIZE], VOIDP),
    event_column_varlen: function('nx_event_column_varlen',
                                  [VOIDP, INT, SIZE, VOIDP, SIZE, VOIDP, SIZE, VOIDP], VOIDP),
    event_cell_varlen: function('nx_event_cell_varlen', [VOIDP, INT, SIZE, SIZE, VOIDP, VOIDP], VOIDP)
  }.freeze

  # The binding's release functions, run by the collector on pointers it frees.
  EVENT_RELEASE = LIBRARY['nx_event_release']
  OUTCOME_FREE = LIBRARY['nx_outcome_free']
  CANCEL_FREE = LIBRARY['nx_cancel_free']

  # A failure the binding returned.
  class Failure < StandardError
    attr_reader :kind, :reference

    def initialize(kind, message, reference)
      super("kind #{kind}: #{message}")
      @kind = kind
      @reference = reference
    end
  end

  # An out-parameter slot of `bytes` bytes, freed by the collector.
  def self.slot(bytes = POINTER_BYTES)
    Fiddle::Pointer.malloc(bytes, Fiddle::RUBY_FREE)
  end

  def self.read_size(slot) = slot[0, SIZE_BYTES].unpack1('J')

  def self.read_pointer(slot) = Fiddle::Pointer.new(slot[0, POINTER_BYTES].unpack1('J'))

  # Copies bytes the binding lent for the duration of this call.
  def self.borrowed(pointer_slot, length_slot)
    length = read_size(length_slot)
    return ''.b if length.zero?

    read_pointer(pointer_slot)[0, length]
  end

  # Raises the error a call returned, releasing it.
  def self.check(error)
    return if error.null?

    message = slot
    message_len = slot(SIZE_BYTES)
    F[:error_message].call(error, message, message_len)
    reference = slot
    reference_len = slot(SIZE_BYTES)
    named = F[:error_reference].call(error, reference, reference_len) ? borrowed(reference, reference_len) : nil
    failure = Failure.new(F[:error_kind].call(error), borrowed(message, message_len).force_encoding('UTF-8'), named)
    F[:error_free].call(error)
    raise failure
  end

  def self.cancel(deadline_millis = nil)
    handle = if deadline_millis.nil?
               F[:cancel_new].call
             else
               out = slot
               check(F[:cancel_with_deadline].call(deadline_millis, out))
               read_pointer(out)
             end
    handle.free = CANCEL_FREE
    handle
  end

  # One field of a schema.
  Field = Struct.new(:name, :type, :nullable, :sensitive) do
    def line(prefix)
      "#{prefix} #{name} #{type} #{nullable ? 'nullable' : 'required'} #{sensitive ? 'sensitive' : 'public'}"
    end
  end

  def self.fields(schema, part)
    count = slot(SIZE_BYTES)
    check(F[:schema_field_count].call(schema, part, count))
    Array.new(read_size(count)) do |index|
      name = slot
      name_len = slot(SIZE_BYTES)
      type = slot(4)
      nullable = slot(1)
      sensitive = slot(1)
      check(F[:schema_field].call(schema, part, index, name, name_len, type, nullable, sensitive))
      Field.new(borrowed(name, name_len), TYPES.fetch(type[0, 4].unpack1('l')),
                nullable[0, 1].unpack1('C') != 0, sensitive[0, 1].unpack1('C') != 0)
    end
  end

  # One reference to an event. The collector releases it when nothing reaches it any more.
  class Event
    attr_reader :handle

    def initialize(handle)
      handle.free = EVENT_RELEASE
      @handle = handle
    end

    def retain = Event.new(F[:event_retain].call(@handle))

    # Releases the reference now instead of waiting for the collector.
    def release = @handle.call_free

    def kind = F[:event_kind].call(@handle)

    def row_count = F[:event_row_count].call(@handle)

    def subscription
      name = Probe.slot
      name_len = Probe.slot(SIZE_BYTES)
      generation = Probe.slot(8)
      F[:event_subscription].call(@handle, name, name_len, generation)
      [Probe.borrowed(name, name_len), generation[0, 8].unpack1('Q')]
    end

    # The frame, read in place through the pointer the binding lends. Valid while this reference
    # is held.
    def frame
      frame = Probe.slot
      frame_len = Probe.slot(SIZE_BYTES)
      Probe.check(F[:event_frame].call(@handle, frame, frame_len))
      pointer = Probe.read_pointer(frame)
      pointer.size = Probe.read_size(frame_len)
      pointer
    end

    def column(part, index, field, cells)
      states = Probe.slot([cells, 1].max)
      Probe.check(F[:event_column_states].call(@handle, part, index, states, cells))
      states = states[0, cells].unpack('C*')
      values = if FIXED.key?(field.type)
                 fixed(part, index, field, cells)
               elsif %w[STRING BYTES].include?(field.type)
                 varlen(part, index, field, cells)
               else
                 raise 'list columns are read from the frame'
               end
      states.each_with_index.map do |state, row|
        case state
        when CELL_NULL then 'null'
        when CELL_REDACTED then 'redacted'
        when CELL_VALUE then values.fetch(row)
        else raise "unknown cell state #{state}"
        end
      end
    end

    def fixed(part, index, field, cells)
      width, directive, prefix = FIXED.fetch(field.type)
      buffer = Probe.slot(cells * width)
      Probe.check(F[:event_column_fixed].call(@handle, part, index, buffer, cells * width))
      buffer[0, cells * width].unpack("#{directive}*").map do |value|
        case field.type
        when 'F32' then format('f32:%08x', value)
        when 'F64' then format('f64:%016x', value)
        when 'BOOL' then "bool:#{value != 0}"
        else "#{prefix}:#{value}"
        end
      end
    end

    def varlen(part, index, field, cells)
      data_len = Probe.slot(SIZE_BYTES)
      Probe.check(F[:event_column_varlen].call(@handle, part, index, nil, 0, nil, 0, data_len))
      needed = [Probe.read_size(data_len), 1].max
      offsets = Probe.slot((cells + 1) * 8)
      data = Probe.slot(needed)
      Probe.check(F[:event_column_varlen].call(@handle, part, index, offsets, cells + 1, data, needed, data_len))
      bounds = offsets[0, (cells + 1) * 8].unpack('Q*')
      prefix = field.type == 'STRING' ? 'str' : 'bytes'
      Array.new(cells) do |row|
        copied = bounds[row] == bounds[row + 1] ? ''.b : data[bounds[row], bounds[row + 1] - bounds[row]]
        "#{prefix}:#{copied.unpack1('H*')}"
      end
    end

    def borrowed_cell(part, row, index)
      value = Probe.slot
      value_len = Probe.slot(SIZE_BYTES)
      Probe.check(F[:event_cell_varlen].call(@handle, part, row, index, value, value_len))
      Probe.borrowed(value, value_len)
    end

    def render(part, fields, cells)
      rows = Array.new(cells) { [] }
      fields.each_with_index do |field, index|
        column(part, index, field, cells).each_with_index do |rendered, row|
          if %w[STRING BYTES].include?(field.type) && rendered.include?(':')
            copied = [rendered.split(':', 2)[1]].pack('H*')
            raise 'a copied value differs from the same value borrowed from the frame' if copied != borrowed_cell(part, row, index)
          end
          rows[row] << "#{field.name}=#{rendered}"
        end
      end
      rows.map { |row| row.join(' ') }
    end

    def row_lines(fields, key_fields)
      key = key_fields.empty? ? '' : render(PART_BRANCH_KEY, key_fields, 1).first
      render(PART_ROWS, fields, row_count).map { |row| "ROW [#{key}] #{row}" }
    end
  end

  # An open session.
  class Session
    def initialize(server, domain, username, password)
      out = Probe.slot
      Probe.check(F[:session_connect].call(server, server.bytesize, domain, domain.bytesize, username,
                                           username.bytesize, password, password.bytesize, nil, out))
      @handle = Probe.read_pointer(out)
    end

    def close = F[:session_free].call(@handle)

    def execute(query, cancel = nil)
      execution = Probe.slot
      Probe.check(F[:session_prepare].call(@handle, query, query.bytesize, nil, execution))
      prepared = Probe.read_pointer(execution)
      begin
        outcome = Probe.slot
        Probe.check(F[:session_execute].call(@handle, prepared, cancel, outcome))
        handle = Probe.read_pointer(outcome)
        handle.free = OUTCOME_FREE
        handle
      ensure
        F[:execution_free].call(prepared)
      end
    end

    def next_event(cancel)
      out = Probe.slot
      Probe.check(F[:session_next_event].call(@handle, cancel, out))
      Event.new(Probe.read_pointer(out))
    end
  end

  def self.disposition(outcome) = DISPOSITIONS.fetch(F[:outcome_disposition].call(outcome))

  def self.error_line(outcome)
    count = F[:outcome_diagnostic_count].call(outcome)
    span = 'none'
    if count.positive?
      message = slot
      message_len = slot(SIZE_BYTES)
      has_span = slot(1)
      start = slot(4)
      finish = slot(4)
      check(F[:outcome_diagnostic].call(outcome, 0, message, message_len, has_span, start, finish))
      span = "#{start[0, 4].unpack1('L')}..#{finish[0, 4].unpack1('L')}" if has_span[0, 1].unpack1('C') != 0
    end
    "ERROR #{disposition(outcome)} diagnostics=#{count} span=#{span}"
  end

  def self.failure_of
    yield
    raise 'a call succeeded where it had to fail'
  rescue Failure => e
    e
  end

  def self.check_retention(retained, reported, fields, key_fields)
    frames = retained.map { |event| event.frame[0, event.frame.size] }
    # References nothing reaches are released by the collector, concurrently with reads of the
    # references that remain.
    retained.each { |event| event.retain.row_lines(fields, key_fields) }
    3.times do
      GC.start(full_mark: true, immediate_sweep: true)
      Array.new(2048) { |size| 'x' * (size * 64) }
    end
    retained.each_with_index do |event, index|
      raise 'a retained frame changed while it was retained' if event.frame[0, event.frame.size] != frames[index]
      raise 'a retained frame is not a ServerMessage frame' if event.frame[4, 4] != 'NXSM'
      raise 'a retained event reads differently than it did' if event.row_lines(fields, key_fields) != reported[index]
    end
    Thread.new { retained.each(&:release) }.join
  end

  def self.check_cancellation(session)
    cancel = cancel()
    waiter = Thread.new { failure_of { session.next_event(cancel) } }
    sleep 0.1
    F[:cancel_trigger].call(cancel)
    raise 'a cancelled wait did not report CANCELLED' unless waiter.join(30)&.value&.kind == ERROR_CANCELLED

    expired = failure_of { session.next_event(cancel(50)) }
    raise 'an expired wait did not report DEADLINE' unless expired.kind == ERROR_DEADLINE

    cancelled = cancel()
    F[:cancel_trigger].call(cancelled)
    command = failure_of { session.execute('SHOW DOMAINS;', cancelled) }
    return if command.kind == ERROR_CANCELLED && !command.reference.to_s.empty?

    raise 'a cancelled command did not report CANCELLED with its execution reference'
  end

  def self.report(line)
    $stdout.puts(line)
    $stdout.flush
  end

  def self.main
    relay = ENV.fetch('NERVIX_PROBE_RELAY')
    subscription = ENV.fetch('NERVIX_PROBE_SUBSCRIPTION')
    expected_rows = Integer(ENV.fetch('NERVIX_PROBE_ROWS'))
    session = Session.new(ENV.fetch('NERVIX_PROBE_GRPC_URI'), ENV.fetch('NERVIX_PROBE_DOMAIN'),
                          ENV.fetch('NERVIX_PROBE_USERNAME'), ENV.fetch('NERVIX_PROBE_PASSWORD'))

    report("OPERATION #{disposition(session.execute("SHOW CREATE RELAY #{relay};"))}")
    report(error_line(session.execute('CREATE RELAY;')))

    opened = session.execute("CREATE SUBSCRIPTION #{subscription} TO #{relay};")
    name = slot
    name_len = slot(SIZE_BYTES)
    generation = slot(8)
    raise 'the subscribe command opened no subscription' unless F[:outcome_subscription].call(opened, name, name_len, generation)

    generation = generation[0, 8].unpack1('Q')
    schema_out = slot
    check(F[:outcome_schema].call(opened, schema_out))
    schema = read_pointer(schema_out)
    fields = fields(schema, PART_ROWS)
    key_fields = fields(schema, PART_BRANCH_KEY)
    branch = slot
    branch_len = slot(SIZE_BYTES)
    branch_name = F[:schema_branch].call(schema, branch, branch_len) ? borrowed(branch, branch_len) : nil
    F[:schema_free].call(schema)
    fields.each { |field| report(field.line('FIELD')) }
    report("BRANCH #{branch_name}") unless branch_name.nil?
    key_fields.each { |field| report(field.line('KEY')) }
    report('SUBSCRIBED')

    deadline = cancel(120_000)
    retained = []
    reported = []
    seen = 0
    while seen < expected_rows
      event = session.next_event(deadline)
      raise 'the subscription reported something other than rows first' unless event.kind == EVENT_ROWS
      raise 'rows arrived for another subscription' unless event.subscription == [subscription, generation]

      lines = event.row_lines(fields, key_fields)
      lines.each { |line| report(line) }
      seen += lines.size
      reported << lines
      # Keep a second reference and release the first now, so the rows must survive on it alone.
      retained << event.retain
      event.release
    end

    check_retention(retained, reported, fields, key_fields)
    check_cancellation(session)
    report('CHECKS ok')
    report("CLOSED #{disposition(session.execute("DELETE SUBSCRIPTION #{subscription};"))}")
    session.close
    report('PASS')
  end
end

begin
  Probe.main
rescue StandardError => e
  warn "probe failed: #{e.full_message}"
  exit 1
end
