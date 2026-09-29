# frozen_string_literal: true

# The Ruby probe of the shared Rust binding, through Fiddle.
#
# It prints the same conformance report as every other probe. Every event reference is a
# Fiddle::Pointer whose free function is the binding's release, so the garbage collector releases a
# reference nothing reaches; the probe also releases references explicitly. Frames are read in
# place through the pointer the binding lends, and columns are copied into Fiddle buffers one call
# per column. Run with the `clock` argument, it attaches to the domain's running clock instead,
# reads the clock the attach reported before its first tick, follows the generation a STOP and
# START begin and the attachment restored after its session ends, and detaches.

require 'fiddle'

module Probe
  LIBRARY = Fiddle.dlopen(ENV.fetch('NERVIX_CLIENT_LIBRARY'))
  VOIDP = Fiddle::TYPE_VOIDP
  SIZE = Fiddle::TYPE_SIZE_T
  INT = Fiddle::TYPE_INT32_T
  BOOL = Fiddle::TYPE_BOOL
  U64 = Fiddle::TYPE_UINT64_T
  I64 = Fiddle::TYPE_INT64_T
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
  CLOCK_KINDS = [nil, 'STATE', 'TICK', 'ENDED', 'INTERRUPTED', 'RESTORATION_FAILED'].freeze
  CLOCK_PACED = 4
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
    event_cell_varlen: function('nx_event_cell_varlen', [VOIDP, INT, SIZE, SIZE, VOIDP, VOIDP], VOIDP),
    session_next_clock_event: function('nx_session_next_clock_event', [VOIDP, VOIDP, VOIDP], VOIDP),
    clock_event_kind: function('nx_clock_event_kind_of', [VOIDP], INT),
    clock_event_domain: function('nx_clock_event_domain', [VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    clock_event_generation: function('nx_clock_event_generation', [VOIDP, VOIDP], VOIDP),
    clock_event_state: function('nx_clock_event_state', [VOIDP, VOIDP], VOIDP),
    clock_event_paced: function('nx_clock_event_paced', [VOIDP, VOIDP, VOIDP, VOIDP, VOIDP, VOIDP], VOIDP),
    clock_event_tick: function('nx_clock_event_tick', [VOIDP, VOIDP, VOIDP, VOIDP, VOIDP], VOIDP),
    clock_event_retain: function('nx_clock_event_retain', [VOIDP], VOIDP),
    session_domain_clock: function('nx_session_domain_clock', [VOIDP, VOIDP, SIZE, VOIDP], VOIDP),
    domain_clock_generation: function('nx_domain_clock_generation', [VOIDP], U64),
    domain_clock_state: function('nx_domain_clock_state', [VOIDP], INT),
    domain_clock_paced: function('nx_domain_clock_paced', [VOIDP, VOIDP, VOIDP, VOIDP, VOIDP, VOIDP], VOIDP),
    domain_clock_tick: function('nx_domain_clock_tick', [VOIDP, VOIDP, VOIDP, VOIDP, VOIDP], BOOL),
    domain_clock_logical_time_at: function('nx_domain_clock_logical_time_at', [VOIDP, I64, VOIDP], VOIDP),
    domain_clock_wall_duration_until: function('nx_domain_clock_wall_duration_until',
                                               [VOIDP, I64, I64, VOIDP], VOIDP),
    domain_clock_admission_window: function('nx_domain_clock_admission_window',
                                            [VOIDP, I64, VOIDP, VOIDP, VOIDP], VOIDP),
    domain_clock_admits: function('nx_domain_clock_admits', [VOIDP, I64, I64, VOIDP], VOIDP),
    domain_clock_retain: function('nx_domain_clock_retain', [VOIDP], VOIDP)
  }.freeze

  # The binding's release functions, run by the collector on pointers it frees.
  EVENT_RELEASE = LIBRARY['nx_event_release']
  CLOCK_EVENT_RELEASE = LIBRARY['nx_clock_event_release']
  DOMAIN_CLOCK_RELEASE = LIBRARY['nx_domain_clock_release']
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

  # The committed clock of a paced generation.
  PacedClock = Struct.new(:generation, :period, :skew, :origin, :anchor, :rate) do
    # The report line of the clock, with `prefix` naming where it was read. The UTC anchor depends
    # on when the scenario's START committed, so it is read but not reported.
    def line(prefix, domain)
      "#{prefix} domain=#{domain} generation=#{generation} state=paced period=#{period} skew=#{skew} " \
        "origin=#{origin} rate=f64:#{format('%016x', [rate].pack('D').unpack1('Q'))}"
    end

    # An instant as the report names it: `origin` for the logical origin, the instant otherwise.
    def relative(instant) = instant == origin ? 'origin' : instant.to_s

    def admission(admitted) = admitted ? 'admitted' : 'refused'

    # The report line of the projections of `read`, which holds this clock, at its own UTC anchor:
    # the logical time there, the wait for the next tick center, the admission window, and whether
    # an event at the skew's edge and one nanosecond past it are admitted.
    def projection_line(read, domain)
      window = read.admission_window(anchor)
      raise 'a paced clock reports no admission window' if window.nil?

      edge = origin + skew
      "PROJECTION domain=#{domain} generation=#{generation} anchor=#{relative(read.logical_time_at(anchor))} " \
        "wait=#{read.wall_duration_until(anchor, origin + period)} " \
        "window=#{relative(window[0])}..#{relative(window[1])} " \
        "skew=#{admission(read.admits(anchor, edge))} beyond=#{admission(read.admits(anchor, edge + 1))}"
    end

    # Holds a tick to this clock: the same generation, a boundary of the logical origin plus one
    # period for every id before it, and a serving node's reading that never precedes the origin.
    def check_tick(tick)
      raise 'a tick of one generation followed the state of another' unless tick.generation == generation
      if tick.id.zero? || tick.boundary != origin + ((tick.id - 1) * period)
        raise "a tick's boundary is not the origin plus one period for every id before it"
      end
      raise "the serving node's logical reading precedes the logical origin" if tick.serving_logical < origin
    end

    # The report line of a tick this clock holds.
    def tick_line(tick, domain)
      check_tick(tick)
      "TICK domain=#{domain} generation=#{tick.generation} boundary=origin+(id-1)*period"
    end
  end

  # The progress a tick event reports.
  Tick = Struct.new(:generation, :id, :boundary, :serving_logical)

  # The committed clock `read` writes for `handle`, a clock event or a domain clock.
  def self.paced_clock(read, handle, generation)
    period = slot(8)
    skew = slot(8)
    origin = slot(8)
    anchor = slot(8)
    rate = slot(8)
    check(read.call(handle, period, skew, origin, anchor, rate))
    PacedClock.new(generation, period[0, 8].unpack1('Q'), skew[0, 8].unpack1('Q'), origin[0, 8].unpack1('q'),
                   anchor[0, 8].unpack1('q'), rate[0, 8].unpack1('D'))
  end

  # One reference to a domain clock event. The collector releases it when nothing reaches it.
  class ClockEvent
    def initialize(handle)
      handle.free = CLOCK_EVENT_RELEASE
      @handle = handle
    end

    def retain = ClockEvent.new(F[:clock_event_retain].call(@handle))

    # Releases the reference now instead of waiting for the collector.
    def release = @handle.call_free

    def kind = CLOCK_KINDS.fetch(F[:clock_event_kind].call(@handle))

    def domain
      name = Probe.slot
      name_len = Probe.slot(SIZE_BYTES)
      F[:clock_event_domain].call(@handle, name, name_len)
      Probe.borrowed(name, name_len).force_encoding('UTF-8')
    end

    def generation
      generation = Probe.slot(8)
      Probe.check(F[:clock_event_generation].call(@handle, generation))
      generation[0, 8].unpack1('Q')
    end

    def state
      state = Probe.slot(4)
      Probe.check(F[:clock_event_state].call(@handle, state))
      state[0, 4].unpack1('l')
    end

    def paced = Probe.paced_clock(F[:clock_event_paced], @handle, generation)

    # The tick. The authority's UTC observation depends on when it was accepted, so it is not read.
    def tick
      id = Probe.slot(8)
      boundary = Probe.slot(8)
      serving_logical = Probe.slot(8)
      Probe.check(F[:clock_event_tick].call(@handle, id, boundary, nil, serving_logical))
      Tick.new(generation, id[0, 8].unpack1('Q'), boundary[0, 8].unpack1('q'), serving_logical[0, 8].unpack1('q'))
    end
  end

  # One reference to the clock the session held for a followed domain when the probe read it. The
  # collector releases it when nothing reaches it.
  class DomainClock
    def initialize(handle)
      handle.free = DOMAIN_CLOCK_RELEASE
      @handle = handle
    end

    def retain = DomainClock.new(F[:domain_clock_retain].call(@handle))

    # Releases the reference now instead of waiting for the collector.
    def release = @handle.call_free

    def generation = F[:domain_clock_generation].call(@handle)

    def state = F[:domain_clock_state].call(@handle)

    def paced = Probe.paced_clock(F[:domain_clock_paced], @handle, generation)

    # The id of the newest tick the read holds, or nil when it holds none.
    def tick_id
      id = Probe.slot(8)
      return nil unless F[:domain_clock_tick].call(@handle, id, nil, nil, nil)

      id[0, 8].unpack1('Q')
    end

    def logical_time_at(utc)
      logical = Probe.slot(8)
      Probe.check(F[:domain_clock_logical_time_at].call(@handle, utc, logical))
      logical[0, 8].unpack1('q')
    end

    def wall_duration_until(utc, target)
      wait = Probe.slot(8)
      Probe.check(F[:domain_clock_wall_duration_until].call(@handle, utc, target, wait))
      wait[0, 8].unpack1('Q')
    end

    # The earliest and latest admitted tick centers, or nil for a clock without a window.
    def admission_window(utc)
      has_window = Probe.slot(1)
      earliest = Probe.slot(8)
      latest = Probe.slot(8)
      Probe.check(F[:domain_clock_admission_window].call(@handle, utc, has_window, earliest, latest))
      return nil if has_window[0, 1].unpack1('C').zero?

      [earliest[0, 8].unpack1('q'), latest[0, 8].unpack1('q')]
    end

    def admits(utc, event)
      admitted = Probe.slot(1)
      Probe.check(F[:domain_clock_admits].call(@handle, utc, event, admitted))
      admitted[0, 1].unpack1('C') != 0
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

    # The clock the session holds for `domain`, or nil when it follows none.
    def domain_clock(domain)
      out = Probe.slot
      Probe.check(F[:session_domain_clock].call(@handle, domain, domain.bytesize, out))
      clock = Probe.read_pointer(out)
      return nil if clock.null?

      DomainClock.new(clock)
    end

    def next_clock_event(cancel, domain)
      out = Probe.slot
      Probe.check(F[:session_next_clock_event].call(@handle, cancel, out))
      event = ClockEvent.new(Probe.read_pointer(out))
      raise 'a clock event arrived for another domain' unless event.domain == domain

      event
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

  # Cancels a clock wait from another thread, then lets a deadline end another. The session follows
  # no clock yet, so nothing but its token ends either wait.
  def self.check_clock_cancellation(session, domain)
    cancel = cancel()
    waiter = Thread.new { failure_of { session.next_clock_event(cancel, domain) } }
    sleep 0.1
    F[:cancel_trigger].call(cancel)
    raise 'a cancelled clock wait did not report CANCELLED' unless waiter.join(30)&.value&.kind == ERROR_CANCELLED

    expired = failure_of { session.next_clock_event(cancel(50), domain) }
    raise 'an expired clock wait did not report DEADLINE' unless expired.kind == ERROR_DEADLINE
  end

  # What the probe has read about the domain's clock: the generation of the newest state, its
  # mapping while it is paced, and whether the session holding the attachment ended since. Every
  # event is held to what was read before it, and a read of the clock taken right after it is held
  # to be no older.
  class FollowedClock
    def initialize(session, domain, clock)
      @session = session
      @domain = domain
      @generation = clock.generation
      @paced = clock
      @interrupted = false
    end

    def clock
      raise 'the followed clock is not paced' if @paced.nil?

      @paced
    end

    # The first tick of the followed generation. A state reporting that generation again is taken
    # on the way; one of another generation fails the probe.
    def first_tick(deadline)
      generation = @generation
      loop do
        event = follow(deadline)
        return event if event.kind == 'TICK'
        next if event.kind == 'STATE' && @generation == generation

        raise "the clock reported #{event.kind} before the first tick of generation #{generation}"
      end
    end

    # The paced state of a generation after the followed one. The followed generation's ticks and
    # the states before the new paced one are taken on the way.
    def next_generation(deadline)
      previous = @generation
      loop do
        event = follow(deadline)
        return event if event.kind == 'STATE' && @generation > previous && !@paced.nil?
        raise "the clock reported #{event.kind} before a generation after #{previous}" unless %w[TICK STATE].include?(event.kind)
      end
    end

    # Waits for the interruption of the attachment. The followed generation's ticks and states are
    # taken on the way.
    def interruption(deadline)
      generation = @generation
      loop do
        event = follow(deadline)
        return if event.kind == 'INTERRUPTED'
        next if event.kind == 'TICK' || (event.kind == 'STATE' && @generation == generation)

        raise "the clock reported #{event.kind} before the interruption"
      end
    end

    # The paced state the restored attachment reports. A refused restoration, which the session
    # repeats, and a clock reported uninstalled are taken on the way.
    def restored(deadline)
      loop do
        event = follow(deadline)
        return event if event.kind == 'STATE' && !@paced.nil?
      end
    end

    private

    # The next event about the domain, held to what the probe read before it.
    def follow(deadline)
      event = @session.next_clock_event(deadline, @domain)
      case event.kind
      when 'STATE' then observe(event)
      when 'TICK' then check_tick(event)
      when 'INTERRUPTED' then @interrupted = true
      when 'ENDED' then raise 'the server ended the attachment'
      end
      event
    end

    def read
      read = @session.domain_clock(@domain)
      raise 'the session follows no clock of the domain after an event about it' if read.nil?

      read
    end

    def observe(event)
      generation = event.generation
      raise 'a state went back to an earlier generation' if generation < @generation

      state = event.state
      paced = state == CLOCK_PACED ? event.paced : nil
      held = read
      raise 'a read of the clock is older than the state the probe took' if held.generation < generation

      if held.generation == generation
        raise 'a read of the clock differs from the state of its generation' if held.state != state
        raise 'a read of the clock differs from the mapping of its generation' if !paced.nil? && held.paced != paced
      end
      @generation = generation
      @paced = paced
      @interrupted = false
    end

    def check_tick(event)
      raise 'a tick arrived before the restored attachment reported its clock' if @interrupted

      tick = event.tick
      clock.check_tick(tick)
      held = read
      raise 'a read of the clock is older than the tick the probe took' if held.generation < tick.generation

      held_id = held.tick_id
      return unless held.generation == tick.generation && !held_id.nil? && held_id < tick.id

      raise 'a read of the clock holds an older tick than the probe took'
    end
  end

  # Reports the paced state a STATE event reports and the first tick after it.
  def self.report_state_and_first_tick(followed, state, deadline, domain)
    report(state.paced.line('STATE', domain))
    report(followed.clock.tick_line(followed.first_tick(deadline).tick, domain))
  end

  # Attaches to the domain's running clock and reads the clock the attach reported before its first
  # tick, then follows the generation the scenario's STOP and START begin and the attachment
  # restored after the scenario ends the session, and detaches.
  def self.run_clock(session, domain)
    check_clock_cancellation(session, domain)
    report("ATTACHED #{disposition(session.execute('ATTACH DOMAIN CLOCK;'))}")

    # The clock the attach reported, read before any event about the attachment.
    read = session.domain_clock(domain)
    raise 'the session follows no clock after its attach completed' if read.nil?
    raise 'the attach reported a clock other than the running paced one' unless read.state == CLOCK_PACED

    clock = read.paced
    reported_clock = clock.line('CLOCK', domain)
    report(reported_clock)
    report(clock.projection_line(read, domain))

    followed = FollowedClock.new(session, domain, clock)
    tick = followed.first_tick(cancel(120_000))
    reported_tick = clock.tick_line(tick.tick, domain)
    report(reported_tick)

    # Keep a second reference to the read and the tick and release the first ones on another
    # thread, so both must read the same on the second alone.
    retained_read = read.retain
    retained_tick = tick.retain
    Thread.new do
      read.release
      tick.release
    end.join
    clock_again = retained_read.paced
    if clock_again.line('CLOCK', domain) != reported_clock || clock_again.tick_line(retained_tick.tick, domain) != reported_tick
      raise 'a retained clock or tick reads differently than it did'
    end

    # The scenario stops the domain and starts it again at another origin and rate.
    deadline = cancel(120_000)
    report_state_and_first_tick(followed, followed.next_generation(deadline), deadline, domain)

    # The scenario ends the session, and the binding attaches the clock again on the next one.
    deadline = cancel(120_000)
    followed.interruption(deadline)
    report("INTERRUPTED domain=#{domain}")
    report_state_and_first_tick(followed, followed.restored(deadline), deadline, domain)

    report("DETACHED #{disposition(session.execute('DETACH DOMAIN CLOCK;'))}")
    report('CHECKS ok')
    session.close
    report('PASS')
  end

  def self.main
    domain = ENV.fetch('NERVIX_PROBE_DOMAIN')
    session = Session.new(ENV.fetch('NERVIX_PROBE_GRPC_URI'), domain,
                          ENV.fetch('NERVIX_PROBE_USERNAME'), ENV.fetch('NERVIX_PROBE_PASSWORD'))
    return run_clock(session, domain) if ARGV == ['clock']

    relay = ENV.fetch('NERVIX_PROBE_RELAY')
    subscription = ENV.fetch('NERVIX_PROBE_SUBSCRIPTION')
    expected_rows = Integer(ENV.fetch('NERVIX_PROBE_ROWS'))

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
