# frozen_string_literal: true

# The Ruby probe of the shared Rust binding, through Fiddle.
#
# It prints the same conformance report as every other probe. Every event reference is a
# Fiddle::Pointer whose free function is the binding's release, so the garbage collector releases a
# reference nothing reaches; the probe also releases references explicitly. Frames are read in
# place through the pointer the binding lends, and columns are copied into Fiddle buffers one call
# per column. Run with the `clock` argument, it attaches to the domain's running clock instead,
# reads the clock the attach reported before its first tick, follows the generation a STOP and
# START begin and the attachment restored after its session ends, and detaches. Run with the `io`
# argument, it publishes typed batches through a client ingestor and reads, retries, rejects and
# acknowledges their output through a client emitter, across the session the scenario cuts.

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
    attr_reader :handle

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

  # ---- Producers and consumers ------------------------------------------------------------------

  WAIT_MILLIS = 120_000
  EXPIRING_MILLIS = 200
  PRODUCER_BATCHES = 2
  CONSUMER_BATCHES = 4
  ENDPOINT_BYTES = 1_048_576
  SCRIBBLE = 0xaa
  ERROR_INVALID_ARGUMENT = 1
  ERROR_REJECTED = 5
  ERROR_INTERRUPTED = 12
  OPEN_SCHEMA_MISMATCH = 6
  ENDPOINT_ACTIVE = 1
  ENDPOINT_CLOSED = 5
  STATES = [nil, 'active', 'interrupted', 'restoring', 'reopen_required', 'closed'].freeze
  SETTLEMENTS = [nil, 'confirmed', 'stale_reference', 'wrong_consumer', 'invalid_reason',
                 'consumer_ended'].freeze
  REFUSALS = [nil, 'invalid_batch', 'suspended', 'busy', 'draining', 'producer_ended',
              'credit_exceeded'].freeze
  DEFECTS = [nil, 'malformed', 'unexpected_message', 'compressed', 'schema_mismatch',
             'not_one_batch', 'too_many_rows', 'too_large', 'invalid_data'].freeze
  FAILURES = [nil, 'ack_timed_out', 'rejected'].freeze
  UNCERTAINTIES = [nil, 'interrupted', 'owner_lost', 'session_lost'].freeze
  U32 = Fiddle::TYPE_UINT32_T

  E = {
    error_open_refusal: function('nx_error_open_refusal', [VOIDP, VOIDP], BOOL),
    fields_new: function('nx_fields_new', [], VOIDP),
    fields_add: function('nx_fields_add', [VOIDP, VOIDP, SIZE, INT, U32, BOOL, BOOL], VOIDP),
    fields_element: function('nx_fields_element', [VOIDP, INT, U32], VOIDP),
    fields_free: function('nx_fields_free', [VOIDP], Fiddle::TYPE_VOID),
    schema_field_levels: function('nx_schema_field_levels', [VOIDP, INT, SIZE, VOIDP], VOIDP),
    schema_field_level: function('nx_schema_field_level', [VOIDP, INT, SIZE, SIZE, VOIDP, VOIDP], VOIDP),
    open_ingestor: function('nx_session_open_ingestor',
                            [VOIDP, VOIDP, SIZE, VOIDP, SIZE, VOIDP, U32, U64, VOIDP, VOIDP], VOIDP),
    subscribe_emitter: function('nx_session_subscribe_emitter',
                                [VOIDP, VOIDP, SIZE, VOIDP, SIZE, VOIDP, U32, U64, VOIDP, VOIDP], VOIDP),
    producer_schema: function('nx_producer_schema', [VOIDP, VOIDP], VOIDP),
    producer_generation: function('nx_producer_generation', [VOIDP], U64),
    producer_grant: function('nx_producer_grant', [VOIDP, VOIDP, VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    producer_policy: function('nx_producer_policy', [VOIDP, VOIDP, VOIDP, VOIDP, VOIDP, VOIDP],
                              Fiddle::TYPE_VOID),
    producer_admission: function('nx_producer_admission', [VOIDP], INT),
    producer_state: function('nx_producer_state', [VOIDP], INT),
    producer_submit: function('nx_producer_submit', [VOIDP, VOIDP, VOIDP, VOIDP], VOIDP),
    producer_submit_ipc: function('nx_producer_submit_ipc', [VOIDP, VOIDP, SIZE, VOIDP, VOIDP], VOIDP),
    producer_rejoin: function('nx_producer_rejoin', [VOIDP, U64, VOIDP, VOIDP], VOIDP),
    producer_pending: function('nx_producer_pending', [VOIDP, VOIDP, VOIDP, SIZE, VOIDP], VOIDP),
    producer_close: function('nx_producer_close', [VOIDP, VOIDP], VOIDP),
    producer_free: function('nx_producer_free', [VOIDP], Fiddle::TYPE_VOID),
    outcome_result: function('nx_submission_outcome_result', [VOIDP], INT),
    outcome_refusal: function('nx_submission_outcome_refusal', [VOIDP, VOIDP], VOIDP),
    outcome_defect: function('nx_submission_outcome_defect', [VOIDP, VOIDP], VOIDP),
    outcome_failure: function('nx_submission_outcome_failure', [VOIDP, VOIDP], VOIDP),
    outcome_uncertainty: function('nx_submission_outcome_uncertainty', [VOIDP, VOIDP], VOIDP),
    submission_free: function('nx_submission_outcome_free', [VOIDP], Fiddle::TYPE_VOID),
    consumer_schema: function('nx_consumer_schema', [VOIDP, VOIDP], VOIDP),
    consumer_generation: function('nx_consumer_generation', [VOIDP], U64),
    consumer_grant: function('nx_consumer_grant', [VOIDP, VOIDP, VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    consumer_policy: function('nx_consumer_policy', [VOIDP, VOIDP, VOIDP, VOIDP, VOIDP, VOIDP],
                              Fiddle::TYPE_VOID),
    consumer_state: function('nx_consumer_state', [VOIDP], INT),
    consumer_next: function('nx_consumer_next', [VOIDP, VOIDP, VOIDP], VOIDP),
    consumer_close: function('nx_consumer_close', [VOIDP, VOIDP], VOIDP),
    consumer_free: function('nx_consumer_free', [VOIDP], Fiddle::TYPE_VOID),
    delivery_identity: function('nx_delivery_identity', [VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    delivery_reference: function('nx_delivery_reference', [VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    delivery_relay: function('nx_delivery_source_relay', [VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    delivery_fingerprint: function('nx_delivery_branch_fingerprint', [VOIDP, VOIDP, VOIDP], BOOL),
    delivery_members: function('nx_delivery_members', [VOIDP], U32),
    delivery_ipc: function('nx_delivery_ipc', [VOIDP, VOIDP, VOIDP], Fiddle::TYPE_VOID),
    delivery_batch: function('nx_delivery_batch', [VOIDP, VOIDP], VOIDP),
    delivery_ack: function('nx_delivery_ack', [VOIDP, VOIDP, VOIDP], VOIDP),
    delivery_retry: function('nx_delivery_retry', [VOIDP, VOIDP, VOIDP], VOIDP),
    delivery_reject: function('nx_delivery_reject', [VOIDP, VOIDP, SIZE, VOIDP, VOIDP], VOIDP),
    delivery_retain: function('nx_delivery_retain', [VOIDP], VOIDP),
    batch_row_count: function('nx_batch_row_count', [VOIDP], SIZE),
    batch_ipc: function('nx_batch_ipc', [VOIDP, VOIDP, VOIDP], VOIDP),
    batch_cells: function('nx_batch_cells', [VOIDP, SIZE, SIZE, VOIDP], VOIDP),
    batch_states: function('nx_batch_states', [VOIDP, SIZE, VOIDP, SIZE], VOIDP),
    batch_offsets: function('nx_batch_offsets', [VOIDP, SIZE, SIZE, VOIDP, SIZE], VOIDP),
    batch_fixed: function('nx_batch_fixed', [VOIDP, SIZE, SIZE, VOIDP, SIZE], VOIDP),
    batch_varlen: function('nx_batch_varlen', [VOIDP, SIZE, SIZE, VOIDP, SIZE, VOIDP, SIZE, VOIDP], VOIDP),
    batch_retain: function('nx_batch_retain', [VOIDP], VOIDP),
    builder_new: function('nx_batch_builder_new', [VOIDP, SIZE, VOIDP], VOIDP),
    builder_states: function('nx_batch_builder_states', [VOIDP, SIZE, VOIDP, SIZE], VOIDP),
    builder_offsets: function('nx_batch_builder_offsets', [VOIDP, SIZE, SIZE, VOIDP, SIZE], VOIDP),
    builder_fixed: function('nx_batch_builder_fixed', [VOIDP, SIZE, SIZE, VOIDP, SIZE], VOIDP),
    builder_varlen: function('nx_batch_builder_varlen', [VOIDP, SIZE, SIZE, VOIDP, SIZE, VOIDP, SIZE], VOIDP),
    builder_finish: function('nx_batch_builder_finish', [VOIDP, VOIDP], VOIDP),
    builder_free: function('nx_batch_builder_free', [VOIDP], Fiddle::TYPE_VOID)
  }.freeze

  DELIVERY_RELEASE = LIBRARY['nx_delivery_release']
  BATCH_RELEASE = LIBRARY['nx_batch_release']

  # The input schema of the probe's ingestor, in declared order: each field's name, the [type,
  # length] of every level of its type, and whether it is nullable and sensitive.
  INPUT_FIELDS = [
    ['id', [['U32', 0]], false, false],
    ['tenant', [['STRING', 0]], false, false],
    ['u8v', [['U8', 0]], false, false],
    ['i8v', [['I8', 0]], false, false],
    ['u16v', [['U16', 0]], false, false],
    ['i16v', [['I16', 0]], false, false],
    ['u32v', [['U32', 0]], false, false],
    ['i32v', [['I32', 0]], false, false],
    ['u64v', [['U64', 0]], false, false],
    ['i64v', [['I64', 0]], false, false],
    ['f32v', [['F32', 0]], false, false],
    ['f64v', [['F64', 0]], false, false],
    ['flag', [['BOOL', 0]], false, false],
    ['text', [['STRING', 0]], true, false],
    ['raw', [['BYTES', 0]], true, false],
    ['at', [['DATETIME', 0]], false, false],
    ['maybe', [['I64', 0]], true, false],
    ['tags', [['LIST', 0], ['STRING', 0]], false, false],
    ['grid', [['FIXED_LIST', 2], ['FIXED_LIST', 2], ['I16', 0]], false, false],
    ['spans', [['LIST', 0], ['FIXED_LIST', 2], ['DATETIME', 0]], true, false],
    ['secret', [['STRING', 0]], false, true]
  ].freeze
  ECHO_FIELD = ['echo', [['U32', 0]], false, false].freeze
  I64_MIN = -(1 << 63)
  I64_MAX = (1 << 63) - 1

  # A row of `tenant` whose every other value is the zero of its type.
  def self.plain_row(id, tenant)
    { 'id' => id, 'tenant' => tenant, 'u8v' => 0, 'i8v' => 0, 'u16v' => 0, 'i16v' => 0, 'u32v' => 0,
      'i32v' => 0, 'u64v' => 0, 'i64v' => 0, 'f32v' => 0, 'f64v' => 0, 'flag' => false, 'text' => nil,
      'raw' => nil, 'at' => 0, 'maybe' => nil, 'tags' => [], 'grid' => [[0, 0], [0, 0]], 'spans' => nil,
      'secret' => 's' }
  end

  # The typed rows of the first batch.
  def self.typed_rows
    [
      plain_row(1, 'acme').merge(
        'u8v' => 255, 'i8v' => 127, 'u16v' => 65_535, 'i16v' => 32_767, 'u32v' => 4_294_967_295,
        'i32v' => 2_147_483_647, 'u64v' => (1 << 64) - 1, 'i64v' => I64_MAX, 'f32v' => 0x7f7fffff,
        'f64v' => 0x7fefffffffffffff, 'flag' => true, 'text' => "héllo 世界 \u{1f680}".b,
        'raw' => "\x00\xff\xfe\x80\x00".b, 'at' => I64_MAX, 'tags' => ['a', '', "héllo"],
        'grid' => [[1, -2], [32_767, -32_768]], 'spans' => [[I64_MIN, I64_MAX]], 'secret' => 's1'
      ),
      plain_row(2, 'acme').merge(
        'i8v' => -128, 'i16v' => -32_768, 'i32v' => -2_147_483_648, 'i64v' => I64_MIN,
        'f32v' => 0x80000000, 'f64v' => 0x0000000000000001, 'text' => "a\x00b".b, 'raw' => ''.b,
        'at' => I64_MIN, 'maybe' => 0, 'secret' => 's2'
      ),
      plain_row(3, 'acme').merge(
        'u8v' => 1, 'i8v' => -1, 'u16v' => 1, 'i16v' => -1, 'u32v' => 1, 'i32v' => -1,
        'u64v' => 9_007_199_254_740_993, 'i64v' => -9_007_199_254_740_993, 'f32v' => 0x3fc00000,
        'f64v' => 0x3fb999999999999a, 'flag' => true, 'at' => 1, 'maybe' => 9_007_199_254_740_992,
        'tags' => ['x'], 'grid' => [[-1, 1], [2, -2]], 'spans' => [], 'secret' => 's3'
      )
    ]
  end

  PACKING = { 'U8' => 'C', 'I8' => 'c', 'U16' => 'S', 'I16' => 's', 'U32' => 'L', 'I32' => 'l',
              'U64' => 'Q', 'I64' => 'q', 'F32' => 'L', 'F64' => 'Q', 'BOOL' => 'C',
              'DATETIME' => 'q' }.freeze
  SCALAR_KINDS = { 'id' => 'U32', 'echo' => 'U32', 'u8v' => 'U8', 'i8v' => 'I8', 'u16v' => 'U16',
                   'i16v' => 'I16', 'u32v' => 'U32', 'i32v' => 'I32', 'u64v' => 'U64',
                   'i64v' => 'I64', 'f32v' => 'F32', 'f64v' => 'F64', 'flag' => 'BOOL',
                   'at' => 'DATETIME' }.freeze

  def self.offsets_of(lengths)
    lengths.each_with_object([0]) { |length, offsets| offsets << (offsets.last + length) }
  end

  # The column `name` of `rows`, as a host lays it out.
  def self.host_column(name, rows)
    column = { states: nil, offsets: nil, leaf: 0, fixed: nil, varlen: nil }
    varlen = lambda do |values|
      column[:varlen] = [offsets_of(values.map(&:bytesize)), values.join.b]
    end
    if SCALAR_KINDS.key?(name)
      source = name == 'echo' ? 'id' : name
      packing = PACKING.fetch(SCALAR_KINDS.fetch(name))
      column[:fixed] = rows.map { |row| [row[source] == true ? 1 : (row[source] == false ? 0 : row[source])].pack(packing) }.join.b
    elsif %w[tenant secret].include?(name)
      varlen.call(rows.map { |row| row[name].b })
    elsif %w[text raw].include?(name)
      column[:states] = rows.map { |row| row[name].nil? ? CELL_NULL : CELL_VALUE }.pack('C*')
      varlen.call(rows.map { |row| (row[name] || '').b })
    elsif name == 'maybe'
      column[:states] = rows.map { |row| row['maybe'].nil? ? CELL_NULL : CELL_VALUE }.pack('C*')
      column[:fixed] = rows.map { |row| row['maybe'] || 0 }.pack('q*')
    elsif name == 'tags'
      column[:leaf] = 1
      column[:offsets] = offsets_of(rows.map { |row| row['tags'].size })
      varlen.call(rows.flat_map { |row| row['tags'].map(&:b) })
    elsif name == 'grid'
      column[:leaf] = 2
      column[:fixed] = rows.flat_map { |row| row['grid'].flatten }.pack('s*')
    elsif name == 'spans'
      column[:leaf] = 2
      column[:states] = rows.map { |row| row['spans'].nil? ? CELL_NULL : CELL_VALUE }.pack('C*')
      column[:offsets] = offsets_of(rows.map { |row| (row['spans'] || []).size })
      column[:fixed] = rows.flat_map { |row| (row['spans'] || []).flatten }.pack('q*')
    else
      raise "the probe holds no column named #{name}"
    end
    column
  end

  def self.fields_handle(specs)
    handle = E[:fields_new].call
    specs.each do |name, levels, nullable, sensitive|
      kind, length = levels.first
      check(E[:fields_add].call(handle, name, name.bytesize, TYPES.index(kind), length, nullable, sensitive))
      levels.drop(1).each do |level_kind, level_length|
        check(E[:fields_element].call(handle, TYPES.index(level_kind), level_length))
      end
    end
    handle
  end

  # A field's type in NSPL's spelling, with nested fixed-size lists as dimensions.
  def self.type_text(levels)
    kind = levels.first.first
    return "VEC<#{type_text(levels.drop(1))}>" if kind == 'LIST'

    if kind == 'FIXED_LIST'
      dimensions = levels.take_while { |level_kind, _| level_kind == 'FIXED_LIST' }.map(&:last)
      return "ARRAY<#{type_text(levels.drop(dimensions.size))}, #{dimensions.join(', ')}>"
    end
    kind
  end

  def self.reported_fields(schema)
    count = slot(SIZE_BYTES)
    check(F[:schema_field_count].call(schema, PART_ROWS, count))
    Array.new(read_size(count)) do |index|
      name = slot
      name_len = slot(SIZE_BYTES)
      type = slot(4)
      nullable = slot(1)
      sensitive = slot(1)
      check(F[:schema_field].call(schema, PART_ROWS, index, name, name_len, type, nullable, sensitive))
      level_count = slot(SIZE_BYTES)
      check(E[:schema_field_levels].call(schema, PART_ROWS, index, level_count))
      levels = Array.new(read_size(level_count)) do |level|
        level_type = slot(4)
        level_length = slot(4)
        check(E[:schema_field_level].call(schema, PART_ROWS, index, level, level_type, level_length))
        [TYPES.fetch(level_type[0, 4].unpack1('l')), level_length[0, 4].unpack1('L')]
      end
      raise "a field's first level differs from its type" unless levels.first.first == TYPES.fetch(type[0, 4].unpack1('l'))

      { name: borrowed(name, name_len), levels: levels, nullable: nullable[0, 1].unpack1('C') != 0,
        sensitive: sensitive[0, 1].unpack1('C') != 0 }
    end
  end

  def self.reported_line(prefix, field)
    "#{prefix} #{field[:name]} #{type_text(field[:levels])} #{field[:nullable] ? 'nullable' : 'required'} " \
      "#{field[:sensitive] ? 'sensitive' : 'public'}"
  end

  # A Fiddle buffer holding `bytes`, which the binding copies.
  def self.buffer(bytes)
    pointer = slot([bytes.bytesize, 1].max)
    pointer[0, bytes.bytesize] = bytes unless bytes.empty?
    pointer
  end

  # One reference to a batch. The collector releases it when nothing reaches it any more.
  class Batch
    attr_reader :handle

    def initialize(handle)
      handle.free = BATCH_RELEASE
      @handle = handle
    end

    def retain = Batch.new(E[:batch_retain].call(@handle))

    def release = @handle.call_free

    # Builds a batch of `rows` for `schema`, writing over every buffer once the binding copied it.
    def self.build(schema, rows)
      out = Probe.slot
      Probe.check(E[:builder_new].call(schema, rows.size, out))
      builder = Probe.read_pointer(out)
      begin
        Probe.reported_fields(schema).each_with_index do |field, index|
          column = Probe.host_column(field[:name], rows)
          unless column[:states].nil?
            states = Probe.buffer(column[:states])
            Probe.check(E[:builder_states].call(builder, index, states, column[:states].bytesize))
            states[0, column[:states].bytesize] = [SCRIBBLE].pack('C') * column[:states].bytesize
          end
          unless column[:offsets].nil?
            offsets = Probe.buffer(column[:offsets].pack('Q*'))
            Probe.check(E[:builder_offsets].call(builder, index, 0, offsets, column[:offsets].size))
            offsets[0, column[:offsets].size * 8] = "\xff".b * (column[:offsets].size * 8)
          end
          if column[:varlen].nil?
            values = Probe.buffer(column[:fixed])
            Probe.check(E[:builder_fixed].call(builder, index, column[:leaf], values, column[:fixed].bytesize))
            values[0, column[:fixed].bytesize] = [SCRIBBLE].pack('C') * column[:fixed].bytesize unless column[:fixed].empty?
          else
            value_offsets, data = column[:varlen]
            offsets = Probe.buffer(value_offsets.pack('Q*'))
            values = Probe.buffer(data)
            Probe.check(E[:builder_varlen].call(builder, index, column[:leaf], offsets, value_offsets.size, values,
                                                data.bytesize))
            offsets[0, value_offsets.size * 8] = "\xff".b * (value_offsets.size * 8)
            values[0, data.bytesize] = [SCRIBBLE].pack('C') * data.bytesize unless data.empty?
          end
        end
        batch = Probe.slot
        Probe.check(E[:builder_finish].call(builder, batch))
        Batch.new(Probe.read_pointer(batch))
      ensure
        E[:builder_free].call(builder)
      end
    end

    def stream
      ipc = Probe.slot
      ipc_len = Probe.slot(SIZE_BYTES)
      Probe.check(E[:batch_ipc].call(@handle, ipc, ipc_len))
      Probe.borrowed(ipc, ipc_len)
    end

    def cells(column, level)
      cells = Probe.slot(SIZE_BYTES)
      Probe.check(E[:batch_cells].call(@handle, column, level, cells))
      Probe.read_size(cells)
    end

    # Reads one column in one call per level of its type and renders each row.
    def column(index, field)
      rows = cells(index, 0)
      states = Probe.slot([rows, 1].max)
      Probe.check(E[:batch_states].call(@handle, index, states, rows))
      states = states[0, rows].unpack('C*')
      levels = field[:levels]
      innermost = levels.size - 1
      lists = levels.take(innermost).each_with_index.map do |(kind, length), level|
        next length unless kind == 'LIST'

        count = cells(index, level) + 1
        offsets = Probe.slot(count * 8)
        Probe.check(E[:batch_offsets].call(@handle, index, level, offsets, count))
        offsets[0, count * 8].unpack('Q*')
      end
      kind = levels.last.first
      count = cells(index, innermost)
      leaves = if %w[STRING BYTES].include?(kind)
                 offsets = Probe.slot((count + 1) * 8)
                 data_len = Probe.slot(SIZE_BYTES)
                 Probe.check(E[:batch_varlen].call(@handle, index, innermost, offsets, count + 1, nil, 0, data_len))
                 needed = Probe.read_size(data_len)
                 data = Probe.slot([needed, 1].max)
                 Probe.check(E[:batch_varlen].call(@handle, index, innermost, offsets, count + 1, data, needed, data_len))
                 bounds = offsets[0, (count + 1) * 8].unpack('Q*')
                 bytes = needed.zero? ? ''.b : data[0, needed]
                 prefix = kind == 'STRING' ? 'str' : 'bytes'
                 Array.new(count) { |cell| "#{prefix}:#{bytes.byteslice(bounds[cell], bounds[cell + 1] - bounds[cell]).unpack1('H*')}" }
               else
                 width, directive, prefix = FIXED.fetch(kind)
                 values = Probe.slot([count * width, 1].max)
                 Probe.check(E[:batch_fixed].call(@handle, index, innermost, values, count * width)) if count.positive?
                 (count.zero? ? [] : values[0, count * width].unpack("#{directive}*")).map do |value|
                   case kind
                   when 'F32' then format('f32:%08x', value)
                   when 'F64' then format('f64:%016x', value)
                   when 'BOOL' then "bool:#{value == 1}"
                   else "#{prefix}:#{value}"
                   end
                 end
               end
      render = lambda do |level, cell|
        next leaves.fetch(cell) if level == lists.size

        shape = lists[level]
        range = shape.is_a?(Array) ? (shape[cell]...shape[cell + 1]) : ((cell * shape)...((cell + 1) * shape))
        "[#{range.map { |element| render.call(level + 1, element) }.join(',')}]"
      end
      states.each_with_index.map { |state, row| state == CELL_NULL ? 'null' : render.call(0, row) }
    end

    def rows(fields)
      columns = fields.each_with_index.map { |field, index| column(index, field) }
      Array.new(E[:batch_row_count].call(@handle)) do |row|
        "ROW #{fields.each_with_index.map { |field, index| "#{field[:name]}=#{columns[index][row]}" }.join(' ')}"
      end
    end

    def ids(fields)
      index = fields.index { |field| field[:name] == 'id' }
      column(index, fields[index]).map { |value| Integer(value.delete_prefix('u32:')) }
    end
  end

  # One reference to a delivery. The collector releases it when nothing reaches it any more.
  class Delivery
    attr_reader :handle

    def initialize(handle)
      handle.free = DELIVERY_RELEASE
      @handle = handle
    end

    def retain = Delivery.new(E[:delivery_retain].call(@handle))

    def release = @handle.call_free

    def read(accessor)
      data = Probe.slot
      data_len = Probe.slot(SIZE_BYTES)
      E[accessor].call(@handle, data, data_len)
      Probe.borrowed(data, data_len)
    end

    def summary
      fingerprint = Probe.slot
      fingerprint_len = Probe.slot(SIZE_BYTES)
      branched = E[:delivery_fingerprint].call(@handle, fingerprint, fingerprint_len)
      branch = branched ? Probe.read_size(fingerprint_len).to_s : 'none'
      "DELIVERY relay=#{read(:delivery_relay)} members=#{E[:delivery_members].call(@handle)} branch=#{branch} " \
        "identity=#{read(:delivery_identity).bytesize} reference=#{read(:delivery_reference).bytesize}"
    end

    def batch
      out = Probe.slot
      Probe.check(E[:delivery_batch].call(@handle, out))
      Batch.new(Probe.read_pointer(out))
    end

    def settle(accessor)
      deadline = Probe.cancel(WAIT_MILLIS)
      settlement = Probe.slot(4)
      Probe.check(E[accessor].call(@handle, deadline, settlement))
      SETTLEMENTS.fetch(settlement[0, 4].unpack1('l'))
    end

    def reject(reason)
      deadline = Probe.cancel(WAIT_MILLIS)
      settlement = Probe.slot(4)
      Probe.check(E[:delivery_reject].call(@handle, reason, reason.bytesize, deadline, settlement))
      SETTLEMENTS.fetch(settlement[0, 4].unpack1('l'))
    end
  end

  def self.failure_kind
    yield
    raise 'a call succeeded where it had to fail'
  rescue Failure => e
    e.kind
  end

  # The producer and consumer exercise.
  class Endpoints
    def initialize(session, domain)
      @session = session
      @domain = domain
      @ingestor = ENV.fetch('NERVIX_PROBE_INGESTOR')
      @emitter = ENV.fetch('NERVIX_PROBE_EMITTER')
      @failures = []
    end

    def expect(holds, what)
      @failures << what unless holds
    end

    def open_producer(fields, batches, asked, out)
      E[:open_ingestor].call(@session.handle, @domain, @domain.bytesize, @ingestor, @ingestor.bytesize, fields,
                             batches, asked, nil, out)
    end

    def open_consumer(fields, out)
      E[:subscribe_emitter].call(@session.handle, @domain, @domain.bytesize, @emitter, @emitter.bytesize, fields,
                                 CONSUMER_BATCHES, ENDPOINT_BYTES, nil, out)
    end

    def expect_refused(error, what)
      refusal = Probe.slot(4)
      if error.null? || F[:error_kind].call(error) != ERROR_REJECTED ||
         !E[:error_open_refusal].call(error, refusal) || refusal[0, 4].unpack1('l') != OPEN_SCHEMA_MISMATCH
        raise 'an open with another schema was not refused exactly'
      end

      F[:error_free].call(error)
      Probe.report("REFUSED #{what} schema mismatch")
    end

    def opened(error, out)
      Probe.check(error)
      Probe.read_pointer(out)
    end

    def submit(producer, batch, cancel = nil)
      submission = Probe.slot(8)
      Probe.check(E[:producer_submit].call(producer, batch.handle, cancel, submission))
      submission[0, 8].unpack1('Q')
    end

    def outcome(producer, submission)
      deadline = Probe.cancel(WAIT_MILLIS)
      out = Probe.slot
      Probe.check(E[:producer_rejoin].call(producer, submission, deadline, out))
      outcome = Probe.read_pointer(out)
      cause = Probe.slot(4)
      begin
        case E[:outcome_result].call(outcome)
        when 2 then 'completed'
        when 1
          Probe.check(E[:outcome_refusal].call(outcome, cause))
          refusal = cause[0, 4].unpack1('l')
          text = "not_admitted #{REFUSALS.fetch(refusal)}"
          if refusal == 1
            Probe.check(E[:outcome_defect].call(outcome, cause))
            text += " #{DEFECTS.fetch(cause[0, 4].unpack1('l'))}"
          end
          text
        when 3
          Probe.check(E[:outcome_failure].call(outcome, cause))
          "processing_failed #{FAILURES.fetch(cause[0, 4].unpack1('l'))}"
        else
          Probe.check(E[:outcome_uncertainty].call(outcome, cause))
          "outcome_unknown #{UNCERTAINTIES.fetch(cause[0, 4].unpack1('l'))}"
        end
      ensure
        E[:submission_free].call(outcome)
      end
    end

    def next_delivery(consumer, cancel = Probe.cancel(WAIT_MILLIS))
      out = Probe.slot
      Probe.check(E[:consumer_next].call(consumer, cancel, out))
      Delivery.new(Probe.read_pointer(out))
    end

    def policy(accessor, handle)
      window = Probe.slot(4)
      outstanding = Probe.slot(8)
      timeout = Probe.slot(8)
      backoff = Probe.slot(8)
      maximum = Probe.slot(8)
      E[accessor].call(handle, window, outstanding, timeout, backoff, maximum)
      "window=#{window[0, 4].unpack1('l') == 1 ? 'sequential' : 'parallel'}/#{outstanding[0, 8].unpack1('Q')} " \
        "ack_timeout=#{timeout[0, 8].unpack1('Q')}"
    end

    def grant(accessor, handle)
      batches = Probe.slot(4)
      granted = Probe.slot(8)
      max_rows = Probe.slot(4)
      max_bytes = Probe.slot(8)
      E[accessor].call(handle, batches, granted, max_rows, max_bytes)
      [batches[0, 4].unpack1('L'), granted[0, 8].unpack1('Q'), max_rows[0, 4].unpack1('L'),
       max_bytes[0, 8].unpack1('Q')]
    end

    def run
      mismatched_specs = INPUT_FIELDS.map do |name, levels, nullable, sensitive|
        [name, levels, nullable, name == 'secret' ? false : sensitive]
      end
      input_fields = Probe.fields_handle(INPUT_FIELDS)
      output_fields = Probe.fields_handle(INPUT_FIELDS + [ECHO_FIELD])
      mismatched = Probe.fields_handle(mismatched_specs)
      out = Probe.slot

      # An open whose expected fields differ from the endpoint's is refused exactly.
      expect_refused(open_producer(mismatched, PRODUCER_BATCHES, ENDPOINT_BYTES, out), 'producer')
      expect_refused(open_consumer(input_fields, out), 'consumer')
      E[:fields_free].call(mismatched)

      consumer = opened(open_consumer(output_fields, out), out)
      producer = opened(open_producer(input_fields, PRODUCER_BATCHES, ENDPOINT_BYTES, out), out)
      output_schema = opened(E[:consumer_schema].call(consumer, out), out)
      input_schema = opened(E[:producer_schema].call(producer, out), out)
      output = Probe.reported_fields(output_schema)
      inputs = Probe.reported_fields(input_schema)

      batches, granted, max_rows, max_bytes = grant(:consumer_grant, consumer)
      Probe.report("CONSUMER generation=#{E[:consumer_generation].call(consumer)} " \
                   "state=#{STATES.fetch(E[:consumer_state].call(consumer))} #{policy(:consumer_policy, consumer)} " \
                   "credit=#{batches}/#{granted} max=#{max_rows}/#{max_bytes}")
      output.each { |field| Probe.report(Probe.reported_line('OUTPUT FIELD', field)) }
      batches, granted, max_rows, max_bytes = grant(:producer_grant, producer)
      expect(max_rows.positive? && max_bytes.positive? && max_bytes <= granted, "the producer's limits fit its grant")
      admission = E[:producer_admission].call(producer) == 1 ? 'open' : 'suspended'
      Probe.report("PRODUCER generation=#{E[:producer_generation].call(producer)} " \
                   "state=#{STATES.fetch(E[:producer_state].call(producer))} admission=#{admission} " \
                   "#{policy(:producer_policy, producer)} credit=#{batches}/#{granted}")
      inputs.each { |field| Probe.report(Probe.reported_line('INPUT FIELD', field)) }
      Probe.report('OPENED')

      # A wait for output that nothing produces ends by its deadline, and one cancelled from another
      # thread by its token; the reads they leave behind are the consumer's.
      expiring = Probe.cancel(EXPIRING_MILLIS)
      expect(Probe.failure_kind { next_delivery(consumer, expiring) } == ERROR_DEADLINE, 'an expired read')
      Probe.report('NEXT deadline')
      token = Probe.cancel
      waiter = Thread.new { Probe.failure_kind { next_delivery(consumer, token) } }
      sleep 0.1
      F[:cancel_trigger].call(token)
      expect(waiter.value == ERROR_CANCELLED, 'a cancelled read reports its cancellation')
      Probe.report('NEXT cancelled')

      # A batch built for another schema is refused before anything is sent, and the same batch
      # written as a stream by other tooling is refused by the server.
      rows = Probe.typed_rows
      other = Batch.build(output_schema, rows)
      expect(Probe.failure_kind { submit(producer, other) } == ERROR_INVALID_ARGUMENT, 'another schema')
      Probe.report('SUBMIT invalid argument')
      foreign = other.stream
      copy = Probe.buffer(foreign)
      submission = Probe.slot(8)
      Probe.check(E[:producer_submit_ipc].call(producer, copy, foreign.bytesize, nil, submission))
      copy[0, foreign.bytesize] = [SCRIBBLE].pack('C') * foreign.bytesize
      other.release
      Probe.report("OUTCOME #{outcome(producer, submission[0, 8].unpack1('Q'))}")

      # The typed batch: its outcome waits for the application's acknowledgement.
      first = submit(producer, Batch.build(input_schema, rows))
      Probe.report('SUBMITTED first')
      delivery = next_delivery(consumer)
      Probe.report(delivery.summary)
      pending_ids = Probe.slot(8 * 4)
      pending_resolved = Probe.slot(4)
      pending_count = Probe.slot(SIZE_BYTES)
      Probe.check(E[:producer_pending].call(producer, pending_ids, pending_resolved, 4, pending_count))
      expect(Probe.read_size(pending_count) == 1 && pending_ids[0, 8].unpack1('Q') == first &&
             pending_resolved[0, 1].unpack1('C').zero?, 'the only submission waits for its outcome')
      early = Probe.slot
      expiring = Probe.cancel(EXPIRING_MILLIS)
      expect(Probe.failure_kind { Probe.check(E[:producer_rejoin].call(producer, first, expiring, early)) } ==
             ERROR_DEADLINE, 'an unacknowledged submission has no outcome')
      Probe.report('PENDING first unresolved')
      delivered = delivery.batch
      printed = delivered.rows(output)
      printed.each { |line| Probe.report(line) }
      expect(delivered.stream == delivery.read(:delivery_ipc), 'the batch borrows the stream it carried')

      # A retried attempt comes back with the same identity and a new reference; the first
      # reference is stale from then on.
      Probe.report("RETRY #{delivery.settle(:delivery_retry)}")
      again = next_delivery(consumer)
      expect(again.read(:delivery_identity) == delivery.read(:delivery_identity), 'a retry keeps the identity')
      expect(again.read(:delivery_reference) != delivery.read(:delivery_reference), 'a retry makes a new reference')
      Probe.report('REDELIVERED same identity new reference')
      Probe.report("ACK #{delivery.settle(:delivery_ack)}")
      delivery.release

      # A reference retained here outlives the first, released on another thread, and reads and
      # settles the same attempt.
      retained = again.retain
      retained_batch = again.batch
      before = retained.read(:delivery_ipc)
      Thread.new { again.release }.join
      expect(retained.read(:delivery_ipc) == before, 'a retained delivery keeps its stream')
      expect(retained_batch.rows(output) == printed, 'a retained batch reads the same rows')
      Probe.report("ACK #{retained.settle(:delivery_ack)}")
      retained.release
      retained_batch.release
      Probe.report("OUTCOME #{outcome(producer, first)}")

      # An application rejection finishes the batch through the emitter's message error policy.
      second = submit(producer, Batch.build(input_schema, [Probe.plain_row(10, 'beta')]))
      Probe.report('SUBMITTED second')
      rejected = next_delivery(consumer)
      Probe.report("REJECT #{rejected.reject('application refused')}")
      rejected.release
      Probe.report("OUTCOME #{outcome(producer, second)}")

      # Two outstanding batches use up the producer's credit, so a third waits for an outcome.
      fifth_batch = Batch.build(input_schema, [Probe.plain_row(24, 'acme'), Probe.plain_row(25, 'acme')])
      third = submit(producer, Batch.build(input_schema, [Probe.plain_row(20, 'acme'), Probe.plain_row(21, 'acme')]))
      Probe.report('SUBMITTED third')
      fourth = submit(producer, Batch.build(input_schema, [Probe.plain_row(22, 'acme'), Probe.plain_row(23, 'acme')]))
      Probe.report('SUBMITTED fourth')
      expiring = Probe.cancel(EXPIRING_MILLIS)
      expect(Probe.failure_kind { submit(producer, fifth_batch, expiring) } == ERROR_DEADLINE, 'beyond the credit')
      Probe.report('SUBMIT deadline')
      # A batch refused as busy is sent again after the ingestor's backoff, behind the batch
      # submitted after it, so the two outputs may arrive in either order; each keeps its rows.
      outputs = Array.new(2) do
        output_delivery = next_delivery(consumer)
        ids = output_delivery.batch.ids(output)
        Probe.report("ACK #{output_delivery.settle(:delivery_ack)}")
        output_delivery.release
        ids
      end
      expect(outputs.sort == [[20, 21], [22, 23]], 'a multiple-row batch keeps its rows')
      Probe.report("OUTCOME #{outcome(producer, third)}")
      Probe.report("OUTCOME #{outcome(producer, fourth)}")
      fifth = submit(producer, fifth_batch)
      Probe.report('SUBMITTED fifth')
      fifth_output = next_delivery(consumer)
      expect(fifth_output.batch.ids(output) == [24, 25], 'a multiple-row batch keeps its rows')
      Probe.report("ACK #{fifth_output.settle(:delivery_ack)}")
      fifth_output.release
      Probe.report("OUTCOME #{outcome(producer, fifth)}")

      # The scenario cuts the session while one delivery is held unacknowledged.
      extra = opened(open_producer(input_fields, 1, 65_536, out), out)
      Probe.report('PRODUCER extra opened')
      held = submit(producer, Batch.build(input_schema, [Probe.plain_row(30, 'gamma')]))
      Probe.report('SUBMITTED held')
      held_delivery = next_delivery(consumer)
      held_identity = held_delivery.read(:delivery_identity)
      Probe.report('HOLDING')
      expect(Probe.failure_kind { next_delivery(consumer) } == ERROR_INTERRUPTED, 'a lost session interrupts')
      Probe.report('NEXT interrupted')
      expect(Probe.failure_kind { held_delivery.settle(:delivery_ack) } == ERROR_REJECTED, 'a delivery expired')
      Probe.report('ACK expired')
      held_delivery.release
      Probe.report("OUTCOME #{outcome(producer, held)}")
      waited_until = Process.clock_gettime(Process::CLOCK_MONOTONIC) + (WAIT_MILLIS / 1000.0)
      while E[:producer_state].call(extra) == ENDPOINT_ACTIVE
        raise 'the extra producer stayed active after its session ended' if Process.clock_gettime(Process::CLOCK_MONOTONIC) > waited_until

        sleep 0.01
      end
      closing = Probe.cancel(WAIT_MILLIS)
      Probe.check(E[:producer_close].call(extra, closing))
      expect(E[:producer_state].call(extra) == ENDPOINT_CLOSED, 'a producer closed during reconnect is closed')
      Probe.report('CLOSED extra producer')
      Probe.report('WAITING restore')

      # The restored consumer receives the held batch again, and the restored producer publishes.
      redelivered = next_delivery(consumer)
      expect(redelivered.read(:delivery_identity) == held_identity, 'the held batch keeps its identity')
      Probe.report('REDELIVERED held same identity')
      Probe.report("ACK #{redelivered.settle(:delivery_ack)}")
      redelivered.release
      sixth = submit(producer, Batch.build(input_schema, [Probe.plain_row(40, 'gamma')]))
      Probe.report('SUBMITTED sixth')
      sixth_output = next_delivery(consumer)
      Probe.report("ACK #{sixth_output.settle(:delivery_ack)}")
      sixth_output.release
      Probe.report("OUTCOME #{outcome(producer, sixth)}")
      Probe.report("STATE producer=#{STATES.fetch(E[:producer_state].call(producer))} " \
                   "extra=#{STATES.fetch(E[:producer_state].call(extra))} " \
                   "consumer=#{STATES.fetch(E[:consumer_state].call(consumer))}")
      Probe.check(E[:consumer_close].call(consumer, closing))
      Probe.check(E[:producer_close].call(producer, closing))
      expect(E[:consumer_state].call(consumer) == ENDPOINT_CLOSED && E[:producer_state].call(producer) == ENDPOINT_CLOSED,
             'closed handles read closed')
      Probe.report('CLOSED completed')
      raise "the probe's checks failed: #{@failures.join('; ')}" unless @failures.empty?

      Probe.report('CHECKS ok')
      E[:producer_free].call(extra)
      E[:producer_free].call(producer)
      E[:consumer_free].call(consumer)
      F[:schema_free].call(input_schema)
      F[:schema_free].call(output_schema)
      E[:fields_free].call(input_fields)
      E[:fields_free].call(output_fields)
    end
  end

  def self.main
    domain = ENV.fetch('NERVIX_PROBE_DOMAIN')
    session = Session.new(ENV.fetch('NERVIX_PROBE_GRPC_URI'), domain,
                          ENV.fetch('NERVIX_PROBE_USERNAME'), ENV.fetch('NERVIX_PROBE_PASSWORD'))
    return run_clock(session, domain) if ARGV == ['clock']

    if ARGV == ['io']
      Endpoints.new(session, domain).run
      session.close
      report('PASS')
      return
    end

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
