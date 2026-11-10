# frozen_string_literal: true

# The Ruby conformance client of Nervix::Client's native gem. Report formatting belongs here;
# sessions, recovery, typed columns, GVL release and object ownership belong to the gem.
require 'nervix_client'

module Probe
  NX = Nervix::Client
  Failure = NX::ClientError
  ERROR_DEADLINE = NX::FailureKind::DEADLINE
  ERROR_CANCELLED = NX::FailureKind::CANCELLED
  ERROR_INVALID_ARGUMENT = NX::FailureKind::INVALID_ARGUMENT
  ERROR_REJECTED = NX::FailureKind::REJECTED
  ERROR_INTERRUPTED = NX::FailureKind::INTERRUPTED
  PART_ROWS = NX::Part::ROWS
  PART_BRANCH_KEY = NX::Part::BRANCH_KEY
  CELL_VALUE = NX::CellState::VALUE
  CELL_NULL = NX::CellState::NULL
  CELL_REDACTED = NX::CellState::REDACTED
  EVENT_ROWS = NX::EventKind::ROWS
  CLOCK_PACED = NX::ClockState::PACED
  ENDPOINT_ACTIVE = NX::EndpointState::ACTIVE
  ENDPOINT_CLOSED = NX::EndpointState::CLOSED
  UPLOAD_IDENTITY_CONFLICT = NX::UploadFailure::IDENTITY_CONFLICT
  WAIT_MILLIS = 120_000
  EXPIRING_MILLIS = 200
  PRODUCER_BATCHES = 2
  CONSUMER_BATCHES = 4
  ENDPOINT_BYTES = 1_048_576
  SCRIBBLE = 0xaa

  def self.name(namespace, value)
    namespace.constants.find { |key| namespace.const_get(key) == value }.to_s
  end

  def self.lower(namespace, value) = name(namespace, value).downcase
  def self.disposition(outcome) = lower(NX::Disposition, outcome.disposition)
  def self.cancel(millis = nil) = millis.nil? ? NX::Cancel.new : NX::Cancel.new(deadline: millis / 1000.0)

  # The width of each fixed-width type, and the `unpack` directive that reads it in native byte
  # order. Floats are read as unsigned integers of their width, because the report prints bits.
  FIXED = {
    'U8' => [1, 'C', 'u8'], 'I8' => [1, 'c', 'i8'], 'U16' => [2, 'S', 'u16'],
    'I16' => [2, 's', 'i16'], 'U32' => [4, 'L', 'u32'], 'I32' => [4, 'l', 'i32'],
    'U64' => [8, 'Q', 'u64'], 'I64' => [8, 'q', 'i64'], 'F32' => [4, 'L', 'f32'],
    'F64' => [8, 'Q', 'f64'], 'BOOL' => [1, 'C', 'bool'], 'DATETIME' => [8, 'q', 'datetime']
  }.freeze

  Field = Struct.new(:name, :type, :nullable, :sensitive) do
    def line(prefix)
      "#{prefix} #{name} #{type} #{nullable ? 'nullable' : 'required'} #{sensitive ? 'sensitive' : 'public'}"
    end
  end

  def self.fields(schema, part)
    selected = part == PART_ROWS ? schema.fields : schema.key_fields
    selected.map { |field| Field.new(field.name, name(NX::FieldType, field.type), field.nullable, field.sensitive) }
  end

  class Event
    def initialize(event) = @event = event
    def retain = Event.new(@event)
    def release = @event = nil
    def kind = @event.kind
    def row_count = @event.row_count
    def subscription = [@event.subscription.name, @event.subscription.generation]
    def frame = @event.frame

    def column(part, index, field, cells)
      states = @event.states(index, part)
      typed = @event.column(index, part)
      values = if FIXED.key?(field.type)
                 _, directive, prefix = FIXED.fetch(field.type)
                 @event.fixed(index, part).unpack("#{directive}*").map do |value|
                   case field.type
                   when 'F32' then format('f32:%08x', value)
                   when 'F64' then format('f64:%016x', value)
                   when 'BOOL' then "bool:#{value != 0}"
                   else "#{prefix}:#{value}"
                   end
                 end
               else
                 offsets, data = @event.varlen(index, part)
                 Array.new(cells) do |row|
                   raw = data.byteslice(offsets[row]...offsets[row + 1])
                   if states[row] == CELL_VALUE && typed[row].b != raw
                     raise 'a typed value differs from the column copied in bulk'
                   end
                   "#{field.type == 'STRING' ? 'str' : 'bytes'}:#{raw.unpack1('H*')}"
                 end
               end
      states.each_with_index.map do |state, row|
        case state
        when CELL_NULL
          raise 'a null cell exposes a value' unless typed[row].nil?
          'null'
        when CELL_REDACTED
          raise 'a redacted cell exposes a value' unless typed[row] == NX::REDACTED
          'redacted'
        when CELL_VALUE then values.fetch(row)
        else raise "unknown cell state #{state}"
        end
      end
    end

    def render(part, fields, cells)
      rows = Array.new(cells) { [] }
      fields.each_with_index do |field, index|
        column(part, index, field, cells).each_with_index { |value, row| rows[row] << "#{field.name}=#{value}" }
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
  def self.paced(read)
    clock = read.paced
    PacedClock.new(clock.generation, clock.period_nanos, clock.skew_nanos, clock.origin, clock.anchor, clock.rate)
  end

  class ClockEvent
    def initialize(event) = @event = event
    def retain = ClockEvent.new(@event)
    def release = @event = nil
    def kind = Probe.name(NX::ClockEventKind, @event.kind)
    def domain = @event.domain
    def generation = @event.generation
    def state = @event.state
    def paced = Probe.paced(@event)
    def tick
      value = @event.tick
      Tick.new(value.generation, value.id, value.boundary, value.serving_logical)
    end
  end

  class DomainClock
    def initialize(clock) = @clock = clock
    def retain = DomainClock.new(@clock)
    def release = @clock = nil
    def generation = @clock.generation
    def state = @clock.state
    def paced = Probe.paced(@clock)
    def tick_id = @clock.tick&.id
    def logical_time_at(utc) = @clock.logical_time_at(utc)
    def wall_duration_until(utc, target) = @clock.wall_duration_until(utc, target)
    def admission_window(utc) = @clock.admission_window(utc)
    def admits(utc, event) = @clock.admits(utc, event)
  end

  class Session
    attr_reader :native
    def initialize(server, domain, username, password)
      @native = NX::Session.connect(server, domain: domain, username: username, password: password)
    end
    def close = @native.close
    def execute(query, cancel = nil) = @native.execute(@native.prepare(query), cancel: cancel)
    def next_event(cancel) = Event.new(@native.next_event(cancel: cancel))
    def domain_clock(domain)
      value = @native.domain_clock(domain)
      DomainClock.new(value) unless value.nil?
    end
    def next_clock_event(cancel, domain)
      value = @native.next_clock_event(cancel: cancel)
      raise 'a clock event arrived for another domain' unless value.domain == domain
      ClockEvent.new(value)
    end
  end

  def self.error_line(outcome)
    first = outcome.diagnostics.first
    span = first&.span.nil? ? 'none' : first.span.join('..')
    "ERROR #{disposition(outcome)} diagnostics=#{outcome.diagnostics.size} span=#{span}"
  end

  def self.failure_of
    yield
    raise 'a call succeeded where it had to fail'
  rescue Failure => e
    e
  end

  def self.check_retention(retained, reported, fields, key_fields)
    frames = retained.map { |event| event.frame.get_string }
    # References nothing reaches are released by the collector, concurrently with reads of the
    # references that remain.
    retained.each { |event| event.retain.row_lines(fields, key_fields) }
    3.times do
      GC.start(full_mark: true, immediate_sweep: true)
      Array.new(2048) { |size| 'x' * (size * 64) }
    end
    retained.each_with_index do |event, index|
      raise 'a retained frame changed while it was retained' if event.frame.get_string != frames[index]
      raise 'a retained frame is not a ServerMessage frame' if event.frame.get_string(4, 4) != 'NXSM'
      raise 'a retained event reads differently than it did' if event.row_lines(fields, key_fields) != reported[index]
    end
    views = retained.map { |event| event.frame.slice(4, 4) }
    Thread.new { retained.each(&:release) }.join
    GC.start(full_mark: true, immediate_sweep: true)
    raise 'a frame slice lost its owner' unless views.all? { |view| view.get_string == 'NXSM' }
  end

  def self.check_cancellation(session)
    cancel = cancel()
    waiter = Thread.new { failure_of { session.next_event(cancel) } }
    sleep 0.1
    cancel.trigger
    raise 'a cancelled wait did not report CANCELLED' unless waiter.join(30)&.value&.kind == ERROR_CANCELLED

    expired = failure_of { session.next_event(cancel(50)) }
    raise 'an expired wait did not report DEADLINE' unless expired.kind == ERROR_DEADLINE

    cancelled = cancel()
    cancelled.trigger
    command = failure_of { session.execute('SHOW DOMAINS;', cancelled) }
    return if command.kind == ERROR_CANCELLED && !command.execution_reference.to_s.empty?

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
    cancel.trigger
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
    fields = NX::Fields.new
    specs.each do |name, levels, nullable, sensitive|
      kind, length = levels.first
      fields.add(name, NX::FieldType.const_get(kind), length, nullable: nullable, sensitive: sensitive)
      levels.drop(1).each { |element, size| fields.element(NX::FieldType.const_get(element), size) }
    end
    fields
  end

  def self.reported_fields(schema)
    schema.fields.map do |field|
      levels = field.levels.map { |level| [name(NX::FieldType, level.type), level.length] }
      raise "a field's first level differs from its type" unless levels.first.first == name(NX::FieldType, field.type)
      { name: field.name, levels: levels, nullable: field.nullable, sensitive: field.sensitive }
    end
  end

  # A field's type in Plexus's spelling, with nested fixed-size lists as dimensions.
  def self.type_text(levels)
    kind = levels.first.first
    return "VEC<#{type_text(levels.drop(1))}>" if kind == 'LIST'

    if kind == 'FIXED_LIST'
      dimensions = levels.take_while { |level_kind, _| level_kind == 'FIXED_LIST' }.map(&:last)
      return "ARRAY<#{type_text(levels.drop(dimensions.size))}, #{dimensions.join(', ')}>"
    end
    kind
  end

  def self.reported_line(prefix, field)
    "#{prefix} #{field[:name]} #{type_text(field[:levels])} #{field[:nullable] ? 'nullable' : 'required'} " \
      "#{field[:sensitive] ? 'sensitive' : 'public'}"
  end

  class Batch
    attr_reader :native
    def initialize(batch) = @native = batch
    def retain = Batch.new(@native)
    def release = @native = nil
    def stream = @native.ipc.get_string
    def cells(column, level) = @native.cells(column, level)

    def self.build(schema, rows)
      builder = NX::BatchBuilder.new(schema, rows.size)
      Probe.reported_fields(schema).each_with_index do |field, index|
        column = Probe.host_column(field[:name], rows)
        unless column[:states].nil?
          states = column[:states].bytes.map { |state| state == CELL_VALUE }
          builder.states(index, states)
          states.fill(false)
        end
        unless column[:offsets].nil?
          offsets = column[:offsets].dup
          builder.offsets(index, 0, offsets)
          offsets.fill((1 << 64) - 1)
        end
        if column[:varlen].nil?
          values = column[:fixed].dup
          builder.fixed(index, column[:leaf], values)
          values.replace([SCRIBBLE].pack('C') * values.bytesize)
        else
          value_offsets, data = column[:varlen]
          offsets = value_offsets.dup
          values = data.dup
          builder.varlen(index, column[:leaf], offsets, values)
          offsets.fill((1 << 64) - 1)
          values.replace([SCRIBBLE].pack('C') * values.bytesize)
        end
      end
      new(builder.finish)
    end

    def column(index, field)
      states = @native.states(index)
      levels = field[:levels]
      innermost = levels.size - 1
      lists = levels.take(innermost).each_with_index.map do |(kind, length), level|
        kind == 'LIST' ? @native.offsets(index, level) : length
      end
      kind = levels.last.first
      count = cells(index, innermost)
      leaves = if %w[STRING BYTES].include?(kind)
                 bounds, bytes = @native.varlen(index, innermost)
                 prefix = kind == 'STRING' ? 'str' : 'bytes'
                 Array.new(count) { |cell| "#{prefix}:#{bytes.byteslice(bounds[cell]...bounds[cell + 1]).unpack1('H*')}" }
               else
                 _, directive, prefix = FIXED.fetch(kind)
                 @native.fixed(index, innermost).unpack("#{directive}*").map do |value|
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
      Array.new(@native.row_count) do |row|
        "ROW #{fields.each_with_index.map { |field, index| "#{field[:name]}=#{columns[index][row]}" }.join(' ')}"
      end
    end

    def ids(fields)
      index = fields.index { |field| field[:name] == 'id' }
      column(index, fields[index]).map { |value| Integer(value.delete_prefix('u32:')) }
    end
  end

  class Delivery
    def initialize(delivery) = @native = delivery
    def retain = Delivery.new(@native)
    def release = @native = nil
    def read(accessor)
      case accessor
      when :delivery_identity then @native.identity
      when :delivery_reference then @native.reference
      when :delivery_relay then @native.source_relay
      when :delivery_ipc then @native.ipc.get_string
      else raise "unknown report field #{accessor}"
      end
    end
    def summary
      branch = @native.branch_fingerprint&.bytesize || 'none'
      "DELIVERY relay=#{@native.source_relay} members=#{@native.members} branch=#{branch} " \
        "identity=#{@native.identity.bytesize} reference=#{@native.reference.bytesize}"
    end
    def batch = Batch.new(@native.batch)
    def settle(accessor)
      value = case accessor
              when :delivery_ack then @native.ack(cancel: Probe.cancel(WAIT_MILLIS))
              when :delivery_retry then @native.retry(cancel: Probe.cancel(WAIT_MILLIS))
              else raise "unknown settlement #{accessor}"
              end
      Probe.lower(NX::Settlement, value)
    end
    def reject(reason) = Probe.lower(NX::Settlement, @native.reject(reason, cancel: Probe.cancel(WAIT_MILLIS)))
  end

  def self.failure_kind
    yield
    raise 'a call succeeded where it had to fail'
  rescue Failure => e
    e.kind
  end

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
    def open_producer(fields, batches = PRODUCER_BATCHES, asked = ENDPOINT_BYTES)
      @session.native.open_ingestor(@domain, @ingestor, fields, batches: batches, bytes: asked)
    end
    def open_consumer(fields)
      @session.native.subscribe_emitter(@domain, @emitter, fields, batches: CONSUMER_BATCHES, bytes: ENDPOINT_BYTES)
    end
    def expect_refused(what)
      error = Probe.failure_of { yield }
      unless error.kind == NX::FailureKind::REJECTED && error.open_refusal == NX::OpenRefusal::SCHEMA_MISMATCH
        raise 'an open with another schema was not refused exactly'
      end
      Probe.report("REFUSED #{what} schema mismatch")
    end
    def submit(producer, batch, cancel = nil) = producer.submit(batch.native, cancel: cancel)
    def outcome(producer, submission)
      result = producer.rejoin(submission, cancel: Probe.cancel(WAIT_MILLIS))
      case result.result
      when NX::SubmissionResult::COMPLETED then 'completed'
      when NX::SubmissionResult::NOT_ADMITTED
        text = "not_admitted #{Probe.lower(NX::SubmissionRefusal, result.refusal)}"
        text += " #{Probe.lower(NX::BatchDefect, result.defect)}" if result.refusal == NX::SubmissionRefusal::INVALID_BATCH
        text
      when NX::SubmissionResult::PROCESSING_FAILED then "processing_failed #{Probe.lower(NX::ProcessingFailure, result.failure)}"
      else "outcome_unknown #{Probe.lower(NX::Uncertainty, result.uncertainty)}"
      end
    end
    def next_delivery(consumer, cancel = Probe.cancel(WAIT_MILLIS)) = Delivery.new(consumer.next(cancel: cancel))
    def policy(handle)
      policy = handle.policy
      "window=#{Probe.lower(NX::WindowKind, policy.window)}/#{policy.outstanding} ack_timeout=#{policy.ack_timeout_nanos}"
    end
    def grant(handle)
      grant = handle.grant
      [grant.batches, grant.bytes, grant.max_batch_rows, grant.max_batch_bytes]
    end

    def run
      mismatched_specs = INPUT_FIELDS.map do |name, levels, nullable, sensitive|
        [name, levels, nullable, name == 'secret' ? false : sensitive]
      end
      input_fields = Probe.fields_handle(INPUT_FIELDS)
      output_fields = Probe.fields_handle(INPUT_FIELDS + [ECHO_FIELD])
      mismatched = Probe.fields_handle(mismatched_specs)

      # An open whose expected fields differ from the endpoint's is refused exactly.
      expect_refused('producer') { open_producer(mismatched) }
      expect_refused('consumer') { open_consumer(input_fields) }

      consumer = open_consumer(output_fields)
      producer = open_producer(input_fields)
      output_schema = consumer.schema
      input_schema = producer.schema
      output = Probe.reported_fields(output_schema)
      inputs = Probe.reported_fields(input_schema)

      batches, granted, max_rows, max_bytes = grant(consumer)
      Probe.report("CONSUMER generation=#{consumer.generation} " \
                   "state=#{Probe.lower(NX::EndpointState, consumer.state)} #{policy(consumer)} " \
                   "credit=#{batches}/#{granted} max=#{max_rows}/#{max_bytes}")
      output.each { |field| Probe.report(Probe.reported_line('OUTPUT FIELD', field)) }
      batches, granted, max_rows, max_bytes = grant(producer)
      expect(max_rows.positive? && max_bytes.positive? && max_bytes <= granted, "the producer's limits fit its grant")
      admission = producer.admission == 1 ? 'open' : 'suspended'
      Probe.report("PRODUCER generation=#{producer.generation} " \
                   "state=#{Probe.lower(NX::EndpointState, producer.state)} admission=#{admission} " \
                   "#{policy(producer)} credit=#{batches}/#{granted}")
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
      token.trigger
      expect(waiter.value == ERROR_CANCELLED, 'a cancelled read reports its cancellation')
      Probe.report('NEXT cancelled')

      # A batch built for another schema is refused before anything is sent, and the same batch
      # written as a stream by other tooling is refused by the server.
      rows = Probe.typed_rows
      other = Batch.build(output_schema, rows)
      expect(Probe.failure_kind { submit(producer, other) } == ERROR_INVALID_ARGUMENT, 'another schema')
      Probe.report('SUBMIT invalid argument')
      foreign = other.stream
      copy = foreign.dup
      submission = producer.submit_ipc(copy)
      copy.replace([SCRIBBLE].pack('C') * copy.bytesize)
      other.release
      Probe.report("OUTCOME #{outcome(producer, submission)}")

      # The typed batch: its outcome waits for the application's acknowledgement.
      first = submit(producer, Batch.build(input_schema, rows))
      Probe.report('SUBMITTED first')
      delivery = next_delivery(consumer)
      Probe.report(delivery.summary)
      pending = producer.pending
      expect(pending.size == 1 && pending.first.id == first && !pending.first.resolved, 'the only submission waits for its outcome')
      expiring = Probe.cancel(EXPIRING_MILLIS)
      expect(Probe.failure_kind { producer.rejoin(first, cancel: expiring) } == ERROR_DEADLINE, 'an unacknowledged submission has no outcome')
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
      extra = open_producer(input_fields, 1, 65_536)
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
      while extra.state == ENDPOINT_ACTIVE
        raise 'the extra producer stayed active after its session ended' if Process.clock_gettime(Process::CLOCK_MONOTONIC) > waited_until

        sleep 0.01
      end
      closing = Probe.cancel(WAIT_MILLIS)
      extra.close(cancel: closing)
      expect(extra.state == ENDPOINT_CLOSED, 'a producer closed during reconnect is closed')
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
      Probe.report("STATE producer=#{Probe.lower(NX::EndpointState, producer.state)} " \
                   "extra=#{Probe.lower(NX::EndpointState, extra.state)} " \
                   "consumer=#{Probe.lower(NX::EndpointState, consumer.state)}")
      consumer.close(cancel: closing)
      producer.close(cancel: closing)
      expect(consumer.state == ENDPOINT_CLOSED && producer.state == ENDPOINT_CLOSED,
             'closed handles read closed')
      Probe.report('CLOSED completed')
      raise "the probe's checks failed: #{@failures.join('; ')}" unless @failures.empty?

      Probe.report('CHECKS ok')
    end
  end

  def self.run_upload(session)
    execution = session.native.prepare(ENV.fetch('NERVIX_PROBE_UPLOAD_QUERY'))
    ['UPLOAD completed', 'RETRY completed'].each do |line|
      outcome = session.native.execute(execution)
      raise 'the upload must complete' unless outcome.disposition == NX::Disposition::COMPLETED
      raise 'a completed upload has no refusal' unless outcome.upload_failure.nil?
      report(line)
    end
    File.binwrite(ENV.fetch('NERVIX_PROBE_UPLOAD_FILE'), "different archive\n")
    outcome = session.native.execute(execution)
    unless outcome.disposition == NX::Disposition::FAILED && outcome.upload_failure == UPLOAD_IDENTITY_CONFLICT
      raise 'the upload must report its typed identity conflict'
    end
    report('CONFLICT IdentityConflict')
  end

  def self.run_inspection(session)
    license = session.native.execute(session.native.prepare('DESCRIBE LICENSE;')).state_inspection
    unless license.domain.nil? && license.entity_name.nil? && license.entity_kind == 'LICENSE' && license.counts.rows == 3 && license.counts.branches.zero?
      raise 'typed cluster licensing observation'
    end
    report('INSPECTION license rows=3')
    execution = session.native.prepare('SHOW RELAY events MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;')
    outcome = session.native.execute(execution)
    inspection = outcome.state_inspection
    raise 'inspection domain' unless inspection.domain == ENV.fetch('NERVIX_PROBE_DOMAIN')
    raise 'inspection identity' unless inspection.entity_name == 'events'
    raise 'empty page counts' unless inspection.counts.rows.zero? && inspection.rejection.nil?
    report('INSPECTION page rows=0')
    refused_outcome = session.native.execute(session.native.prepare('SHOW RELAY plain MATERIALIZED STATE FORMAT JSON;'))
    refused = refused_outcome.state_inspection
    unless refused.rejection == NX::InspectionRejection::NOT_MATERIALIZED && refused.counts.nil?
      raise 'typed inspection refusal'
    end
    report('INSPECTION refused not_materialized')
    query = 'SHOW RELAY populated MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1'
    first = session.native.execute(session.native.prepare(query + ' FORMAT JSON;')).state_inspection
    raise 'complete page counts' unless first.counts.rows == 2 && first.counts.branches == 2 && first.continuation
    unit = first.units.fetch(0)
    raise 'unit identity' unless unit.kind == NX::InspectionUnitKind::SINGLE && unit.index.nil?
    raise 'read origin' unless unit.owner == unit.origin_node && unit.origin_incarnation.positive? && unit.origin_fingerprint.bytesize == 32 && unit.origin_stage == NX::InspectionStage::LIVE
    section = unit.sections.fetch(0)
    schema = section.schema
    raise 'branch sensitivity' unless schema.branch == 'by_tenant' && schema.key_fields.fetch(0).sensitive
    raise 'schema order' unless schema.fields.first(3).map(&:name) == %w[tenant secret sequence]
    raise 'field metadata' unless schema.fields.fetch(0).sensitive && schema.fields.fetch(1).sensitive && schema.fields.fetch(2).type == NX::FieldType::I64
    raise 'section metadata' unless section.omitted_rows == 1 && section.watermark_low != section.watermark_high
    frame = section.frames.fetch(0)
    raise 'frame schema' unless frame.row_count == 1 && frame.schema.branch == schema.branch
    buffer = frame.frame
    expected = buffer.get_string
    raise 'verified read-only bytes' unless buffer.readonly? && !expected.empty?
    token = first.continuation.gsub("'", "''")
    second = session.native.execute(session.native.prepare(query + " AFTER '#{token}' FORMAT JSON;")).state_inspection
    raise 'continuation page' unless second.continuation.nil? && second.counts.rows == 2 && second.units.fetch(0).sections.fetch(0).frames.fetch(0).row_count == 1
    raise 'distinct redacted pages' unless first.json != second.json && !first.json.include?('alpha-private') && !first.json.include?('first-private')
    report('INSPECTION branched rows=2 pages=2')
    session.close
    outcome = refused_outcome = first = second = unit = section = frame = nil
    GC.start
    raise 'retained inspection' unless inspection.counts.rows.zero? && !inspection.json.empty?
    raise 'retained refusal' unless refused.rejection == NX::InspectionRejection::NOT_MATERIALIZED
    raise 'retained verified frame and schema' unless buffer.get_string == expected && schema.fields.fetch(2).name == 'sequence'
    report('INSPECTION retained')
  end

  def self.main
    domain = ENV.fetch('NERVIX_PROBE_DOMAIN')
    session = Session.new(ENV.fetch('NERVIX_PROBE_GRPC_URI'), domain,
                          ENV.fetch('NERVIX_PROBE_USERNAME'), ENV.fetch('NERVIX_PROBE_PASSWORD'))
    if ARGV == ['inspection']
      run_inspection(session)
      report('PASS')
      return
    end
    if ARGV == ['upload']
      run_upload(session)
      session.close
      report('PASS')
      return
    end
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
    raise 'the subscribe command opened no subscription' if opened.subscription.nil?
    generation = opened.subscription.generation
    schema = opened.schema
    fields = fields(schema, PART_ROWS)
    key_fields = fields(schema, PART_BRANCH_KEY)
    branch_name = schema.branch
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
