# frozen_string_literal: true

# Test-owned construction, bounded host waits and the scenario's declared action exchange.
require 'nervix_client'
require 'socket'
require 'uri'
require 'weakref'
require 'open3'
require 'rbconfig'
require 'fileutils'
require 'tmpdir'

module RubySuite
  NX = Nervix::Client
  WAIT = 120

  def self.deadline
    NX::Cancel.new(deadline: WAIT)
  end

  def self.wait_until(message, seconds: WAIT)
    finish = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    loop do
      return if yield
      remaining = finish - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise message unless remaining.positive?

      # Poll only inside this named condition's absolute deadline.
      Kernel.sleep([remaining, 0.01].min)
    end
  end

  class Cluster
    attr_reader :uri, :leader, :domain, :backups

    def initialize
      @uri = ENV.fetch('NERVIX_PROBE_GRPC_URI')
      @leader = ENV.fetch('NERVIX_SUITE_LEADER_GRPC_URI')
      @domain = ENV.fetch('NERVIX_PROBE_DOMAIN')
      @backups = ENV.fetch('NERVIX_SUITE_BACKUP_DIRECTORY')
    end

    def options
      {
        domain: domain,
        username: ENV.fetch('NERVIX_PROBE_USERNAME'),
        password: ENV.fetch('NERVIX_PROBE_PASSWORD'),
        ca_certificate: File.binread(ENV.fetch('NERVIX_SUITE_CA_CERTIFICATE')),
        connect_timeout: WAIT,
        request_timeout: WAIT,
        retry_timeout: WAIT,
        backup_wait_timeout: WAIT,
        cancel: RubySuite.deadline
      }
    end

    def connect(server: uri, **changes)
      NX::Session.connect(server, **options.merge(changes))
    end
  end

  # A transport relay copies opaque bytes, including TLS, through Ruby's socket implementation.
  # Cutting closes every current connection and refuses new ones until restore is called.
  class Relay
    attr_reader :uri

    def initialize(target)
      @target = URI(target)
      @server = TCPServer.new('127.0.0.1', 0)
      @uri = "#{@target.scheme}://127.0.0.1:#{@server.addr[1]}"
      @mutex = Mutex.new
      @enabled = true
      @streams = []
      @workers = []
      @acceptor = Thread.new do
        loop do
          client = @server.accept
          @mutex.synchronize do
            unless @enabled
              client.close
              next
            end
            upstream = Socket.tcp(@target.host, @target.port, connect_timeout: WAIT)
            @streams.concat([client, upstream])
            [[client, upstream], [upstream, client]].each do |input, output|
              @workers << Thread.new do
                IO.copy_stream(input, output)
              rescue IOError, SystemCallError
                # cut/close deliberately withdraw both halves of this exact connection.
              ensure
                begin
                  output.shutdown(Socket::SHUT_WR) unless output.closed?
                rescue IOError, SystemCallError
                  # The relay owns descriptor closure; its cut may already have shut it down.
                end
              end
            end
          end
        end
      rescue IOError, SystemCallError
        raise unless @server.closed?
      end
    end

    def cut
      streams, workers = @mutex.synchronize do
        @enabled = false
        taken = [@streams, @workers]
        @streams = []
        @workers = []
        taken
      end
      begin
        streams.each do |stream|
          next if stream.closed?

          begin
            stream.shutdown(Socket::SHUT_RDWR)
          rescue IOError, SystemCallError
            # A peer or the opposite copying worker may already have ended this socket.
          end
        end
        # Keep descriptors allocated until every copying call has ended, so a reconnect cannot
        # reuse a descriptor a copying worker still holds. One deadline bounds the whole cut.
        finish = Process.clock_gettime(Process::CLOCK_MONOTONIC) + WAIT
        workers.each do |worker|
          remaining = finish - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          unless remaining.positive? && worker.join(remaining)
            raise "relay worker did not stop: #{worker.backtrace&.join("\n")}"
          end
        end
      ensure
        streams.each { |stream| stream.close unless stream.closed? }
      end
    end

    def restore
      @mutex.synchronize { @enabled = true }
    end

    def close
      cut
      @server.close unless @server.closed?
      raise 'relay acceptor did not stop' unless @acceptor.join(WAIT)

    end
  end

  module Actions
    @sequence = 0
    @mutex = Mutex.new

    def self.request(action)
      @mutex.synchronize do
        @sequence += 1
        request = "NERVIX-SUITE-ACTION #{@sequence} #{action}"
        $stdout.puts(request)
        $stdout.flush
        expected = "NERVIX-SUITE-CONFIRMED #{@sequence} #{action}\n"
        finish = Process.clock_gettime(Process::CLOCK_MONOTONIC) + WAIT
        line = ''.b
        until line.end_with?("\n")
          remaining = finish - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise 'scenario action confirmation timed out' unless remaining.positive? && IO.select([$stdin], nil, nil, remaining)

          bytes = $stdin.read_nonblock(257 - line.bytesize, exception: false)
          raise 'scenario action confirmation ended' if bytes.nil?
          next if bytes == :wait_readable

          line << bytes
          raise 'scenario action confirmation is oversized' if line.bytesize > 256
        end
        raise "unexpected scenario confirmation #{line.inspect}" unless line == expected
      end
    end
  end

  module Rows
    def input_fields
      fields = NX::Fields.new
      fields.add('id', NX::FieldType::U32)
      fields.add('tenant', NX::FieldType::STRING)
      fields.add('amount', NX::FieldType::I64)
      fields.add('ratio', NX::FieldType::F64)
      fields.add('flag', NX::FieldType::BOOL)
      fields.add('note', NX::FieldType::STRING, nullable: true)
      fields.add('raw', NX::FieldType::BYTES, nullable: true)
      fields.add('at', NX::FieldType::DATETIME)
      fields.add('tags', NX::FieldType::LIST)
      fields.element(NX::FieldType::STRING)
      fields.add('grid', NX::FieldType::FIXED_LIST, 2)
      fields.element(NX::FieldType::I16)
      fields.add('secret', NX::FieldType::STRING, sensitive: true)
      fields
    end

    def output_fields
      fields = input_fields
      fields.add('echo', NX::FieldType::U32)
      fields
    end

    def build(schema, id = 101, tenant = 'acme')
      builder = NX::BatchBuilder.new(schema, 1)
      builder.fixed(0, 0, [id].pack('L'))
      builder.varlen(1, 0, [0, tenant.bytesize], tenant)
      builder.fixed(2, 0, [-9_223_372_036_854_775_808].pack('q'))
      builder.fixed(3, 0, [0x8000000000000000].pack('Q'))
      builder.fixed(4, 0, [1].pack('C'))
      builder.states(5, [false])
      builder.varlen(5, 0, [0, 0], ''.b)
      builder.states(6, [true])
      builder.varlen(6, 0, [0, 2], "\x00\xff".b)
      builder.fixed(7, 0, [-1].pack('q'))
      builder.offsets(8, 0, [0, 2])
      builder.varlen(8, 1, [0, 1, 3], "aé")
      builder.fixed(9, 1, [-32_768, 32_767].pack('s*'))
      builder.varlen(10, 0, [0, 6], 'secret')
      builder.fixed(11, 0, [id].pack('L')) if schema.fields.length == 12
      builder.finish
    end
  end

  class Boundary < Minitest::Test
    include Rows
    def self.test_order = :alpha

    def assert_collected(weak)
      RubySuite.wait_until('the collector did not release the unreachable native object') do
        GC.start(full_mark: true, immediate_sweep: true)
        GC.compact
        !weak.weakref_alive?
      end
    end

    def assert_packaged_gem
      # ANCHOR: packaging
      fresh = File.join(ENV.fetch('NERVIX_CLIENT_CONFORMANCE_DIR'), 'ruby-fresh')
      environment = { 'GEM_HOME' => fresh, 'GEM_PATH' => fresh, 'RUBYLIB' => nil, 'RUBYOPT' => nil }
      stdout, stderr, status = Open3.capture3(
        environment, RbConfig.ruby, '-rnervix_client', '-e',
        'abort "wrong ABI" unless Nervix::Client::RUBY_ABI == RbConfig::CONFIG.fetch("ruby_version"); puts Nervix::Client::VERSION'
      )
      assert status.success?, stderr
      assert_equal NX::VERSION, stdout.strip
      # ANCHOR_END: packaging
    end
  end

  class ClusterTest < Boundary
    attr_reader :cluster, :session

    def setup
      @cluster = Cluster.new
      @session = cluster.connect
      @owned = []
    end

    def teardown
      @owned.reverse_each { |object| object.close(cancel: RubySuite.deadline) }
    ensure
      session&.close
    end

    def producer(target = session, name = 'typed_in')
      value = target.open_ingestor(cluster.domain, name, input_fields,
                                   batches: 2, bytes: 1 << 20, cancel: RubySuite.deadline)
      @owned << value
      value
    end

    def consumer(target = session)
      value = target.subscribe_emitter(cluster.domain, 'typed_out', output_fields,
                                      batches: 4, bytes: 1 << 20, cancel: RubySuite.deadline)
      @owned << value
      value
    end

    def execute(statement, target = session)
      execution = target.prepare(statement, cancel: RubySuite.deadline)
      target.execute(execution, cancel: RubySuite.deadline)
    end

    def completed(statement, target = session)
      outcome = execute(statement, target)
      assert_equal NX::Disposition::COMPLETED, outcome.disposition, outcome.message
      outcome
    end

    def unique(prefix)
      "#{prefix}_#{name.gsub(/[^a-z0-9]/i, '_')}"
    end

    def next_event(target = session)
      target.next_event(cancel: RubySuite.deadline)
    end

    def next_clock(target, &wanted)
      RubySuite.wait_until('the expected clock event did not arrive') do
        event = target.next_clock_event(cancel: RubySuite.deadline)
        return event if wanted.call(event)
      end
    end
  end
end
