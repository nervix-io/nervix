# frozen_string_literal: true

module RubySuite
  class ColumnsTest < Boundary
    def test_fields_describe_every_level_and_keep_flags_and_order
      # ANCHOR: fields
      fields = NX::Fields.new
      fields.add('id', NX::FieldType::U64)
      fields.add('nested', NX::FieldType::LIST, nullable: true, sensitive: true)
      fields.element(NX::FieldType::FIXED_LIST, 2)
      fields.element(NX::FieldType::I16)
      schema = fields.schema
      assert_instance_of NX::Schema, schema
      assert_nil schema.branch
      assert_empty schema.key_fields
      field = schema.fields.last
      assert_instance_of NX::Field, field
      assert_equal 'nested', field.name
      assert_equal NX::FieldType::LIST, field.type
      assert field.nullable
      assert field.sensitive
      level = field.levels[1]
      assert_instance_of NX::Level, level
      assert_equal NX::FieldType::FIXED_LIST, level.type
      assert_equal 2, level.length
      # ANCHOR_END: fields
      assert_equal %w[id nested], schema.fields.map(&:name)
      assert_equal [NX::FieldType::LIST, NX::FieldType::FIXED_LIST, NX::FieldType::I16], field.levels.map(&:type)
      assert_equal [0, 2, 0], field.levels.map(&:length)
      assert_raises(NX::InvalidArgumentError) { fields.element(NX::FieldType::U8) }
    end

    def test_a_builder_copies_inputs_and_ipc_preserves_typed_nested_columns
      schema = input_fields.schema
      # ANCHOR: build_batch
      builder = NX::BatchBuilder.new(schema, 1)
      assert_instance_of NX::BatchBuilder, builder
      bytes = [4_294_967_295].pack('L')
      builder.fixed(0, 0, bytes)
      bytes.replace("\x00".b * bytes.bytesize)
      builder.varlen(1, 0, [0, 5], "a\0éx")
      builder.fixed(2, 0, [-9_223_372_036_854_775_808].pack('q'))
      builder.fixed(3, 0, [0x8000000000000000].pack('Q'))
      builder.fixed(4, 0, [1].pack('C'))
      builder.states(5, [false])
      builder.varlen(5, 0, [0, 0], ''.b)
      builder.varlen(6, 0, [0, 2], "\x00\xff".b)
      builder.fixed(7, 0, [-1].pack('q'))
      builder.offsets(8, 0, [0, 2])
      builder.varlen(8, 1, [0, 1, 3], 'aé')
      builder.fixed(9, 1, [-32_768, 32_767].pack('s*'))
      builder.varlen(10, 0, [0, 6], 'secret')
      batch = builder.finish
      assert_instance_of NX::Batch, batch
      assert_equal 1, batch.row_count
      assert_equal [4_294_967_295], batch.column(0)
      assert_equal ["a\0éx"], batch.column(1)
      assert_equal [nil], batch.column(5)
      assert_equal [["a", "é"]], batch.column(8)
      assert_equal [[-32_768, 32_767]], batch.column(9)
      # ANCHOR_END: build_batch
      assert_equal [NX::CellState::NULL], batch.states(5)
      assert_equal [0x8000000000000000], batch.fixed(3, 0).unpack('Q*')
      assert_equal [-1], batch.column(7)
      assert_equal [0, 2], batch.offsets(8, 0)
      assert_equal 2, batch.cells(8, 1)
      assert_equal [[0, 1, 3], 'aé'.b], batch.varlen(8, 1)
      assert_equal schema.fields.map(&:name), batch.schema.fields.map(&:name)
      # ANCHOR: ipc
      stream = batch.ipc
      assert_instance_of IO::Buffer, stream
      assert stream.readonly?
      restored = NX::Batch.decode(batch.schema, stream.get_string, batch.row_count)
      assert_instance_of NX::Batch, restored
      schema.fields.each_index { |index| assert_equal batch.column(index), restored.column(index) }
      # ANCHOR_END: ipc
      assert_raises(NX::TypeMismatchError) { batch.fixed(1, 0) }
      assert_raises(NX::InvalidArgumentError) { batch.column(100) }
      assert_raises(NX::ClientError) { NX::Batch.decode(schema, 'not an Arrow stream', 1) }
    end

    def test_every_scalar_width_preserves_its_exact_bits
      fields = NX::Fields.new
      descriptions = [
        [NX::FieldType::U8, 'C', [0, 255]],
        [NX::FieldType::I8, 'c', [-128, 127]],
        [NX::FieldType::U16, 'S', [0, 65_535]],
        [NX::FieldType::I16, 's', [-32_768, 32_767]],
        [NX::FieldType::U32, 'L', [0, 4_294_967_295]],
        [NX::FieldType::I32, 'l', [-2_147_483_648, 2_147_483_647]],
        [NX::FieldType::U64, 'Q', [0, (1 << 64) - 1]],
        [NX::FieldType::I64, 'q', [-(1 << 63), (1 << 63) - 1]],
        [NX::FieldType::F32, 'L', [0x80000000, 0x7fc01234]],
        [NX::FieldType::F64, 'Q', [0x8000000000000000, 0x7ff8000000001234]],
        [NX::FieldType::BOOL, 'C', [0, 1]],
        [NX::FieldType::DATETIME, 'q', [-1, (1 << 63) - 1]]
      ]
      descriptions.each_with_index { |(type, _, _), index| fields.add("f#{index}", type) }
      builder = NX::BatchBuilder.new(fields.schema, 2)
      descriptions.each_with_index { |(_, directive, values), index| builder.fixed(index, 0, values.pack("#{directive}*")) }
      batch = builder.finish
      decoded = NX::Batch.decode(fields.schema, batch.ipc.get_string, 2)
      descriptions.each_with_index do |(_, directive, values), index|
        assert_equal values, batch.fixed(index, 0).unpack("#{directive}*")
        assert_equal values, decoded.fixed(index, 0).unpack("#{directive}*")
      end
      assert_equal [0, (1 << 64) - 1], decoded.column(6)
      assert_equal [-1, (1 << 63) - 1], decoded.column(11)
      assert_equal [false, true], decoded.column(10)
    end

    def test_an_ipc_slice_keeps_its_owner_until_the_last_view_is_collected
      # ANCHOR: ipc_lifetime
      batch = build(input_fields.schema)
      stream = batch.ipc
      bytes = stream.get_string
      slice = stream.slice(1, stream.size - 1)
      weak = WeakRef.new(batch)
      batch = nil
      stream = nil
      GC.start(full_mark: true, immediate_sweep: true)
      GC.compact
      assert weak.weakref_alive?
      assert_equal bytes.byteslice(1..), slice.get_string
      assert_raises(IO::Buffer::AccessError) { slice.set_string('x') }
      slice = nil
      assert_collected weak
      # ANCHOR_END: ipc_lifetime
    end

    def test_invalid_field_and_builder_inputs_are_typed_failures
      fields = NX::Fields.new
      assert_raises(NX::InvalidArgumentError) { fields.element(NX::FieldType::U8) }
      assert_raises(NX::InvalidArgumentError) { fields.add('bad', -1) }
      assert_raises(NX::InvalidArgumentError) { fields.add('bad', NX::FieldType::FIXED_LIST, 0) }
      fields.add('id', NX::FieldType::U32)
      builder = NX::BatchBuilder.new(fields.schema, 1)
      builder.fixed(0, 0, ''.b)
      assert_raises(NX::InvalidArgumentError) { builder.finish }
    end
  end
end
