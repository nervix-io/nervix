# encoding: UTF-8
# frozen_string_literal: true

# Complete logical-value oracles for the native Ruby builder/reader and field descriptions.
module RubyProperties
  NX = Nervix::Client
  SCALARS = [
    ['U8', 1], ['I8', 1], ['U16', 2], ['I16', 2], ['U32', 4], ['I32', 4],
    ['U64', 8], ['I64', 8], ['F32', 4], ['F64', 8], ['BOOL', 1], ['DATETIME', 8]
  ].freeze

  def self.check(condition, message)
    raise message unless condition
  end

  def self.byte(input, index)
    input.empty? ? 0 : input.getbyte(index % input.bytesize)
  end

  def self.fields(input)
    fields = NX::Fields.new
    expected = []
    (byte(input, 0) % 8).times do |index|
      scalar = NX::FieldType.const_get((SCALARS.map(&:first) + %w[STRING BYTES])[byte(input, index + 1) % 14])
      levels = []
      (byte(input, index + 2) % 4).times do |depth|
        if byte(input, index + depth + 3).even?
          levels << [NX::FieldType::LIST, 0]
        else
          levels << [NX::FieldType::FIXED_LIST, 1 + byte(input, index + depth + 4) % 4]
        end
      end
      levels << [scalar, 0]
      name = "field_#{index}"
      nullable = byte(input, index + 5).odd?
      sensitive = byte(input, index + 6).odd?
      fields.add(name, *levels.first, nullable: nullable, sensitive: sensitive)
      levels.drop(1).each { |level| fields.element(*level) }
      expected << [name, levels.first.first, nullable, sensitive, levels]
    end
    schema = fields.schema
    actual = schema.fields.map do |field|
      [field.name, field.type, field.nullable, field.sensitive, field.levels.map { |level| [level.type, level.length] }]
    end
    check(actual == expected, 'a complete field description changed')
    check(schema.key_fields.empty? && schema.branch.nil?, 'row fields acquired a branch')
    fields.add('additional', NX::FieldType::U8)
    check(schema.fields.size == expected.size, 'a field snapshot changed after a draft edit')
  end

  def self.columns(input)
    rows = byte(input, 0) % 8
    fields = NX::Fields.new
    SCALARS.each { |name, _| fields.add(name.downcase, NX::FieldType.const_get(name), nullable: true, sensitive: byte(input, name.size).odd?) }
    fields.add('text', NX::FieldType::STRING, nullable: true)
    fields.add('raw', NX::FieldType::BYTES, nullable: true)
    fields.add('lists', NX::FieldType::LIST, nullable: true)
    fields.element(NX::FieldType::FIXED_LIST, 2)
    fields.element(NX::FieldType::I16)
    schema = fields.schema
    builder = NX::BatchBuilder.new(schema, rows)
    presence = Array.new(rows) { |row| byte(input, row + 1).even? }
    scalar_bytes = SCALARS.each_with_index.map do |(name, width), column|
      data = Array.new(rows * width) { |offset| byte(input, offset + column + 2) }.pack('C*')
      data = data.bytes.map { |value| value % 2 }.pack('C*') if name == 'BOOL'
      builder.states(column, presence)
      builder.fixed(column, 0, data)
      data
    end
    text = Array.new(rows) { |row| [0, byte(input, row + 3), 0x100 + byte(input, row + 4)].pack('U*') }
    binary = Array.new(rows) { |row| Array.new(byte(input, row + 5) % 8) { |index| byte(input, index + row) }.pack('C*') }
    [text, binary].each_with_index do |values, index|
      offsets = [0]
      values.each { |value| offsets << offsets.last + value.bytesize }
      builder.states(SCALARS.size + index, presence)
      builder.varlen(SCALARS.size + index, 0, offsets, values.join.b)
    end
    list_offsets = [0]
    rows.times { |row| list_offsets << list_offsets.last + byte(input, row + 6) % 3 }
    list_values = Array.new(list_offsets.last * 2) { |index| byte(input, index + 7) - 128 }
    builder.states(SCALARS.size + 2, presence)
    builder.offsets(SCALARS.size + 2, 0, list_offsets)
    builder.fixed(SCALARS.size + 2, 2, list_values.pack('s*'))
    batch = builder.finish
    decoded = NX::Batch.decode(schema, batch.ipc.get_string, rows)
    [batch, decoded].each do |current|
      check(current.row_count == rows, 'row count changed')
      descriptions = ->(value) do
        value.fields.map { |field| [field.name, field.type, field.nullable, field.sensitive, field.levels.map { |level| [level.type, level.length] }] }
      end
      check(descriptions.call(current.schema) == descriptions.call(schema), 'a complete field description changed')
      SCALARS.each_with_index do |(name, width), column|
        actual = current.fixed(column, 0)
        typed = current.column(column)
        states = current.states(column)
        directive = { 'U8' => 'C', 'I8' => 'c', 'U16' => 'S', 'I16' => 's',
                      'U32' => 'L', 'I32' => 'l', 'U64' => 'Q', 'I64' => 'q',
                      'F32' => 'f', 'F64' => 'd', 'BOOL' => 'C', 'DATETIME' => 'q' }.fetch(name)
        expected = scalar_bytes[column].unpack("#{directive}*")
        rows.times do |row|
          check(states[row] == (presence[row] ? NX::CellState::VALUE : NX::CellState::NULL), 'scalar validity changed')
          unless presence[row]
            check(typed[row].nil?, 'a scalar null became a value')
            next
          end
          check(actual.byteslice(row * width, width) == scalar_bytes[column].byteslice(row * width, width), 'complete scalar bits changed')
          value = name == 'BOOL' ? expected[row] != 0 : expected[row]
          if value.is_a?(Float)
            if value.nan?
              check(typed[row].is_a?(Float) && typed[row].nan?, 'a typed NaN changed')
            else
              check([typed[row]].pack('d') == [value].pack('d'), 'a typed float or signed zero changed')
            end
          else
            check(typed[row] == value, 'a complete typed scalar changed')
          end
        end
      end
      [text, binary].each_with_index do |values, index|
        offsets, data = current.varlen(SCALARS.size + index, 0)
        actual = current.column(SCALARS.size + index)
        rows.times do |row|
          if presence[row]
            check(actual[row].b == values[row].b, 'a typed variable value changed')
            check(data.byteslice(offsets[row]...offsets[row + 1]) == values[row].b, 'a bulk variable value changed')
          else
            check(actual[row].nil?, 'a variable null became a value')
          end
        end
      end
      check(current.offsets(SCALARS.size + 2, 0) == list_offsets, 'list shape changed')
      actual = current.column(SCALARS.size + 2)
      rows.times do |row|
        expected = if presence[row]
                     list_values.slice(list_offsets[row] * 2...list_offsets[row + 1] * 2).each_slice(2).to_a
                   end
        check(actual[row] == expected, 'nested logical values or validity changed')
      end
    end
  end
end
