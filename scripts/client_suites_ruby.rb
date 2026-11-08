# frozen_string_literal: true

# Parse Ruby with its bundled Prism parser. A member reference needs an actual constant,
# constructor, or receiver whose native class the test explicitly asserts at runtime.
require 'prism'
require 'json'
require 'set'
require 'pathname'

class RubySuiteAnalysis
  NativeValue = Struct.new(:name, :singleton)

  def initialize(suite, native_module)
    @suite = Pathname(suite)
    @native_module = native_module
    @constants = {}
    @classes = {}
    @superclasses = {}
    @references = Set.new
    @tests = {}
    @problems = []
    @trees = Dir[@suite.join('**/*.rb')].sort.map do |path|
      parsed = Prism.parse_file(path)
      parsed.errors.each { |error| @problems << "#{path}:#{error.location.start_line}: #{error.message}" }
      [path, parsed.value]
    end
  end

  def run
    @trees.each { |path, node| declarations(node, [], path) }
    @superclasses.each { |name, (node, scope)| @classes[name] = resolved_constant(node, scope) }
    @trees.each { |path, node| visit(node, {}, [], path) }
    { references: @references.to_a.sort, tests: @tests.sort.to_h, problems: @problems }
  end

  private

  def constant_parts(node)
    return [node.name.to_s] if node.is_a?(Prism::ConstantReadNode)
    return node.full_name_parts.map(&:to_s) if node.is_a?(Prism::ConstantPathNode)

    nil
  end

  def resolved_constant(node, scope)
    parts = constant_parts(node)
    return nil unless parts

    head = parts.first
    scope.length.downto(0) do |length|
      candidate = (scope.take(length) + [head]).join('::')
      if @constants.key?(candidate)
        base = @constants[candidate]
        return nil unless base

        return ([base] + parts.drop(1)).join('::')
      end
    end
    parts.join('::')
  end

  def declared_name(node, scope)
    parts = constant_parts(node)
    return nil unless parts

    return parts.join('::') if parts.length > 1

    (scope + parts).join('::')
  end

  def declarations(node, scope, path)
    return unless node

    case node
    when Prism::ModuleNode, Prism::ClassNode
      name = declared_name(node.constant_path, scope)
      return unless name

      @constants[name] = name
      @superclasses[name] = [node.superclass, scope] if node.is_a?(Prism::ClassNode)
      declarations(node.body, name.split('::'), path)
    when Prism::ConstantWriteNode
      @constants[(scope + [node.name.to_s]).join('::')] = resolved_constant(node.value, scope)
    when Prism::DefNode
      # Constants in a method are not permitted by Ruby and are never declarations here.
    else
      node.compact_child_nodes.each { |child| declarations(child, scope, path) }
    end
  end

  def native_constant(node, scope)
    name = resolved_constant(node, scope)
    return nil unless name

    if name == 'Nervix' || name.start_with?("#{@native_module}::") || name == @native_module
      parts = name.split('::')
      1.upto(parts.length) { |length| @references << parts.take(length).join('::') }
    end
    NativeValue.new(name, true)
  end

  def minitest_class?(name)
    seen = Set.new
    until name.nil? || seen.include?(name)
      return true if name == 'Minitest::Test'

      seen << name
      name = @classes[name]
    end
    false
  end

  def variable_key(node)
    case node
    when Prism::LocalVariableReadNode, Prism::LocalVariableWriteNode
      [:local, node.name]
    when Prism::InstanceVariableReadNode, Prism::InstanceVariableWriteNode
      [:instance, node.name]
    when Prism::CallNode
      [:call, node.name] if node.receiver.nil? && node.arguments.nil?
    end
  end

  def constrain(node, value, environment)
    key = variable_key(node)
    environment[key] = value if key
  end

  def call(node, environment, scope, path)
    receiver = visit(node.receiver, environment, scope, path)
    arguments = node.arguments&.arguments || []
    values = arguments.map { |argument| visit(argument, environment, scope, path) }
    if node.receiver.nil? && %i[assert_instance_of assert_kind_of].include?(node.name)
      expected = values[0]
      if expected&.singleton && expected.name.start_with?("#{@native_module}::")
        constrain(arguments[1], NativeValue.new(expected.name, false), environment)
      end
    end
    if receiver && receiver.name.start_with?("#{@native_module}::")
      separator = receiver.singleton ? '.' : '#'
      @references << "#{receiver.name}#{separator}#{node.name}"
    end
    visit(node.block, environment.dup, scope, path) if node.block
    if node.receiver.nil? && node.name == :assert_raises
      expected = values[0]
      return NativeValue.new(expected.name, false) if expected&.singleton
    end
    if receiver&.singleton && %i[new connect].include?(node.name)
      return NativeValue.new(receiver.name, false)
    end
    return environment[[:call, node.name]] if node.receiver.nil? && arguments.empty?

    nil
  end

  def visit(node, environment, scope, path)
    return nil unless node

    case node
    when Prism::ConstantReadNode, Prism::ConstantPathNode
      native_constant(node, scope)
    when Prism::ConstantWriteNode
      native_constant(node.value, scope)
    when Prism::ModuleNode, Prism::ClassNode
      name = declared_name(node.constant_path, scope)
      visit(node.body, {}, name.split('::'), path) if name
    when Prism::DefNode
      if node.receiver.nil? && node.name.to_s.start_with?('test_') && minitest_class?(scope.join('::'))
        tier = Pathname(path).relative_path_from(@suite).each_filename.first
        identity = "#{scope.join('::')}##{node.name}"
        @problems << "Ruby suite defines #{identity} twice" if @tests.key?(identity)
        @problems << "#{path}: #{identity} is outside the boundary and cluster tiers" unless %w[boundary cluster].include?(tier)
        @tests[identity] = tier
      end
      visit(node.body, {}, scope, path)
    when Prism::LocalVariableWriteNode, Prism::InstanceVariableWriteNode
      value = visit(node.value, environment, scope, path)
      constrain(node, value, environment)
      value
    when Prism::LocalVariableReadNode, Prism::InstanceVariableReadNode
      environment[variable_key(node)]
    when Prism::CallNode
      call(node, environment, scope, path)
    when Prism::StringNode, Prism::SymbolNode
      # Neither a diagnostic string nor a symbol is a binding reference.
      nil
    else
      value = nil
      node.compact_child_nodes.each { |child| value = visit(child, environment, scope, path) }
      value
    end
  end
end

if $PROGRAM_NAME == __FILE__
  puts JSON.generate(RubySuiteAnalysis.new(ARGV.fetch(0), ARGV.fetch(1)).run)
end
