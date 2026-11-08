# frozen_string_literal: true

require 'rbconfig'
require_relative 'nervix_client/build'

unless RUBY_ENGINE == 'ruby' && RbConfig::CONFIG.fetch('ruby_version') == Nervix::Client::BUILD_ABI
  raise LoadError, "this Nervix client gem requires CRuby #{Nervix::Client::BUILD_ABI}; rebuild it with the Ruby interpreter that will load it"
end

require_relative 'nervix_client_ruby'
