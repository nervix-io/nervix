# frozen_string_literal: true

# One minitest result per completed teardown, under the shared host-suite inventory contract.
require_relative 'results'
require_relative 'support'

tier = ARGV.shift
abort 'choose exactly one tier: boundary or cluster' unless %w[boundary cluster].include?(tier)
Dir[File.join(__dir__, tier, '*_test.rb')].sort.each { |path| require path }
# A failed fresh-GEM_HOME smoke stops this tier before it opens any cluster session.
smoke = '/^test_00_packaged_gem_loads_in_a_fresh_gem_home$/'
exit 1 unless Minitest.run(['--seed', '1', '--name', smoke])

exit(Minitest.run(['--seed', '1', '--exclude', smoke, *ARGV]) ? 0 : 1)
