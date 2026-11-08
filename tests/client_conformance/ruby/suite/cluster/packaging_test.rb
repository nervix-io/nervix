# frozen_string_literal: true

module RubySuite
  class ClusterPackagingTest < Boundary
    def test_00_packaged_gem_loads_in_a_fresh_gem_home
      assert_packaged_gem
    end
  end
end
