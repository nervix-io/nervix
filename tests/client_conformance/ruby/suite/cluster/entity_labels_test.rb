# frozen_string_literal: true

module RubySuite
  class EntityLabelsTest < ClusterTest
    def test_entity_labels_keep_exact_values_revisions_and_staged_views
      name = unique('labelled')
      completed("CREATE SCHEMA #{name} ( value U32 ) LABELS { 'empty' = '', 'owner' = ' Δ ' };")
      # ANCHOR: entity_labels
      outcome = execute("SHOW LABELS ON SCHEMA #{name} FORMAT TEXT;")
      assert_instance_of NX::Outcome, outcome
      labels = outcome.entity_labels
      assert_instance_of NX::EntityLabelsInspection, labels
      assert_equal [cluster.domain, 'SCHEMA', name], [labels.domain, labels.entity_kind, labels.entity_name]
      assert_equal({ 'empty' => '', 'owner' => ' Δ ' }, labels.labels)
      assert_equal NX::EntityLabelViewKind::COMMITTED, labels.view
      assert_operator labels.revision, :>=, 0
      assert_nil labels.transaction_id
      assert_nil labels.position
      assert_nil labels.planning_basis
      assert_operator labels.policy_revision, :>, 0
      assert_equal [64, 128, 1024, 16_384], [labels.max_count, labels.max_key_bytes, labels.max_value_bytes, labels.max_bytes]
      snapshot = labels.labels
      snapshot['owner'] = 'only the returned hash changed'
      assert_equal ' Δ ', labels.labels.fetch('owner')
      completed('BEGIN;')
      completed("SET LABELS ON SCHEMA #{name} { 'owner' = 'Platform', 'team/path' = 'east' };")
      staged = execute("SHOW LABELS ON SCHEMA #{name};").entity_labels
      assert_instance_of NX::EntityLabelsInspection, staged
      assert_equal NX::EntityLabelViewKind::STAGED, staged.view
      assert_nil staged.revision
      refute_empty staged.transaction_id
      assert_equal 1, staged.position
      assert_equal 32, staged.planning_basis.bytesize
      assert_equal({ 'empty' => '', 'owner' => 'Platform', 'team/path' => 'east' }, staged.labels)
      completed('REVERT;')
      committed = execute("SHOW LABELS ON SCHEMA #{name} FORMAT JSON;").entity_labels
      assert_equal({ 'empty' => '', 'owner' => ' Δ ' }, committed.labels)
      assert_equal NX::EntityLabelViewKind::COMMITTED, committed.view
      assert_operator committed.revision, :>=, labels.revision
      outcome = committed = nil
      GC.start
      GC.compact
      assert_equal({ 'empty' => '', 'owner' => ' Δ ' }, labels.labels)
      assert_equal({ 'empty' => '', 'owner' => 'Platform', 'team/path' => 'east' }, staged.labels)
      refute_empty staged.transaction_id
      assert_equal 1, staged.position
      # ANCHOR_END: entity_labels
    end
  end
end
