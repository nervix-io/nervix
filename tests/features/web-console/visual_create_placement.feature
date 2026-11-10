@visual_create_placement @slots_3
Feature: Placement rules are created through the console's visual forms

  Scenario Outline: A placement popup previews and submits a domain-wide rule
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='placement']" is pressed with "Enter"
    Then selector ".create-dialog" contains "Create placement"
    And selector ".create-scope" contains "{{domain}}"
    And selector ".create-placement-coverage [data-scope='domain'].active" contains "Entire domain"
    When selector ".create-name" is filled with "domain_policy"
    And selector ".create-placement-options [data-value='PREFER COLOCATION']" is pressed with "Enter"
    And selector ".create-placement-rank" is filled with "2"
    Then selector ".create-preview" contains "CREATE PLACEMENT domain_policy DOMAIN WIDE PREFER COLOCATION RANK 2;"
    When selector ".create-submit" is pressed with "Enter"
    Then selector ".create-status" contains "Completed"
    And selector ".terminal" contains "CREATE PLACEMENT domain_policy DOMAIN WIDE PREFER COLOCATION RANK 2;"
    When selector ".create-close" is pressed with "Enter"
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE PLACEMENT domain_policy;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT domain_policy DOMAIN WIDE PREFER COLOCATION RANK 2;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_placement_corridor
  Scenario Outline: A corridor rule orders typed runtime-node members and is applied under the placement contract
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_place_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_place_wire MODE STRICT (message string);
      CREATE WIRE JSON CODEC visual_place_codec FROM WIRE SCHEMA visual_place_wire TO SCHEMA visual_place_event;
      CREATE RELAY visual_place_input SCHEMA visual_place_event UNBRANCHED;
      CREATE RELAY visual_place_middle SCHEMA visual_place_event UNBRANCHED;
      CREATE RELAY visual_place_output SCHEMA visual_place_event UNBRANCHED;
      CREATE RELAY visual_place_shared SCHEMA visual_place_event UNBRANCHED;
      CREATE VHOST visual_place_host place-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_place_endpoint ON visual_place_host PATH '/place';
      CREATE HTTP ENDPOINT INGESTOR visual_place_listener FROM visual_place_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_place_codec
        TO visual_place_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION visual_place_junction FROM visual_place_input UNBRANCHED
        TO visual_place_middle INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION visual_place_finish FROM visual_place_middle UNBRANCHED
        TO visual_place_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION visual_place_shared FROM visual_place_shared UNBRANCHED
        TO visual_place_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='placement']" is clicked
    And selector ".create-name" is filled with "visual_corridor"
    And selector ".create-placement-coverage [data-scope='corridor']" is clicked
    Then selector ".create-placement-coverage [data-scope='corridor'].active" contains "Corridor"
    When selector ".create-placement-from .create-choice-search" is filled with "visual_place"
    Then selector ".create-placement-from [data-value='visual_place_junction']" exists
    And selector ".create-placement-from [data-value='visual_place_input']" exists
    And selector ".create-placement-from [data-value='visual_place_finish']" exists
    And selector ".create-placement-from [data-value='visual_place_listener']" does not exist
    And selector ".create-placement-from [data-value='visual_place_event']" does not exist
    And selector ".create-placement-from [data-value='visual_place_codec']" does not exist
    And selector ".create-placement-from [data-value='visual_place_shared']" does not exist
    When selector ".create-placement-from [data-value='visual_place_input']" is clicked
    And selector ".create-placement-from [data-value='visual_place_junction']" is clicked
    And selector ".create-placement-to .create-choice-search" is filled with "visual_place_output"
    Then selector ".create-placement-to [data-value='visual_place_output']" exists
    When selector ".create-placement-to [data-value='visual_place_output']" is clicked
    Then selector ".create-placement-from-members .create-placement-member:nth-child(1)" contains "visual_place_input"
    And selector ".create-placement-from-members .create-placement-member:nth-child(2)" contains "visual_place_junction"
    When selector ".create-placement-from-members [aria-label='Move visual_place_junction up']" is clicked
    Then selector ".create-placement-from-members .create-placement-member:nth-child(1)" contains "visual_place_junction"
    And selector ".create-placement-options [data-value='SUGGEST SEPARATION']" exists
    When selector ".create-placement-options [data-value='SUGGEST SEPARATION']" is clicked
    And selector ".create-placement-rank" is filled with "3"
    Then selector ".create-preview" contains "CREATE PLACEMENT visual_corridor FROM visual_place_junction, visual_place_input TO visual_place_output SUGGEST SEPARATION RANK 3;"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE PLACEMENT visual_corridor;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT visual_corridor FROM visual_place_junction, visual_place_input TO visual_place_output SUGGEST SEPARATION RANK 3;
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT visual_corridor;
      """
    Then the last command output contains
      """
      policy: SUGGEST SEPARATION
      """
    And the last command output contains
      """
      pair: visual_place_input -> visual_place_output
      """
    And the last command output contains
      """
      connected: true
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_placement_errors
  Scenario: A corridor draft reports rank, empty-side and member reference errors inline and stays editable
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_refuse_event (message STRING);
      CREATE RELAY visual_refuse_input SCHEMA visual_refuse_event UNBRANCHED;
      CREATE RELAY visual_refuse_output SCHEMA visual_refuse_event UNBRANCHED;
      CREATE RELAY visual_refuse_dropped SCHEMA visual_refuse_event UNBRANCHED;
      CREATE JUNCTION visual_refuse_junction FROM visual_refuse_input UNBRANCHED
        TO visual_refuse_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='placement']" is clicked
    And selector ".create-name" is filled with "visual_refused"
    And selector ".create-placement-coverage [data-scope='corridor']" is clicked
    And selector ".create-placement-from .create-choice-search" is filled with "visual_refuse_input"
    Then selector ".create-placement-from [data-value='visual_refuse_input']" exists
    When selector ".create-placement-from [data-value='visual_refuse_input']" is clicked
    And selector ".create-placement-options [data-value='REQUIRE COLOCATION']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Choose at least one TO member"
    When selector ".create-placement-to .create-choice-search" is filled with "visual_refuse_dropped"
    Then selector ".create-placement-to [data-value='visual_refuse_dropped']" exists
    When selector ".create-placement-to [data-value='visual_refuse_dropped']" is clicked
    And selector ".create-placement-rank" is filled with "0"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Rank must be a positive 64-bit integer"
    When selector ".create-placement-rank" is filled with ""
    And these Plexus commands are executed on the leader node
      """
      DROP RELAY visual_refuse_dropped;
      """
    And selector ".create-submit" is clicked
    Then selector ".create-error" contains "placement member 'visual_refuse_dropped' does not exist"
    And selector ".create-name" has value "visual_refused"
    And selector ".create-placement-to-members .create-placement-member" contains "visual_refuse_dropped"
    When selector ".create-placement-to-members [aria-label='Remove visual_refuse_dropped']" is clicked
    And selector ".create-placement-to .create-choice-search" is filled with "visual_refuse_output"
    Then selector ".create-placement-to [data-value='visual_refuse_output']" exists
    When selector ".create-placement-to [data-value='visual_refuse_output']" is clicked
    Then selector ".create-preview" contains "CREATE PLACEMENT visual_refused FROM visual_refuse_input TO visual_refuse_output REQUIRE COLOCATION;"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"

  @visual_placement_stale_scope
  Scenario: Corridor members selected in another domain must be selected again
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_other;
      CREATE SCHEMA visual_scope_event (message STRING);
      CREATE RELAY visual_scope_input SCHEMA visual_scope_event UNBRANCHED;
      CREATE RELAY visual_scope_output SCHEMA visual_scope_event UNBRANCHED;
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='placement']" is clicked
    And selector ".create-name" is filled with "visual_scoped"
    And selector ".create-placement-coverage [data-scope='corridor']" is clicked
    And selector ".create-placement-from .create-choice-search" is filled with "visual_scope_input"
    Then selector ".create-placement-from [data-value='visual_scope_input']" exists
    When selector ".create-placement-from [data-value='visual_scope_input']" is clicked
    And selector ".create-placement-to .create-choice-search" is filled with "visual_scope_output"
    Then selector ".create-placement-to [data-value='visual_scope_output']" exists
    When selector ".create-placement-to [data-value='visual_scope_output']" is clicked
    And selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='placement']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{domain}}_other"
    And selector ".create-placement-from-members .create-placement-member.invalid" contains "visual_scope_input"
    And selector ".create-placement-to-members .create-placement-member.invalid" contains "visual_scope_output"
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "changed context"
    And selector ".terminal" does not contain "created placement 'visual_scoped'"

  @visual_placement_contextual
  Scenario: Graph and sidebar actions open their family's form in the selected domain
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_context_event (message STRING);
      CREATE RELAY visual_context_input SCHEMA visual_context_event UNBRANCHED;
      CREATE RELAY visual_context_output SCHEMA visual_context_event UNBRANCHED;
      CREATE JUNCTION visual_context_junction FROM visual_context_input UNBRANCHED
        TO visual_context_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    And selector ".node-hit[data-label='visual_context_junction']" exists
    When selector ".node-hit[data-label='visual_context_junction']" is clicked
    Then selector ".graph-action-menu" contains "PLACE"
    When selector ".graph-action-menu [data-action='place']" is clicked
    Then selector ".create-dialog" contains "Create placement"
    And selector ".create-scope" contains "{{domain}}"
    And selector ".create-placement-coverage [data-scope='corridor'].active" exists
    And selector ".create-placement-from-members .create-placement-member" contains "visual_context_junction"
    When selector ".create-name" is filled with "visual_context_local"
    And selector ".create-placement-to .create-choice-search" is filled with "visual_context_output"
    Then selector ".create-placement-to [data-value='visual_context_output']" exists
    When selector ".create-placement-to [data-value='visual_context_output']" is clicked
    And selector ".create-placement-options [data-value='PREFER COLOCATION']" is clicked
    Then selector ".create-preview" contains "CREATE PLACEMENT visual_context_local FROM visual_context_junction TO visual_context_output PREFER COLOCATION;"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector "#sidebar-create-schema" is clicked
    Then selector ".create-dialog" contains "Create schema"
    And selector ".create-scope" contains "{{domain}}"
    When selector ".create-close" is clicked
    Then selector "#sidebar-create-schema" is focused
    When selector "#sidebar-create-wire-schema" is clicked
    And selector ".sidebar-create-menu [data-create-kind='wire-cbor-schema']" is clicked
    Then selector ".create-dialog" contains "Create wire CBOR schema"
    When selector ".create-close" is clicked
    And selector "#sidebar-create-codec" is clicked
    Then selector ".create-dialog" contains "Create codec"
    When selector ".create-close" is clicked
    And selector "#sidebar-create-client" is clicked
    Then selector ".create-dialog" contains "Create client"
    When selector ".create-close" is clicked
    And selector "#sidebar-create-vhost" is clicked
    Then selector ".create-dialog" contains "Create VHOST"
    When selector ".create-close" is clicked
    And selector "#sidebar-create-endpoint" is clicked
    Then selector ".create-dialog" contains "Create endpoint"
    And selector ".create-scope" contains "{{domain}}"
