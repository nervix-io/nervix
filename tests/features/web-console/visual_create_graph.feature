@visual_create_graph @slots_3
Feature: A complete graph is built from the console's visual forms

  # The transaction must survive the console following a new leader, so this scenario needs a
  # three-node cluster whose leadership moves while the transaction is open.
  Scenario: Visual forms stage a whole graph in one transaction that commits across a leader change
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When leadership is transferred from node "{{new_leader}}" to node "{{old_leader}}"
    And the web console is opened on node "{{old_leader}}"
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='schema']" is clicked
    And selector ".create-name" is filled with "graph_event"
    And selector ".create-add-field" is clicked
    And selector ".create-field-name" is filled with "message"
    And selector ".create-field-type [data-type='STRING']" is clicked
    Then selector ".create-preview" contains "CREATE SCHEMA graph_event"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 1"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='wire-json-schema']" is clicked
    And selector ".create-name" is filled with "graph_wire"
    And selector ".create-mode-options [data-value='STRICT']" is clicked
    And selector ".create-add-field" is clicked
    And selector ".create-field-name" is filled with "message"
    And selector ".create-field-type [data-type='string']" is clicked
    Then selector ".create-preview" contains "CREATE WIRE JSON SCHEMA graph_wire MODE STRICT"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 2"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='codec']" is clicked
    And selector ".create-name" is filled with "graph_codec"
    And selector ".create-codec-format [data-format='wire-json']" is clicked
    And selector ".create-codec-schema .create-choice-search" is filled with "graph_event"
    Then selector ".create-codec-schema [data-value='graph_event']" exists
    When selector ".create-codec-schema [data-value='graph_event']" is clicked
    And selector ".create-codec-wire-schema .create-choice-search" is filled with "graph_wire"
    Then selector ".create-codec-wire-schema [data-value='graph_wire']" exists
    When selector ".create-codec-wire-schema [data-value='graph_wire']" is clicked
    Then selector ".create-preview" contains "CREATE WIRE JSON CODEC graph_codec FROM WIRE SCHEMA graph_wire"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 3"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='relay']" is clicked
    And selector ".create-name" is filled with "graph_input"
    And selector ".create-schema-ref .create-choice-search" is filled with "graph_event"
    Then selector ".create-schema-ref [data-value='graph_event']" exists
    When selector ".create-schema-ref [data-value='graph_event']" is clicked
    And selector ".create-branching-options [data-value='UNBRANCHED']" is clicked
    Then selector ".create-preview" contains "CREATE RELAY graph_input SCHEMA graph_event UNBRANCHED"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 4"
    When selector ".create-name" is filled with "graph_output"
    Then selector ".create-preview" contains "CREATE RELAY graph_output SCHEMA graph_event UNBRANCHED"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 5"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='vhost']" is clicked
    And selector ".create-name" is filled with "graph_host"
    And selector ".create-vhost-hostname" is filled with "graph-{{test_id}}.example.com"
    Then selector ".create-preview" contains "CREATE VHOST graph_host graph-{{test_id}}.example.com"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 6"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='endpoint']" is clicked
    And selector ".create-name" is filled with "graph_endpoint"
    And selector ".create-endpoint-vhost .create-choice-search" is filled with "graph_host"
    Then selector ".create-endpoint-vhost [data-value='graph_host']" exists
    When selector ".create-endpoint-vhost [data-value='graph_host']" is clicked
    And selector ".create-endpoint-path" is filled with "/graph"
    And selector ".create-endpoint-kind [data-kind='http']" is clicked
    Then selector ".create-preview" contains "CREATE HTTP ENDPOINT graph_endpoint ON graph_host PATH '/graph'"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 7"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "graph_ingestor"
    And selector ".create-ingestor-source [data-source='http-endpoint']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "graph_endpoint"
    Then selector ".create-ingestor-source-ref [data-value='graph_endpoint']" exists
    When selector ".create-ingestor-source-ref [data-value='graph_endpoint']" is clicked
    And selector ".create-ingestor-quiesce [data-quiesce='buffer']" is clicked
    And selector ".create-ingestor-buffer-size" is filled with "1MiB"
    And selector ".create-ingestor-codec .create-choice-search" is filled with "graph_codec"
    Then selector ".create-ingestor-codec [data-value='graph_codec']" exists
    When selector ".create-ingestor-codec [data-value='graph_codec']" is clicked
    And selector ".create-ingestor-timestamp [data-timestamp='now']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branching [data-branch='unbranched']" is clicked
    And selector ".create-ingestor-route .create-ingestor-relay .create-choice-search" is filled with "graph_input"
    Then selector ".create-ingestor-route .create-ingestor-relay [data-value='graph_input']" exists
    When selector ".create-ingestor-route .create-ingestor-relay [data-value='graph_input']" is clicked
    And selector ".create-ingestor-route .create-ingestor-inherit [data-inherit='all']" is clicked
    And selector ".create-ingestor-route .create-ingestor-flush [data-flush='immediate']" is clicked
    And selector ".create-ingestor-route .create-ingestor-message-error [data-error='log']" is clicked
    And selector ".create-ingestor-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE HTTP ENDPOINT INGESTOR graph_ingestor FROM graph_endpoint"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 8"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='junction']" is clicked
    And selector ".create-name" is filled with "graph_junction"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "graph_input"
    Then selector ".create-processor-input [data-value='graph_input']" exists
    When selector ".create-processor-input [data-value='graph_input']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "graph_output"
    Then selector ".create-processor-route-relay [data-value='graph_output']" exists
    When selector ".create-processor-route-relay [data-value='graph_output']" is clicked
    And selector ".create-processor-inherit [data-inherit='all']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE JUNCTION graph_junction"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 9"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='placement']" is clicked
    And selector ".create-name" is filled with "graph_local"
    And selector ".create-placement-coverage [data-scope='corridor']" is clicked
    And selector ".create-placement-from .create-choice-search" is filled with "graph_input"
    Then selector ".create-placement-from [data-value='graph_input']" exists
    And selector ".create-placement-from [data-value='graph_ingestor']" does not exist
    When selector ".create-placement-from [data-value='graph_input']" is clicked
    And selector ".create-placement-to .create-choice-search" is filled with "graph_output"
    Then selector ".create-placement-to [data-value='graph_output']" exists
    When selector ".create-placement-to [data-value='graph_output']" is clicked
    And selector ".create-placement-options [data-value='REQUIRE COLOCATION']" is clicked
    And selector ".create-placement-rank" is filled with "1"
    Then selector ".create-preview" contains "CREATE PLACEMENT graph_local FROM graph_input TO graph_output REQUIRE COLOCATION RANK 1;"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "position 10"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "10 accepted"
    And selector ".transaction-inspector .inspector-item[data-name='graph_junction']" contains "JUNCTION"
    And selector ".transaction-inspector .inspector-item[data-name='graph_ingestor']" contains "INGESTOR"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{new_leader}}'"
    And selector ".transaction-inspector .inspector-summary" contains "10 accepted"
    When selector ".transaction-inspector .inspector-toolbar button:has-text('Refresh')" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "CURRENT"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION graph_subscription TO graph_output;
      START;
      """
    And http payload is posted to host "graph-{{test_id}}.example.com" path "/graph"
      """
      {"message":"built visually"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "message":"built visually"
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT graph_local;
      """
    Then the last command output contains
      """
      policy: REQUIRE COLOCATION
      """
    And the last command output contains
      """
      pair: graph_input -> graph_output
      """
    And the last command output contains
      """
      connected: true
      """
