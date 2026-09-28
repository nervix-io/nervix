Feature: Web console visual hash map and Roto UDF creation

  @visual_lookup_create
  Scenario Outline: A visual hash map loads a resource before it can be queried
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "visual_lookup" containing
      """
      {
        "lookup.jsonl": "{\"id\":\"one\",\"value\":42}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE visual_lookup_bundle;
      UPLOAD RESOURCE visual_lookup_bundle VERSION '{{visual_lookup}}';
      CREATE SCHEMA visual_lookup_entry (id STRING, value I64);
      CREATE WIRE JSON SCHEMA visual_lookup_wire MODE STRICT (id string, value integer);
      CREATE CODEC visual_lookup_codec FROM WIRE JSON SCHEMA visual_lookup_wire TO SCHEMA visual_lookup_entry;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='hash-map']" is clicked
    And selector ".create-name" is filled with "visual_by_id"
    And selector ".create-hash-resource .create-choice-search" is filled with "visual_lookup_bundle"
    Then selector ".create-hash-resource [data-value='visual_lookup_bundle']" exists
    When selector ".create-hash-resource [data-value='visual_lookup_bundle']" is clicked
    Then selector ".create-hash-version [data-value='LATEST']" exists
    When selector ".create-hash-version [data-value='LATEST']" is clicked
    And selector ".create-hash-codec .create-choice-search" is filled with "visual_lookup_codec"
    Then selector ".create-hash-codec [data-value='visual_lookup_codec']" exists
    When selector ".create-hash-codec [data-value='visual_lookup_codec']" is clicked
    Then selector ".create-hash-key [data-value='id']" exists
    When selector ".create-hash-key [data-value='id']" is clicked
    And selector ".create-hash-path" is filled with "lookup.jsonl"
    Then selector ".create-preview" contains "CREATE HASH MAP visual_by_id KEY id FROM RESOURCE visual_lookup_bundle VERSION LATEST PATH 'lookup.jsonl' DECODE USING visual_lookup_codec"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "LOOKUP visual_by_id KEY 'one';"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains
      """
      "value":42
      """
    When selector ".prompt-row input" is filled with "SHOW CREATE HASH MAP visual_by_id;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "VERSION 1 PATH 'lookup.jsonl'"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_lookup_recovery
  Scenario: A failed hash map load retains the entered draft
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "visual_lookup_mixed" containing
      """
      {
        "bad.jsonl": "{not-json}\n",
        "good.jsonl": "{\"id\":\"one\",\"value\":42}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE visual_lookup_bundle;
      UPLOAD RESOURCE visual_lookup_bundle VERSION '{{visual_lookup_mixed}}';
      CREATE SCHEMA visual_lookup_entry (id STRING, value I64);
      CREATE WIRE JSON SCHEMA visual_lookup_wire MODE STRICT (id string, value integer);
      CREATE CODEC visual_lookup_codec FROM WIRE JSON SCHEMA visual_lookup_wire TO SCHEMA visual_lookup_entry;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='hash-map']" is clicked
    And selector ".create-name" is filled with "recover_by_id"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "resource"
    When selector ".create-hash-resource .create-choice-search" is filled with "visual_lookup_bundle"
    Then selector ".create-hash-resource [data-value='visual_lookup_bundle']" exists
    When selector ".create-hash-resource [data-value='visual_lookup_bundle']" is clicked
    And selector ".create-hash-version [data-value='1']" is clicked
    And selector ".create-hash-codec .create-choice-search" is filled with "visual_lookup_codec"
    Then selector ".create-hash-codec [data-value='visual_lookup_codec']" exists
    When selector ".create-hash-codec [data-value='visual_lookup_codec']" is clicked
    And selector ".create-hash-key [data-value='id']" is clicked
    And selector ".create-hash-path" is filled with "bad.jsonl"
    And selector ".create-submit" is clicked
    Then selector ".create-error" contains "failed to decode lookup"
    And selector ".create-hash-path" has value "bad.jsonl"
    And selector ".create-name" has value "recover_by_id"

  @visual_udf_create
  Scenario: A tested Roto UDF is usable immediately after visual creation
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_udf_input (value I64);
      CREATE SCHEMA visual_udf_output (result I64);
      CREATE WIRE JSON SCHEMA visual_udf_wire MODE STRICT (value integer);
      CREATE CODEC visual_udf_codec FROM WIRE JSON SCHEMA visual_udf_wire TO SCHEMA visual_udf_input;
      CREATE RELAY visual_udf_inputs SCHEMA visual_udf_input UNBRANCHED;
      CREATE RELAY visual_udf_outputs SCHEMA visual_udf_output UNBRANCHED;
      CREATE VHOST visual_udf_edge udf-{{test_id}}.example.com;
      CREATE ENDPOINT visual_udf_ingress ON visual_udf_edge PATH '/udf' TYPE HTTP;
      CREATE INGESTOR visual_udf_source FROM ENDPOINT visual_udf_ingress MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_udf_codec TO visual_udf_inputs INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='udf']" is clicked
    And selector ".create-name" is filled with "visual_add_one"
    And selector ".create-udf-add-argument" is clicked
    And selector ".create-udf-argument-name" is filled with "value"
    And selector ".create-udf-argument-type [data-type='I64']" is clicked
    And selector ".create-udf-return-type [data-type='I64']" is clicked
    And selector ".create-udf-code" is filled with text
      """
      fn visual_add_one(value: I64Column) -> I64Column {
          value.add_s(1)
      }

      // A quoted "$roto$" remains inside the source.
      test source_is_valid {
          if 1 == 1 { accept } else { reject }
      }
      """
    And selector ".create-udf-volatile" is clicked
    Then selector ".create-preview" contains "CREATE UDF visual_add_one"
    And selector ".create-preview" contains "ROTO_0_13"
    And selector ".create-preview" contains "VOLATILE"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE UDF visual_add_one;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "test source_is_valid"
    And selector ".terminal" contains "quoted"
    And selector ".terminal" contains "VOLATILE"
    When selector ".prompt-row input" is filled with "CREATE JUNCTION visual_udf_apply FROM visual_udf_inputs UNBRANCHED TO visual_udf_outputs SET result = udf::visual_"
    Then selector ".suggestions" contains "visual_add_one"
    When selector ".prompt-row input" is filled with "CREATE JUNCTION visual_udf_apply FROM visual_udf_inputs UNBRANCHED TO visual_udf_outputs SET result = co"
    Then selector ".suggestions" contains "coalesce"
    When these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION visual_udf_apply FROM visual_udf_inputs UNBRANCHED TO visual_udf_outputs SET result = udf::visual_add_one(input.value) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION visual_udf_sub TO visual_udf_outputs;
      START;
      """
    And http payload is posted to host "udf-{{test_id}}.example.com" path "/udf"
      """
      {"value":41}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "result":42
      """

  @visual_udf_recovery
  Scenario: A rejected Roto test leaves its source and typed signature editable
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='udf']" is clicked
    And selector ".create-name" is filled with "visual_reject"
    And selector ".create-udf-add-argument" is clicked
    And selector ".create-udf-argument-name" is filled with "value"
    And selector ".create-udf-argument-type [data-type='I64']" is clicked
    And selector ".create-udf-return-type [data-type='I64']" is clicked
    And selector ".create-udf-code" is filled with text
      """
      fn visual_reject(value: I64Column) -> I64Column { value }
      test rejects { reject }
      """
    When selector ".create-submit" is clicked
    Then selector ".create-error" contains "Roto test block failed"
    And selector ".create-udf-code" has value containing "test rejects"
    When selector ".create-udf-code" is filled with text
      """
      fn visual_reject(value: I64Column) -> I64Column { value }
      test accepts { accept }
      """
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"

  @visual_lookup_missing_version
  Scenario: A resource without a completed version cannot be selected for a hash map
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE empty_bundle;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='hash-map']" is clicked
    And selector ".create-name" is filled with "missing_version"
    And selector ".create-hash-resource .create-choice-search" is filled with "empty_bundle"
    Then selector ".create-hash-resource [data-value='empty_bundle']" exists
    When selector ".create-hash-resource [data-value='empty_bundle']" is clicked
    Then selector ".create-hash-version .create-choice-state" contains "No choices"
    And selector ".create-hash-version [data-value='LATEST']" does not exist
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Choose LATEST or a completed resource version"

  @visual_udf_exact_types
  Scenario: The UDF declaration must exactly match its Roto entry signature
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='udf']" is clicked
    And selector ".create-name" is filled with "exact_result"
    And selector ".create-udf-add-argument" is clicked
    And selector ".create-udf-argument-name" is filled with "value"
    And selector ".create-udf-argument-type [data-type='I64']" is clicked
    And selector ".create-udf-return-type [data-type='STRING']" is clicked
    And selector ".create-udf-code" is filled with text
      """
      fn exact_result(value: I64Column) -> I64Column { value }
      """
    And selector ".create-submit" is clicked
    Then selector ".create-error" contains "mismatched types"
    And selector ".create-udf-code" has value containing "I64Column"
    When selector ".create-udf-return-type [data-type='I64']" is clicked
    And selector ".create-udf-argument-type [data-type='STRING']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-error" contains "mismatched types"
    And selector ".create-udf-argument-name" has value "value"
    When selector ".create-udf-argument-type [data-type='I64']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"

  @visual_lookup_domain_scope
  Scenario: Changing a hash map draft's domain invalidates selected resource and codec references
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_other;
      CREATE RESOURCE scoped_bundle;
      CREATE SCHEMA scoped_entry (id STRING);
      CREATE WIRE JSON SCHEMA scoped_wire MODE STRICT (id string);
      CREATE CODEC scoped_codec FROM WIRE JSON SCHEMA scoped_wire TO SCHEMA scoped_entry;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='hash-map']" is clicked
    And selector ".create-name" is filled with "scoped_lookup"
    And selector ".create-hash-resource .create-choice-search" is filled with "scoped_bundle"
    Then selector ".create-hash-resource [data-value='scoped_bundle']" exists
    When selector ".create-hash-resource [data-value='scoped_bundle']" is clicked
    And selector ".create-hash-codec .create-choice-search" is filled with "scoped_codec"
    Then selector ".create-hash-codec [data-value='scoped_codec']" exists
    When selector ".create-hash-codec [data-value='scoped_codec']" is clicked
    Then selector ".create-hash-key [data-value='id']" exists
    When selector ".create-hash-key [data-value='id']" is clicked
    And selector ".create-hash-path" is filled with "lookup.jsonl"
    And selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='hash-map']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{domain}}_other"
    And selector ".create-selected-key" contains "id"
    And selector ".create-reference-invalid" exists
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "selected resource belongs to a changed context"

  @visual_lookup_transaction
  Scenario: A hash map can select a codec and its fields from the attached transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "visual_tx_lookup" containing
      """
      {
        "lookup.jsonl": "{\"id\":\"one\",\"value\":42}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE tx_bundle;
      UPLOAD RESOURCE tx_bundle VERSION '{{visual_tx_lookup}}';
      CREATE SCHEMA tx_entry (id STRING, value I64);
      CREATE WIRE JSON SCHEMA tx_wire MODE STRICT (id string, value integer);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".prompt-row input" is filled with "CREATE CODEC tx_codec FROM WIRE JSON SCHEMA tx_wire TO SCHEMA tx_entry;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='hash-map']" is clicked
    And selector ".create-name" is filled with "tx_lookup"
    And selector ".create-hash-resource .create-choice-search" is filled with "tx_bundle"
    Then selector ".create-hash-resource [data-value='tx_bundle']" exists
    When selector ".create-hash-resource [data-value='tx_bundle']" is clicked
    Then selector ".create-hash-version [data-value='LATEST']" exists
    When selector ".create-hash-version [data-value='LATEST']" is clicked
    And selector ".create-hash-codec .create-choice-search" is filled with "tx_codec"
    Then selector ".create-hash-codec [data-value='tx_codec']" exists
    When selector ".create-hash-codec [data-value='tx_codec']" is clicked
    Then selector ".create-hash-key [data-value='id']" exists
    When selector ".create-hash-key [data-value='id']" is clicked
    And selector ".create-hash-path" is filled with "lookup.jsonl"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "COMPLETE"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector ".prompt-row input" is filled with "LOOKUP tx_lookup KEY 'one';"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains
      """
      "value":42
      """
