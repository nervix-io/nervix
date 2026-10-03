Feature: Web console visual schema and branch creation

  @visual_schema_create
  Scenario Outline: A schema and branch created from ordered controls are usable downstream
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='schema']" is clicked
    And selector ".create-name" is filled with "visual_branch_key"
    And selector ".create-add-field" is clicked
    And selector ".create-field-name" is filled with "tenant_id"
    And selector ".create-field-type [data-type='U32']" is clicked
    Then selector ".create-preview" contains "CREATE SCHEMA visual_branch_key"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='branch']" is clicked
    And selector ".create-name" is filled with "visual_by_tenant"
    And selector ".create-schema-ref .create-choice-search" is filled with "visual_branch_key"
    Then selector ".create-schema-ref [data-value='visual_branch_key']" exists
    When selector ".create-schema-ref [data-value='visual_branch_key']" is clicked
    And selector ".create-ttl" is filled with "5m"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE BRANCH visual_by_tenant;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE BRANCH visual_by_tenant SCHEMA visual_branch_key TTL 5m;" exactly 2 times
    When selector ".prompt-row input" is filled with "CREATE RELAY visual_relay SCHEMA visual_branch_key BRANCHED BY visual_by_tenant;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE RELAY visual_relay SCHEMA visual_branch_key BRANCHED BY visual_by_tenant;"
    When selector ".prompt-row input" is filled with "SHOW CREATE RELAY visual_relay;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE RELAY visual_relay SCHEMA visual_branch_key BRANCHED BY visual_by_tenant CAPACITY 1;"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_wire_schema_create
  Scenario Outline: Each declared wire format has a complete visual create form
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='<kind>']" is clicked
    And selector ".create-name" is filled with "visual_wire"
    And selector ".create-mode-options [data-value='LOOSE']" is clicked
    And selector ".create-add-field" is clicked
    And selector ".create-field-name" is filled with "payload"
    And selector ".create-field-type [data-type='<type>']" is clicked
    And selector ".create-field-optional" is clicked
    Then selector ".create-preview" contains "CREATE WIRE <format> SCHEMA visual_wire MODE LOOSE"
    And selector ".create-preview" contains "payload <canonical_type> OPTIONAL"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE WIRE <format> SCHEMA visual_wire;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE WIRE <format> SCHEMA visual_wire MODE LOOSE" exactly 2 times

    Examples:
      | kind             | format | type   | canonical_type |
      | wire-json-schema | JSON   | string | STRING         |
      | wire-cbor-schema | CBOR   | string | STRING         |
      | wire-avro-schema | AVRO   | string | STRING         |

  Scenario: Wire formats retain separate identities when their names coincide
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE WIRE JSON SCHEMA same_name MODE STRICT (payload string);
      CREATE WIRE CBOR SCHEMA same_name MODE LOOSE (payload string);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='wire-avro-schema']" is clicked
    And selector ".create-name" is filled with "same_name"
    And selector ".create-mode-options [data-value='STRICT']" is clicked
    And selector ".create-add-field" is clicked
    And selector ".create-field-name" is filled with "payload"
    And selector ".create-field-type [data-type='string']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE WIRE AVRO SCHEMA same_name;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE WIRE AVRO SCHEMA same_name MODE STRICT" exactly 2 times
    When selector ".prompt-row input" is filled with "SHOW CREATE WIRE JSON SCHEMA same_name;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE WIRE JSON SCHEMA same_name MODE STRICT"
    When selector ".prompt-row input" is filled with "SHOW CREATE WIRE CBOR SCHEMA same_name;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE WIRE CBOR SCHEMA same_name MODE LOOSE"

  Scenario: A branch form sees a schema staged earlier in its attached transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='schema']" is clicked
    And selector ".create-name" is filled with "staged_key"
    And selector ".create-add-field" is clicked
    And selector ".create-field-name" is filled with "tenant"
    And selector ".create-field-type [data-type='U32']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='branch']" is clicked
    And selector ".create-name" is filled with "staged_branch"
    And selector ".create-schema-ref .create-choice-search" is filled with "staged_key"
    Then selector ".create-schema-ref [data-value='staged_key']" exists
    When selector ".create-schema-ref [data-value='staged_key']" is clicked
    And selector ".create-ttl" is filled with "5m"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    And selector ".create-status" contains "position 2"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "COMPLETE"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector ".prompt-row input" is filled with "SHOW CREATE BRANCH staged_branch;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE BRANCH staged_branch SCHEMA staged_key TTL 5m;" exactly 2 times

  Scenario: Duplicate fields remain editable and invalid branch keys report the registry reason
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA byte_key (payload BYTES);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='schema']" is clicked
    And selector ".create-name" is filled with "duplicate_fields"
    And selector ".create-add-field" is clicked
    And selector ".create-field-entry:first-child .create-field-name" is filled with "same"
    And selector ".create-field-entry:first-child .create-field-type [data-type='U32']" is clicked
    And selector ".create-add-field" is clicked
    And selector ".create-field-entry:nth-child(2) .create-field-name" is filled with "same"
    And selector ".create-field-entry:nth-child(2) .create-field-type [data-type='U64']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "same"
    And selector ".create-field-entry:nth-child(2) .create-field-name" has value "same"
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='branch']" is clicked
    And selector ".create-name" is filled with "invalid_branch"
    And selector ".create-schema-ref .create-choice-search" is filled with "byte_key"
    Then selector ".create-schema-ref [data-value='byte_key']" exists
    When selector ".create-schema-ref [data-value='byte_key']" is clicked
    And selector ".create-ttl" is filled with "5m"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "BYTES"
    And selector ".create-error" contains "payload"

  Scenario: Nested sensitive fields and bounded branches preserve their exact visual choices
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA tenant_key (tenant U32);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='schema']" is clicked
    And selector ".create-name" is filled with "nested_payload"
    And selector ".create-add-field" is clicked
    And selector ".create-field-name" is filled with "samples"
    And selector ".create-field-type [data-type='F32']" is clicked
    And selector ".create-add-array" is clicked
    And selector ".create-array-length" is filled with "6"
    And selector ".create-add-vector" is clicked
    And selector ".create-field-optional" is clicked
    And selector ".create-field-sensitive" is clicked
    Then selector ".create-preview" contains "samples VEC<ARRAY<F32, 6>> OPTIONAL SENSITIVE"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE SCHEMA nested_payload;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "samples VEC<ARRAY<F32, 6>> OPTIONAL SENSITIVE" exactly 2 times
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='branch']" is clicked
    And selector ".create-name" is filled with "bounded_tenants"
    And selector ".create-schema-ref .create-choice-search" is filled with "tenant_key"
    Then selector ".create-schema-ref [data-value='tenant_key']" exists
    When selector ".create-schema-ref [data-value='tenant_key']" is clicked
    And selector ".create-ttl" is filled with "5m"
    And selector ".create-limit-instances" is clicked
    And selector ".create-max-instances" is filled with "3"
    Then selector ".create-preview" contains "MAX INSTANCES 3 EVICT LRU"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE BRANCH bounded_tenants;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE BRANCH bounded_tenants SCHEMA tenant_key TTL 5m MAX INSTANCES 3 EVICT LRU;" exactly 2 times

  Scenario: Changing a branch draft's domain retains and invalidates its selected schema
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE DOMAIN {{domain}}_other;
      CREATE SCHEMA tenant_key (tenant U32);
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='branch']" is clicked
    And selector ".create-name" is filled with "retained_branch"
    And selector ".create-schema-ref .create-choice-search" is filled with "tenant_key"
    Then selector ".create-schema-ref [data-value='tenant_key']" exists
    When selector ".create-schema-ref [data-value='tenant_key']" is clicked
    And selector ".create-ttl" is filled with "5m"
    And selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='branch']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    And selector ".create-scope-change" exists
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{domain}}_other"
    And selector ".create-selected-schema" contains "tenant_key"
    And selector ".create-reference-invalid" exists
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "selected schema belongs to a changed context"

  Scenario: A selected branch schema remains usable after leader reconnection
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA reconnect_key (tenant U32);
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='branch']" is clicked
    And selector ".create-name" is filled with "reconnected_branch"
    And selector ".create-schema-ref .create-choice-search" is filled with "reconnect_key"
    Then selector ".create-schema-ref [data-value='reconnect_key']" exists
    When selector ".create-schema-ref [data-value='reconnect_key']" is clicked
    And selector ".create-ttl" is filled with "5m"
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".create-selected-schema" contains "reconnect_key"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE BRANCH reconnected_branch;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "CREATE BRANCH reconnected_branch SCHEMA reconnect_key TTL 5m;" exactly 2 times
