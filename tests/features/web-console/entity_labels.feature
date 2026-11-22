@web_console_labels
Feature: Web console entity labels
  Create forms add labels to the entities they create, and the label editor shows the typed
  labels of an existing entity and saves every change to them as one SET LABELS statement on the
  durable command path.

  Scenario Outline: Create forms add the labels the operator writes to the entity they create
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='schema']" is clicked
    And selector ".create-name" is filled with "labelled_event"
    And selector ".create-add-field" is clicked
    And selector ".create-field-name" is filled with "value"
    And selector ".create-field-type [data-type='U32']" is clicked
    And selector ".create-label-add" is clicked
    And selector ".create-label-row[data-row='0'] .create-label-key" is filled with "team/path"
    And selector ".create-label-row[data-row='0'] .create-label-value" is filled with "Platform / East Δ"
    And selector ".create-label-add" is clicked
    And selector ".create-label-row[data-row='1'] .create-label-key" is filled with "empty"
    Then selector ".create-preview" contains "LABELS {"
    And selector ".create-preview" contains "'empty' = ''"
    And selector ".create-preview" contains "'team/path' = 'Platform / East Δ'"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"empty":"","team/path":"Platform / East Δ"}
      """
    When selector ".create-close" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='resource']" is clicked
    Then selector ".create-label-row" does not exist
    When selector ".create-name" is filled with "labelled_bundle"
    And selector ".create-label-add" is clicked
    And selector ".create-label-row[data-row='0'] .create-label-key" is filled with "Owner"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Label 1"
    And selector ".create-validation" contains "invalid label key"
    And selector ".terminal" does not contain "CREATE RESOURCE labelled_bundle"
    When selector ".create-label-row[data-row='0'] .create-label-key" is filled with "owner"
    And selector ".create-label-row[data-row='0'] .create-label-value" is filled with "Operations"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON RESOURCE labelled_bundle FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner":"Operations"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The label editor saves sets, changes and removals of one entity as one statement
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA readings ( message STRING ) LABELS {
        'owner' = 'Operations',
        'team' = 'orders',
        'kept' = 'as is'
      };
      CREATE RELAY labelled_input SCHEMA readings UNBRANCHED;
      CREATE RELAY labelled_output SCHEMA readings UNBRANCHED;
      CREATE JUNCTION labelled_junction FROM labelled_input UNBRANCHED
        TO labelled_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        LABELS { 'stage' = 'one' };
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector "#nav-labels-schema-readings" is clicked
    Then selector ".create-dialog" contains "Edit labels"
    And selector ".labels-owner" contains "SCHEMA readings in domain {{domain}}"
    And selector ".labels-view" contains "Committed"
    And selector ".labels-entry[data-key='kept'] .labels-entry-value" has value "as is"
    When selector ".labels-entry[data-key='owner'] .labels-entry-value" is filled with "Platform"
    And selector ".labels-entry[data-key='team'] .labels-entry-remove" is clicked
    And selector ".labels-new-add" is clicked
    And selector ".labels-new-row[data-row='0'] .labels-new-key" is filled with "region"
    And selector ".labels-new-row[data-row='0'] .labels-new-value" is filled with "east"
    Then selector ".create-preview" contains "SET LABELS ON SCHEMA readings { 'owner' = 'Platform', 'region' = 'east', 'team' DROP };"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    And selector ".terminal" contains "SET LABELS ON SCHEMA readings { 'owner' = 'Platform', 'region' = 'east', 'team' DROP };"
    And selector ".labels-entry[data-key='region'] .labels-entry-value" has value "east"
    And selector ".labels-entry[data-key='team']" does not exist
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA readings FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"kept":"as is","owner":"Platform","region":"east"}
      """
    When selector ".create-close" is clicked
    And selector ".node-hit[data-label='labelled_junction']" is clicked
    And selector ".graph-action-menu [data-action='edit-labels']" is clicked
    Then selector ".labels-owner" contains "JUNCTION labelled_junction in domain {{domain}}"
    And selector ".labels-entry[data-key='stage'] .labels-entry-value" has value "one"
    When selector ".labels-entry[data-key='stage'] .labels-entry-remove" is clicked
    Then selector ".create-preview" contains "SET LABELS ON JUNCTION labelled_junction { 'stage' DROP };"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    And selector ".labels-empty" contains "No labels"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A label edit in the attached transaction is staged, reverts with it and commits with it
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA readings ( value U32 ) LABELS { 'owner' = 'Operations' };
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector "#nav-labels-schema-readings" is clicked
    And selector ".labels-entry[data-key='owner'] .labels-entry-value" is filled with "Staged"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    And selector ".labels-view" contains "Staged in transaction"
    And selector ".labels-entry[data-key='owner'] .labels-entry-value" has value "Staged"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA readings FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Operations"
      /view/state = "committed"
      """
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "REVERT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector "#nav-labels-schema-readings" is clicked
    Then selector ".labels-view" contains "Committed"
    And selector ".labels-entry[data-key='owner'] .labels-entry-value" has value "Operations"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector "#nav-labels-schema-readings" is clicked
    And selector ".labels-entry[data-key='owner'] .labels-entry-value" is filled with "Committed later"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is pressed with "Enter"
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA readings FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Committed later"
      /view/state = "committed"
      """

  Scenario: An open label edit keeps its entity when the console selects another domain with the same names
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the active domain is saved as placeholder "first_domain"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA readings ( value U32 ) LABELS { 'owner' = 'First' };
      """
    And the active domain is "second_{{test_id}}"
    And the active domain is saved as placeholder "second_domain"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA readings ( value U32 ) LABELS { 'owner' = 'Second' };
      """
    Given the active domain is "{{first_domain}}"
    When the web console is opened on the leader node
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{first_domain}}']" is clicked
    Then selector ".domain-select" contains "{{first_domain}}"
    When selector "#nav-labels-schema-readings" is clicked
    Then selector ".labels-entry[data-key='owner'] .labels-entry-value" has value "First"
    When selector ".labels-entry[data-key='owner'] .labels-entry-value" is filled with "First updated"
    And selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{second_domain}}']" is clicked
    Then selector ".domain-select" contains "{{second_domain}}"
    When selector ".labels-menu-button" is clicked
    Then selector ".labels-owner" contains "SCHEMA readings in domain {{first_domain}}"
    And selector ".create-scope" contains "{{first_domain}}"
    And selector ".labels-entry[data-key='owner'] .labels-entry-value" has value "First updated"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA readings FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "First updated"
      """
    Given the active domain is "{{second_domain}}"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA readings FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Second"
      """

  Scenario: Invalid, repeated and over-limit label edits are refused inline before they are sent
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      ALTER CLUSTER SET ENTITY_LABELS_MAX_COUNT = 2;
      CREATE SCHEMA readings ( value U32 ) LABELS { 'one' = '1', 'two' = '2' };
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector "#nav-labels-schema-readings" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Nothing to save"
    When selector ".labels-new-add" is clicked
    And selector ".labels-new-row[data-row='0'] .labels-new-key" is filled with "three"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "label set exceeds entity-labels-max-count: 3 > 2"
    And selector ".terminal" does not contain "SET LABELS ON SCHEMA readings"
    When selector ".labels-entry[data-key='two'] .labels-entry-remove" is clicked
    And selector ".labels-new-add" is clicked
    And selector ".labels-new-row[data-row='1'] .labels-new-key" is filled with "three"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "duplicate label key"
    When selector ".labels-new-row[data-row='1'] .labels-new-key" is filled with "one"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "'one' is already labelled"
    When selector ".labels-new-row[data-row='1'] .labels-new-key" is filled with "Three"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "New label 2"
    When selector ".labels-new-row[data-row='1'] .labels-new-remove" is clicked
    Then selector ".create-preview" contains "SET LABELS ON SCHEMA readings { 'three' = '', 'two' DROP };"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA readings FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"one":"1","three":""}
      """

  Scenario: Reloading shows another session's change to an edited label instead of overwriting it
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA readings ( value U32 ) LABELS { 'owner' = 'Operations', 'team' = 'orders' };
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector "#nav-labels-schema-readings" is clicked
    And selector ".labels-entry[data-key='owner'] .labels-entry-value" is filled with "Platform"
    And these Plexus commands are executed on the leader node
      """
      SET LABELS ON SCHEMA readings { 'owner' = 'Security', 'team' = 'returns', 'region' = 'west' };
      """
    And selector ".labels-reload" is clicked
    Then selector ".labels-entry[data-key='owner'] .labels-entry-conflict" contains "Another session changed this label to 'Security'"
    And selector ".labels-entry[data-key='team'] .labels-entry-value" has value "returns"
    And selector ".labels-entry[data-key='region'] .labels-entry-value" has value "west"
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Another session changed labels you edited"
    And selector ".terminal" does not contain "SET LABELS ON SCHEMA readings"
    When selector ".labels-entry[data-key='owner'] .labels-entry-keep" is clicked
    Then selector ".create-preview" contains "SET LABELS ON SCHEMA readings { 'owner' = 'Platform' };"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA readings FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner":"Platform","region":"west","team":"returns"}
      """

  Scenario: One label save survives a leader change without being applied twice
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA readings ( value U32 ) LABELS { 'owner' = 'Operations' };
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When leadership is transferred from node "{{new_leader}}" to node "{{old_leader}}"
    And the web console is opened on node "{{old_leader}}"
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector "#nav-labels-schema-readings" is clicked
    And selector ".labels-entry[data-key='owner'] .labels-entry-value" is filled with "Recovered"
    And selector ".labels-new-add" is clicked
    And selector ".labels-new-row[data-row='0'] .labels-new-key" is filled with "once"
    Given command response delivery on node "{{old_leader}}" pauses after execution
    When selector ".create-submit" is clicked
    Then the command response delivery pause on node "{{old_leader}}" is reached
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    And the command response delivery pause on node "{{old_leader}}" is released
    Then selector ".terminal" contains "connected to leader '{{new_leader}}'"
    And selector ".create-status" contains "Completed"
    And selector ".terminal" contains "SET LABELS ON SCHEMA readings { 'once' = '', 'owner' = 'Recovered' };" exactly 1 times
    And selector ".labels-entry[data-key='once']" exists
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA readings FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"once":"","owner":"Recovered"}
      """

  Scenario: Domain and user labels are edited from the top bar and never inside a transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'tier' = 'gold' };
      CREATE USER labels_auditor WITH PASSWORD 'auditor-password' LABELS { 'purpose' = 'Audit' };
      """
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".labels-menu-button" is clicked
    Then selector ".labels-owner" contains "DOMAIN {{domain}}"
    And selector ".create-scope" contains "Cluster"
    And selector ".labels-entry[data-key='tier'] .labels-entry-value" has value "gold"
    When selector ".labels-entry[data-key='tier'] .labels-entry-value" is filled with "silver"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".labels-target-kind" is set to option "user"
    And selector ".labels-target-name" is filled with "labels_auditor"
    And selector ".labels-target-show" is clicked
    Then selector ".labels-owner" contains "USER labels_auditor"
    And selector ".labels-entry[data-key='purpose'] .labels-entry-value" has value "Audit"
    When selector ".labels-entry[data-key='purpose'] .labels-entry-value" is filled with "Review"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".labels-menu-button" is clicked
    Then selector ".labels-owner" contains "DOMAIN {{domain}}"
    When selector ".labels-entry[data-key='tier'] .labels-entry-remove" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "cannot be queued in a transaction"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON DOMAIN {{domain}} FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"tier":"silver"}
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON USER labels_auditor FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"purpose":"Review"}
      """
