Feature: Ephemeral activation expiry and the licensing stop gate
  At its fixed expiry an ephemeral cluster stops processing, keeps its data and its administrative
  access, and never falls back to the free policy. Only a new activation lifts the gate, and the
  domains licensing stopped stay stopped until an operator starts them.

  Scenario Outline: Expiry stops every running domain with the licensing cause on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "expiry_bundle" containing
      """
      {
        "payload.txt": "kept-through-expiry\n"
      }
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    And node "<observer>" eventually reports status containing "{{domain}} status=Running"
    When a licensing notice session is opened on the leader node
    And the cluster licensing clock passes the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      mode: activated
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    And a licensing Error notice saying "Processing stopped" is observed
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    When these Plexus commands fail with "licensing stopped processing"
      """
      START;
      """
    Given client "transaction" is connected to the leader node
    When client "transaction" selects domain "{{domain}}"
    And client "transaction" fails to execute these Plexus commands
      """
      BEGIN;
      START;
      COMMIT;
      """
    Then the last command error contains
      """
      licensing stopped processing
      """
    When these Plexus commands fail with "(stopped)"
      """
      CREATE UNPACED DOMAIN {{domain}}_after_expiry;
      """
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And client "operator" executes these Plexus commands
      """
      CREATE RESOURCE expiry_bundle;
      UPLOAD RESOURCE expiry_bundle VERSION '{{expiry_bundle}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    And the current leader node is saved as placeholder "licensing_leader"
    When the CLI backs up "domain {{domain}}" from node "{{licensing_leader}}" into "expired.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the cluster is restarted
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And the independent activation binding matches placeholder "ephemeral"
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE RESOURCE expiry_bundle;
      """
    Then the last command output contains
      """
      expiry_bundle
      """

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  Scenario Outline: A new activation lifts the gate and the stopped domains wait for START on <cluster_size> nodes
    Given an HTTPS licensing test service allows one ephemeral activation
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "first_activation"
    And the ephemeral expiry of license placeholder "first_activation" is saved as placeholder "expired_at"
    And node "<observer>" eventually reports status containing "{{domain}} status=Running"
    When a licensing notice session is opened on the leader node
    And both licensing clocks pass the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then node "<observer>" eventually describes this license without a selected domain
      """
      status: expired
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And the independent activation binding matches placeholder "first_activation"
    And the licensing service marks placeholder "first_activation" expired and frees its slot
    When these Plexus commands fail with "(stopped)"
      """
      CREATE UNPACED DOMAIN {{domain}}_free_policy;
      """
    And these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: licensed fresh
      processing: running
      """
    And a licensing Info notice saying "Processing may resume" is observed
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then node "<observer>" eventually reports status containing "{{domain}} status=Running"

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  Scenario: A leader transfer keeps the committed expiry, the closed gate and the stopped domains
    Given an HTTPS licensing test service is available
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    When the cluster licensing clock passes the "ephemeral" deadline
    Then node "node-1" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    And node "node-1" eventually reports status containing "{{domain}} status=Stopped"
    When leadership is transferred to node "node-2"
    And leadership is transferred to node "node-3"
    Then node "node-1" eventually describes this license without a selected domain
      """
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And the independent activation binding matches placeholder "ephemeral"
    And node "node-2" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands fail with "licensing stopped processing"
      """
      START;
      """

  Scenario Outline: A backup whose cut the expiry interrupts completes and leaves its domain stopped by licensing on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    And node "<observer>" eventually reports status containing "{{domain}} status=Running"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    Then the current leader node is saved as placeholder "leader"
    When the CLI begins backing up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "interrupted.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    When the cluster licensing clock passes the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    When the backup cut pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    When these Plexus commands fail with "licensing stopped processing"
      """
      START;
      """

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |
