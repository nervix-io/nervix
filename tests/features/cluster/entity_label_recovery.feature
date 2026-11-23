@entity_label_recovery
Feature: Entity labels through faults and recovery
  A label edit commits with the entity it describes and recovers with the cluster. An edit whose
  answer was lost, whose leader moved or whose node fell behind ends with exactly one effect, a
  replay under its execution reference returns that outcome without applying it again, and every
  inspection reads one whole committed label map.

  Scenario Outline: A label edit whose answer was lost on <target> is recovered under its reference and keeps a later edit
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'owner' = 'Operations' };
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      CREATE RESOURCE labelled_resource LABELS { 'owner' = 'Operations' };
      CREATE USER labelled_user WITH PASSWORD 'example-password' LABELS { 'owner' = 'Operations' };
      """
    Then the current leader node is saved as placeholder "leader"
    Given command response delivery on node "{{leader}}" pauses after execution
    When the active session begins this Plexus command request with execution reference "lost-label-{{test_id}}" in the background
      """
      SET LABELS ON <target> { 'owner' = 'Platform', 'edit' = 'lost' };
      """
    Then the command response delivery pause on node "{{leader}}" is reached
    When the background command request connection is dropped
    And the command response delivery pause on node "{{leader}}" is released
    And this Plexus command request is executed on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Finance' };
      """
    And this Plexus command request with execution reference "lost-label-{{test_id}}" is executed on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Platform', 'edit' = 'lost' };
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON <target> FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"edit": "lost", "owner": "Finance"}
      /view/state = "committed"
      """

    Examples:
      | cluster_size | target                     |
      | 1            | SCHEMA labelled_event      |
      | 3            | SCHEMA labelled_event      |
      | 1            | RESOURCE labelled_resource |
      | 3            | RESOURCE labelled_resource |
      | 1            | DOMAIN {{domain}}          |
      | 3            | DOMAIN {{domain}}          |
      | 1            | USER labelled_user         |
      | 3            | USER labelled_user         |

  Scenario Outline: A label edit on <target> whose leader moved before admitting it applies once under its reference
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'owner' = 'Operations' };
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      CREATE RESOURCE labelled_resource LABELS { 'owner' = 'Operations' };
      CREATE USER labelled_user WITH PASSWORD 'example-password' LABELS { 'owner' = 'Operations' };
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given leadership moves from node "{{old_leader}}" to node "{{new_leader}}" before its next admission proposal
    When this Plexus command request with execution reference "moved-label-{{test_id}}" begins executing in the background on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Platform', 'edit' = 'moved' };
      """
    Then the leadership move before the admission proposal completed
    And the background command request reports an unknown outcome because leadership moved
    And node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When this Plexus command request with execution reference "moved-label-{{test_id}}" is executed on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Platform', 'edit' = 'moved' };
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Finance' };
      """
    And this Plexus command request with execution reference "moved-label-{{test_id}}" is executed on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Platform', 'edit' = 'moved' };
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON <target> FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"edit": "moved", "owner": "Finance"}
      """

    Examples:
      | target                     |
      | SCHEMA labelled_event      |
      | RESOURCE labelled_resource |
      | DOMAIN {{domain}}          |
      | USER labelled_user         |

  Scenario Outline: A label edit on <target> its leader admitted before leadership moved completes once and keeps a later edit
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'owner' = 'Operations' };
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      CREATE RESOURCE labelled_resource LABELS { 'owner' = 'Operations' };
      CREATE USER labelled_user WITH PASSWORD 'example-password' LABELS { 'owner' = 'Operations' };
      """
    Then the current leader node is saved as placeholder "admitting_leader"
    And a node other than placeholder "admitting_leader" is saved as placeholder "successor"
    Given leadership moves from node "{{admitting_leader}}" to node "{{successor}}" under its next durably admitted command
    When this Plexus command request with execution reference "admitted-label-{{test_id}}" is executed on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Platform', 'edit' = 'admitted' };
      """
    Then the leadership move under the durably admitted command completed
    And the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON <target> FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"edit": "admitted", "owner": "Platform"}
      """
    When this Plexus command request is executed on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Finance' };
      """
    And this Plexus command request with execution reference "admitted-label-{{test_id}}" is executed on the leader node
      """
      SET LABELS ON <target> { 'owner' = 'Platform', 'edit' = 'admitted' };
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON <target> FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"edit": "admitted", "owner": "Finance"}
      """

    Examples:
      | target                     |
      | SCHEMA labelled_event      |
      | RESOURCE labelled_resource |
      | DOMAIN {{domain}}          |
      | USER labelled_user         |

  Scenario: A label transaction whose COMMIT its leader admitted before leadership moved commits every edit once
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations', 'empty' = '' };
      CREATE RELAY labelled_events SCHEMA labelled_event UNBRANCHED LABELS { 'purpose' = 'audit' };
      CREATE RESOURCE labelled_resource LABELS { 'owner' = 'Operations' };
      """
    When these Plexus commands are executed on the active session
      """
      BEGIN;
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform', 'stage' = 'committed' };
      SET LABELS ON SCHEMA labelled_event { 'empty' DROP };
      SET LABELS ON RELAY labelled_events { 'purpose' = 'billing' };
      SET LABELS ON RESOURCE labelled_resource { 'owner' = 'Platform' };
      """
    And this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only open transaction id is saved as placeholder "label_transaction"
    And the current leader node is saved as placeholder "admitting_leader"
    And a node other than placeholder "admitting_leader" is saved as placeholder "successor"
    Given leadership moves from node "{{admitting_leader}}" to node "{{successor}}" under its next durably admitted command
    When these Plexus commands are executed on the active session
      """
      COMMIT;
      """
    Then the leadership move under the durably admitted command completed
    And transaction "{{label_transaction}}" eventually has state "COMMITTED"
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform", "stage": "committed"}
      /view/state = "committed"
      """
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON RELAY labelled_events FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"purpose": "billing"}
      """
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON RESOURCE labelled_resource FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """
    Given client "former_leader" is connected to node "{{admitting_leader}}"
    When client "former_leader" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform", "stage": "committed"}
      """

  Scenario Outline: An inspection beside a held label edit reads the whole previous map until the whole new map is visible
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'a' = '1', 'b' = '1', 'c' = '1', 'd' = '1' };
      """
    Then the current leader node is saved as placeholder "leader"
    Given command execution on node "{{leader}}" pauses after durable admission
    When this Plexus command request with execution reference "held-label-{{test_id}}" begins executing in the background on the leader node
      """
      SET LABELS ON SCHEMA labelled_event { 'a' = '2', 'b' = '2', 'c' = '2' };
      """
    Then the durable command admission pause on node "{{leader}}" is reached
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"a": "1", "b": "1", "c": "1", "d": "1"}
      /view/state = "committed"
      """
    When the durable command admission pause on node "{{leader}}" is released
    Then the background command request succeeds
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"a": "2", "b": "2", "c": "2", "d": "1"}
      /view/state = "committed"
      """
    Given command finalization on node "{{leader}}" pauses before its outcome is recorded
    When this Plexus command request with execution reference "finalizing-label-{{test_id}}" begins executing in the background on the leader node
      """
      SET LABELS ON SCHEMA labelled_event { 'a' DROP, 'b' DROP, 'c' DROP };
      """
    Then the command finalization pause on node "{{leader}}" is reached
    When this Plexus command request is executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"d": "1"}
      /view/state = "committed"
      """
    When the command finalization pause on node "{{leader}}" is released
    Then the background command request succeeds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A follower reads the previous map while a label edit waits after admission and the new map once it completes
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'a' = '1', 'b' = '1' };
      CREATE USER labelled_user WITH PASSWORD 'example-password' LABELS { 'a' = '1', 'b' = '1' };
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    Given client "reader" is connected to node "{{follower}}"
    And command execution on node "{{leader}}" pauses after durable admission
    When this Plexus command request with execution reference "held-user-label-{{test_id}}" begins executing in the background on the leader node
      """
      SET LABELS ON USER labelled_user { 'a' = '2', 'b' = '2' };
      """
    Then the durable command admission pause on node "{{leader}}" is reached
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON USER labelled_user FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"a": "1", "b": "1"}
      /view/state = "committed"
      """
    When the durable command admission pause on node "{{leader}}" is released
    Then the background command request succeeds
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON USER labelled_user FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"a": "2", "b": "2"}
      /view/state = "committed"
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON DOMAIN {{domain}} FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"a": "1", "b": "1"}
      """

  Scenario: A follower stopped through label edits, drops and recreations reads the exact labels once it catches up
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'owner' = 'Operations' };
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations', 'empty' = '' };
      CREATE RELAY labelled_events SCHEMA labelled_event UNBRANCHED LABELS { 'generation' = 'first', 'owner' = 'Operations' };
      CREATE RESOURCE labelled_resource LABELS { 'owner' = 'Operations' };
      CREATE USER labelled_user WITH PASSWORD 'example-password' LABELS { 'owner' = 'Operations' };
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "stale"
    Given client "editor" is connected to node "{{leader}}"
    When node "{{stale}}" is stopped
    And client "editor" executes these Plexus commands
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform', 'stage' = 'edited' };
      SET LABELS ON SCHEMA labelled_event { 'empty' DROP };
      DROP RELAY labelled_events;
      CREATE RELAY labelled_events SCHEMA labelled_event UNBRANCHED LABELS { 'generation' = 'second' };
      SET LABELS ON RESOURCE labelled_resource { 'owner' = 'Platform' };
      SET LABELS ON DOMAIN {{domain}} { 'owner' = 'Platform' };
      SET LABELS ON USER labelled_user { 'owner' = 'Platform', 'audited' = '' };
      """
    And node "{{stale}}" is started
    Given client "reader" is connected to node "{{stale}}"
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform", "stage": "edited"}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON RELAY labelled_events FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"generation": "second"}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE labelled_resource FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON DOMAIN {{domain}} FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON USER labelled_user FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"audited": "", "owner": "Platform"}
      """
    When leadership is transferred to node "{{stale}}"
    Then node "{{stale}}" eventually reports leader "{{stale}}"
    When client "reader" executes these Plexus commands
      """
      SET LABELS ON RELAY labelled_events { 'owner' = 'Successor' };
      """
    And client "reader" executes these Plexus commands
      """
      SHOW LABELS ON RELAY labelled_events FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"generation": "second", "owner": "Successor"}
      """

  Scenario: A follower that missed compacted label edits installs a snapshot with the exact labels
    Given raft snapshots after 8 entries retaining 2 covered entries
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'owner' = 'Operations' };
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations', 'empty' = '' };
      CREATE RELAY labelled_events SCHEMA labelled_event UNBRANCHED LABELS { 'generation' = 'first' };
      CREATE RESOURCE labelled_resource LABELS { 'owner' = 'Operations' };
      CREATE USER labelled_user WITH PASSWORD 'example-password' LABELS { 'owner' = 'Operations' };
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "stale"
    Given client "editor" is connected to node "{{leader}}"
    And node "{{stale}}" raft log head is saved before stop
    When node "{{stale}}" is stopped
    And client "editor" executes these Plexus commands
      """
      SET LABELS ON SCHEMA labelled_event { 'step' = '1' };
      SET LABELS ON SCHEMA labelled_event { 'step' = '2' };
      SET LABELS ON SCHEMA labelled_event { 'step' = '3' };
      SET LABELS ON SCHEMA labelled_event { 'empty' DROP };
      DROP RELAY labelled_events;
      CREATE RELAY labelled_events SCHEMA labelled_event UNBRANCHED LABELS { 'generation' = 'second' };
      SET LABELS ON RESOURCE labelled_resource { 'step' = '1' };
      SET LABELS ON RESOURCE labelled_resource { 'step' = '2' };
      SET LABELS ON DOMAIN {{domain}} { 'step' = '1' };
      SET LABELS ON DOMAIN {{domain}} { 'step' = '2' };
      SET LABELS ON USER labelled_user { 'step' = '1' };
      SET LABELS ON USER labelled_user { 'step' = '2' };
      SET LABELS ON USER labelled_user { 'owner' DROP };
      """
    Then within "60s" the leader node has purged its covered raft log beyond stopped node "{{stale}}"
    When node "{{stale}}" is started
    Then within "60s" node "{{stale}}" recovers by installing a raft snapshot
    Given client "reader" is connected to node "{{stale}}"
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Operations", "step": "3"}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON RELAY labelled_events FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"generation": "second"}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE labelled_resource FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Operations", "step": "2"}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON DOMAIN {{domain}} FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Operations", "step": "2"}
      """
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON USER labelled_user FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"step": "2"}
      """

  Scenario Outline: A label transaction refused at COMMIT on <cluster_size> nodes leaves no metadata on any of its entities
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      CREATE RESOURCE labelled_resource LABELS { 'owner' = 'Operations' };
      """
    And client "editor" is connected to the leader node
    And client "contender" is connected to the leader node
    When client "editor" executes these Plexus commands
      """
      BEGIN;
      SET LABELS ON RESOURCE labelled_resource { 'owner' = 'Platform', 'edit' = 'refused' };
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform', 'edit' = 'refused' };
      """
    And client "contender" executes these Plexus commands
      """
      SET LABELS ON SCHEMA labelled_event { 'region' = 'east' };
      """
    And client "editor" attempts to commit its transaction
    Then client "editor" commit was refused because its expected preview is stale
    And client "editor" transaction state is "OPEN"
    When client "contender" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE labelled_resource FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Operations"}
      /view/state = "committed"
      """
    When client "contender" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Operations", "region": "east"}
      """
    When client "editor" executes these Plexus commands
      """
      REVERT;
      """
    And client "contender" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE labelled_resource FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Operations"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Every killed process restarts with the exact labels and a replayed edit keeps its one effect
    Given a 3 node nervix-server process cluster is started
    And the active domain is "{{domain}}"
    And the server process cluster is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'owner' = 'Operations' };
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations', 'empty' = '' };
      CREATE RESOURCE labelled_resource LABELS { 'owner' = 'Operations' };
      CREATE USER labelled_user WITH PASSWORD 'example-password' LABELS { 'owner' = 'Operations' };
      """
    When this Plexus command request with execution reference "killed-label-{{test_id}}" is executed on the server process cluster
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform', 'edit' = 'before-kill' };
      """
    And these Plexus commands are executed on the server process cluster
      """
      SET LABELS ON RESOURCE labelled_resource { 'owner' = 'Platform' };
      SET LABELS ON DOMAIN {{domain}} { 'owner' = 'Platform' };
      SET LABELS ON USER labelled_user { 'owner' = 'Platform' };
      """
    When all server processes receive SIGKILL
    And all server processes restart from their existing databases
    And these Plexus commands are executed on the server process cluster
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"edit": "before-kill", "empty": "", "owner": "Platform"}
      /view/state = "committed"
      """
    When these Plexus commands are executed on the server process cluster
      """
      SHOW LABELS ON RESOURCE labelled_resource FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """
    When these Plexus commands are executed on the server process cluster
      """
      SHOW LABELS ON DOMAIN {{domain}} FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """
    When these Plexus commands are executed on the server process cluster
      """
      SHOW LABELS ON USER labelled_user FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """
    When these Plexus commands are executed on the server process cluster
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Finance' };
      """
    And this Plexus command request with execution reference "killed-label-{{test_id}}" is executed on the server process cluster
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform', 'edit' = 'before-kill' };
      """
    And these Plexus commands are executed on the server process cluster
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"edit": "before-kill", "empty": "", "owner": "Finance"}
      """

  Scenario: A label edit while a unit's owner is down keeps every unit where it is
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      CREATE RELAY labelled_events SCHEMA labelled_event UNBRANCHED LABELS { 'owner' = 'Operations' };
      RELOCATE RELAY labelled_events ONTO NODE {{owner}} IGNORE PREFERENCES;
      """
    Given client "editor" is connected to node "{{leader}}"
    # The verdict that marks the owner unavailable also starts failover, which moves the relay on
    # the leader's next scheduling pass. The move is held once its destination is prepared, so the
    # edits run while the owner is down without racing it.
    And the first forced recovery publication is paused
    When node "{{owner}}" is stopped
    Then within "60s" node "{{leader}}" eventually reports status containing "raft member '{{owner}}' is marked unavailable"
    And the first forced recovery publication reaches its pause
    When client "editor" executes these Plexus commands
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform' };
      """
    And client "editor" executes these Plexus commands
      """
      SET LABELS ON RELAY labelled_events { 'owner' = 'Platform', 'unit' = 'kept' };
      """
    And client "editor" executes these Plexus commands
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "relay" "labelled_events" is saved as placeholder "owner_after_labels"
    And placeholder "owner_after_labels" equals placeholder "owner"
    When client "editor" executes these Plexus commands
      """
      SHOW LABELS ON RELAY labelled_events FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform", "unit": "kept"}
      """
    # Placement changes through failover, never through the edits.
    When the first forced recovery publication is released
    Then within "60s" node "{{leader}}" eventually reports scheduled "relay" "labelled_events" owner different from placeholder "owner"
    When node "{{owner}}" is started
    Given client "returned" is connected to node "{{owner}}"
    When client "returned" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """

  @slots_2
  Scenario Outline: Label edits on every entity of a running rate-limited, partitioned Kafka graph keep its consumer, allowance, placement and deduplication on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "labelled_rate_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (tenant STRING, id STRING);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE PARAMETRIZER tenants SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, id string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY distinct_events SCHEMA event BRANCHED BY tenants;
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'session.timeout.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE KAFKA TOPIC INGESTOR limited
        FROM kafka_main TOPIC 'labelled_rate_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'labelled_rate_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        RATE LIMIT 2 PER 1s BURST 2
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_ids FROM events
        DEDUPLICATE ON input.id MAX TIME 5m BRANCHED BY tenants
        TO distinct_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observations TO distinct_events;
      START;
      """
    Then the stable member of Kafka consumer group "labelled_rate_{{test_id}}" is saved as placeholder "member_before_labels"
    Given client "labeller" is connected to the leader node
    When client "labeller" executes these Plexus commands
      """
      SHOW CLUSTER STATUS;
      """
    Then the owner of every partition of relay "events" is saved with placeholder prefix "events_before"
    And the owner of every partition of deduplicator "distinct_ids" is saved with placeholder prefix "deduplicator_before"
    When the rate limit start is recorded
    And these Kafka messages are rapidly published to topic "labelled_rate_{{test_id}}"
      """
      {"tenant":"acme","id":"a-1"}
      {"tenant":"globex","id":"g-1"}
      {"tenant":"acme","id":"a-1"}
      {"tenant":"globex","id":"g-2"}
      {"tenant":"acme","id":"a-2"}
      {"tenant":"globex","id":"g-1"}
      {"tenant":"acme","id":"a-3"}
      {"tenant":"globex","id":"g-3"}
      """
    And client "labeller" executes these Plexus commands
      """
      BEGIN;
      SET LABELS ON SCHEMA event { 'owner' = 'Platform' };
      SET LABELS ON SCHEMA tenant_key { 'owner' = 'Platform' };
      SET LABELS ON PARAMETRIZER tenants { 'owner' = 'Platform' };
      SET LABELS ON WIRE JSON SCHEMA event_wire { 'owner' = 'Platform' };
      SET LABELS ON CODEC event_codec { 'owner' = 'Platform' };
      SET LABELS ON RELAY events { 'owner' = 'Platform' };
      SET LABELS ON RELAY distinct_events { 'owner' = 'Platform' };
      SET LABELS ON CLIENT kafka_main { 'owner' = 'Platform' };
      SET LABELS ON INGESTOR limited { 'owner' = 'Platform' };
      SET LABELS ON DEDUPLICATOR distinct_ids { 'owner' = 'Platform' };
      COMMIT;
      """
    And client "labeller" executes these Plexus commands
      """
      SET LABELS ON DOMAIN {{domain}} { 'owner' = 'Platform' };
      """
    Then within "60s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "tenant":"acme" | "id":"a-1"
      "tenant":"globex" | "id":"g-1"
      "tenant":"globex" | "id":"g-2"
      "tenant":"acme" | "id":"a-2"
      "tenant":"acme" | "id":"a-3"
      "tenant":"globex" | "id":"g-3"
      """
    And arrival 5 came no sooner than "2500ms" after the recorded rate limit start
    And arrival 6 came no sooner than "3s" after the recorded rate limit start
    And within "30s" Kafka consumer group "labelled_rate_{{test_id}}" next offset for topic "labelled_rate_{{test_id}}" partition 0 is "at least 8"
    And the stable member of Kafka consumer group "labelled_rate_{{test_id}}" is saved as placeholder "member_after_labels"
    And placeholder "member_after_labels" equals placeholder "member_before_labels"
    When client "labeller" executes these Plexus commands
      """
      SHOW CLUSTER STATUS;
      """
    Then the owner of every partition of relay "events" is saved with placeholder prefix "events_after"
    And the owner of every partition of deduplicator "distinct_ids" is saved with placeholder prefix "deduplicator_after"
    And placeholder "events_after_0" equals placeholder "events_before_0"
    And placeholder "events_after_1" equals placeholder "events_before_1"
    And placeholder "deduplicator_after_0" equals placeholder "deduplicator_before_0"
    And placeholder "deduplicator_after_1" equals placeholder "deduplicator_before_1"
    When these Kafka messages are rapidly published to topic "labelled_rate_{{test_id}}"
      """
      {"tenant":"acme","id":"a-2"}
      {"tenant":"acme","id":"a-4"}
      """
    Then within "60s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "tenant":"acme" | "id":"a-4"
      """
    When client "labeller" executes these Plexus commands
      """
      SHOW LABELS ON INGESTOR limited FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Label edits on a WASM processor and the resource it binds keep its pinned version on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE wasm_filter LABELS { 'owner' = 'Operations' };
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE SCHEMA result ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 30m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA result BRANCHED BY by_tenant;
      CREATE CLIENT PRODUCER INGESTOR metric_in
        FROM SCHEMA metric
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO raw_metrics INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_filter VERSION LATEST
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET tenant = branch.tenant, note = coalesce(note, "even")
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      SET LABELS ON RESOURCE wasm_filter { 'owner' = 'Platform' };
      SET LABELS ON WASM PROCESSOR filter_even_rows { 'owner' = 'Platform' };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE WASM PROCESSOR filter_even_rows;
      """
    Then the last command output contains
      """
      USING RESOURCE wasm_filter VERSION 1
      """
    And the last command output contains
      """
      LABELS {
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON RESOURCE wasm_filter FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner": "Platform"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
