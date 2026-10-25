Feature: A backup cut reaches every scheduled state owner

  Background:
    Given these Plexus commands are saved as placeholder "owner_models"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA payment ( tenant STRING, transaction_id STRING, amount I64 );
      CREATE WIRE JSON SCHEMA payment_wire MODE STRICT ( tenant string, transaction_id string, amount integer );
      CREATE CODEC payment_codec FROM WIRE JSON SCHEMA payment_wire TO SCHEMA payment;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
      CREATE RELAY payments SCHEMA payment BRANCHED BY by_tenant;
      CREATE RELAY unique_payments SCHEMA payment BRANCHED BY by_tenant;
      CREATE VHOST edge backup-owners-{{test_id}}.example.com;
      CREATE ENDPOINT payment_ingress ON edge PATH '/payments' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR payment_source FROM payment_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING payment_codec
        TO payments INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR unique_payment_filter FROM payments
        DEDUPLICATE ON input.transaction_id, input.amount
        MAX TIME 30m
        BRANCHED BY by_tenant
        TO unique_payments INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

  Scenario Outline: A quiesced cut refuses to archive without a deduplicator whose owner went down, with <replica_count> replicas
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    When these Plexus statements are executed on the leader node
      """
      {{owner_models}}
      RELOCATE DEDUPLICATOR unique_payment_filter ONTO NODE {{owner}} IGNORE PREFERENCES;
      START;
      CREATE SUBSCRIPTION unique_seen TO unique_payments;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=unique_payment_filter owner={{owner}}
      """
    When http payload is posted to node "{{leader}}" with host "backup-owners-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "{{leader}}" with host "backup-owners-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to node "{{leader}}" with host "backup-owners-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-3","amount":30}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-1"
      key={"tenant":"beta"} | "transaction_id":"txn-2"
      key={"tenant":"alpha"} | "transaction_id":"txn-3"
      """
    Given the backup cut for domain "{{domain}}" will pause before its capture
    When the CLI begins backing up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "unreached-owner.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    When node "{{owner}}" is stopped
    Then within "60s" node "{{leader}}" eventually reports status containing "raft member '{{owner}}' is marked unavailable"
    When the backup cut pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "domain '{{domain}}'"
    And the CLI backup failure mentions "node '{{owner}}'"
    And the CLI backup failure mentions "is not live, so the cut cannot capture its state"
    And backup archive "unreached-owner.nvxb" does not exist
    And node "{{leader}}" eventually reports status containing "{{domain}} status=Running pace=UNPACED"

    Examples:
      | replica_count |
      | 0             |
      | 1             |

  Scenario Outline: A stopped cut refuses to archive without a deduplicator whose owner went down, with <replica_count> replicas
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    When these Plexus statements are executed on the leader node
      """
      {{owner_models}}
      RELOCATE DEDUPLICATOR unique_payment_filter ONTO NODE {{owner}} IGNORE PREFERENCES;
      START;
      CREATE SUBSCRIPTION unique_seen TO unique_payments;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=unique_payment_filter owner={{owner}}
      """
    When http payload is posted to node "{{leader}}" with host "backup-owners-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "{{leader}}" with host "backup-owners-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":20}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-1"
      key={"tenant":"beta"} | "transaction_id":"txn-2"
      """
    When these Plexus statements are executed on the leader node
      """
      STOP;
      """
    Then node "{{leader}}" eventually reports status containing "{{domain}} status=Stopped"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    When the CLI begins backing up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "unreached-stopped-owner.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    When node "{{owner}}" is stopped
    Then within "60s" node "{{leader}}" eventually reports status containing "raft member '{{owner}}' is marked unavailable"
    When the backup cut pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "domain '{{domain}}'"
    And the CLI backup failure mentions "node '{{owner}}'"
    And the CLI backup failure mentions "is not live, so the cut cannot capture its state"
    And backup archive "unreached-stopped-owner.nvxb" does not exist

    Examples:
      | replica_count |
      | 0             |
      | 1             |

  Scenario Outline: A quiesced cut refuses before it pauses when a node that runs the domain's work is down, with <replica_count> replicas
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    And a node other than placeholders "leader" and "owner" is saved as placeholder "worker"
    When these Plexus statements are executed on the leader node
      """
      {{owner_models}}
      RELOCATE DEDUPLICATOR unique_payment_filter ONTO NODE {{owner}} IGNORE PREFERENCES;
      START;
      CREATE SUBSCRIPTION unique_seen TO unique_payments;
      """
    And http payload is posted to node "{{leader}}" with host "backup-owners-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-1"
      """
    # The worker runs the endpoint ingestor, as every node does, and owns no archived state. The cut
    # pauses once its lease keeps failover from moving that work, and the worker stops meanwhile.
    Given the backup cut for domain "{{domain}}" will pause once it holds its lease
    When the CLI begins backing up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "unreached-worker.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its lease pause
    When node "{{worker}}" is stopped
    Then within "60s" node "{{leader}}" eventually reports status containing "raft member '{{worker}}' is marked unavailable"
    When the backup lease pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "domain '{{domain}}'"
    And the CLI backup failure mentions "node '{{worker}}'"
    And the CLI backup failure mentions "is not live, so the quiesced cut cannot drain its work"
    And backup archive "unreached-worker.nvxb" does not exist
    And node "{{leader}}" eventually reports status containing "{{domain}} status=Running pace=UNPACED"

    Examples:
      | replica_count |
      | 0             |
      | 1             |

  Scenario Outline: A quiesced cut refuses before it pauses when a deduplicator's owner is down, with <replica_count> replicas
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    When these Plexus statements are executed on the leader node
      """
      {{owner_models}}
      RELOCATE DEDUPLICATOR unique_payment_filter ONTO NODE {{owner}} IGNORE PREFERENCES;
      START;
      CREATE SUBSCRIPTION unique_seen TO unique_payments;
      """
    And http payload is posted to node "{{leader}}" with host "backup-owners-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-1"
      """
    # The cut's lease keeps failover from moving the deduplicator, so its stopped owner stays its
    # scheduled primary when the cut checks the nodes it must reach, before it pauses the domain.
    Given the backup cut for domain "{{domain}}" will pause once it holds its lease
    When the CLI begins backing up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "unreached-owner-before-pause.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its lease pause
    When node "{{owner}}" is stopped
    Then within "60s" node "{{leader}}" eventually reports status containing "raft member '{{owner}}' is marked unavailable"
    When the backup lease pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "domain '{{domain}}'"
    And the CLI backup failure mentions "node '{{owner}}'"
    And the CLI backup failure mentions "is not live, so the cut cannot capture its state"
    And backup archive "unreached-owner-before-pause.nvxb" does not exist
    And node "{{leader}}" eventually reports status containing "{{domain}} status=Running pace=UNPACED"

    Examples:
      | replica_count |
      | 0             |
      | 1             |

  Scenario Outline: A <cut> cut refuses while the schedule places a deduplicator's state on no node
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    And a node other than placeholders "leader" and "owner" is saved as placeholder "survivor"
    When these Plexus statements are executed on the leader node
      """
      {{owner_models}}
      RELOCATE DEDUPLICATOR unique_payment_filter ONTO NODE {{owner}} IGNORE PREFERENCES;
      START;
      CREATE SUBSCRIPTION unique_seen TO unique_payments;
      """
    And http payload is posted to node "{{leader}}" with host "backup-owners-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-1"
      """
    # With both live nodes cordoned, no node can host the deduplicator once its owner stops, so the
    # schedule places it on none while its keys remain on the stopped owner.
    When these Plexus statements are executed through the client on node "{{leader}}"
      """
      CORDON NODE {{leader}};
      CORDON NODE {{survivor}};
      """
    And node "{{owner}}" is stopped
    Then within "60s" node "{{leader}}" eventually reports status containing "raft member '{{owner}}' is marked unavailable"
    And within "60s" node "{{leader}}" eventually reports status containing "- domain={{domain}} kind=deduplicator name=unique_payment_filter owner=- replicas=-"
    When the CLI backs up "domain {{domain}} <options>" from node "{{leader}}" into "unplaced-<cut>.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "domain '{{domain}}'"
    And the CLI backup failure mentions "is placed on no node, so the cut cannot capture its state"
    And backup archive "unplaced-<cut>.nvxb" does not exist

    Examples:
      | cut      | options         |
      | quiesced | --timeout 60s   |
      | live     | --without-pause |
