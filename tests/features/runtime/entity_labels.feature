@entity_labels
Feature: Domain entity labels
  Labels are bounded control-plane metadata. Their transaction and persistence follow the
  entity they describe, while a label-only edit leaves its running execution intact.

  Scenario Outline: A domain Model retains exact labels through creation and mutation
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS {
        'owner' = 'Operations',
        'empty' = '',
        'team/path' = 'Mixed Case / Δ'
      };
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Operations"
      /labels/empty = ""
      /labels/team~1path = "Mixed Case / Δ"
      """
    When these Plexus commands are executed on the leader node
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform', 'region' = 'east' };
      DROP LABELS ON SCHEMA labelled_event { 'empty', 'absent' };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Platform"
      /labels/region = "east"
      /labels/team~1path = "Mixed Case / Δ"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Metadata leaves buffered branches alone and mixed configuration still drains them
    Given HTTP receiver "emitter" is running
    And HTTP receiver "emitter" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64, tenant STRING);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY orders SCHEMA incoming BRANCHED BY tenants;
      CREATE CLIENT PRODUCER INGESTOR orders_in
        FROM SCHEMA incoming
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT publication_api CONFIG {
        'endpoint' = '{{http_receiver.emitter}}', 'timeout_ms' = 60000
      };
      CREATE HTTP REQUEST EMITTER published FROM orders
        TO publication_api METHOD 'POST' PATH concat('/orders/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens producer "orders" on ingestor "orders_in" expecting fields "id STRING, amount I64, tenant STRING"
    And producer "orders" submits batch "buffered" with rows
      | id  | amount | tenant |
      | o-1 | 1      | acme   |
      | o-2 | 2      | globex |
      | o-3 | 3      | acme   |
    Then batch "buffered" remains pending for "200ms"
    And HTTP receiver "emitter" has captured exactly 0 requests
    When these Plexus commands are executed on the leader node
      """
      BEGIN;
      SET LABELS ON SCHEMA incoming { 'owner' = 'Platform' };
      SET LABELS ON SCHEMA tenant_key { 'owner' = 'Platform' };
      SET LABELS ON INGESTOR orders_in { 'owner' = 'Platform' };
      SET LABELS ON RELAY orders { 'owner' = 'Platform' };
      COMMIT;
      SET LABELS ON INGESTOR orders_in { 'owner' = 'Platform' };
      DROP LABELS ON RELAY orders { 'absent' };
      """
    Then batch "buffered" remains pending for "200ms"
    And HTTP receiver "emitter" has captured exactly 0 requests
    And producer "orders" eventually reports admission "open"
    When these Plexus commands are executed on the leader node
      """
      BEGIN;
      SET LABELS ON SCHEMA incoming { 'owner' = 'Operations' };
      SET LABELS ON SCHEMA tenant_key { 'owner' = 'Operations' };
      SET LABELS ON INGESTOR orders_in { 'owner' = 'Operations' };
      ALTER CLIENT PRODUCER INGESTOR orders_in REPLACE ROUTE TO orders
        INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH EACH 10ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      COMMIT;
      """
    Then batch "buffered" completes
    And HTTP receiver "emitter" has captured exactly 3 requests
    And producer "orders" eventually reports admission "open"
    When producer "orders" submits batch "after configuration" with rows
      | id  | amount | tenant |
      | o-4 | 4      | globex |
      | o-5 | 5      | acme   |
    Then batch "after configuration" completes
    And HTTP receiver "emitter" has captured exactly 5 requests
    And producer "orders" holds no batches

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Concurrent metadata edits invalidate an earlier commit preview
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      """
    And client "editor" is connected to the leader node
    And client "contender" is connected to the leader node
    When client "editor" executes these Plexus commands
      """
      BEGIN;
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform' };
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
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Operations"
      /labels/region = "east"
      """
    When client "editor" executes these Plexus commands
      """
      REVERT;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A configuration alteration in the same transaction keeps its labels
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      """
    When these Plexus commands are executed on the leader node
      """
      BEGIN;
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform' };
      ALTER SCHEMA labelled_event ADD FIELD note STRING OPTIONAL;
      COMMIT;
      SHOW CREATE SCHEMA labelled_event;
      """
    Then the last command output contains
      """
      note STRING OPTIONAL
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Platform"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_labels_publication
  Scenario Outline: Label commits preserve two branch states and in-flight acknowledgement trees
    Given HTTP receiver "emitter" is running
    And HTTP receiver "emitter" answers with
      """
      hold response until released
      """
    And HTTP receiver "emitter" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64, tenant STRING);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY orders SCHEMA incoming BRANCHED BY tenants;
      CREATE RELAY distinct_orders SCHEMA incoming BRANCHED BY tenants;
      CREATE CLIENT PRODUCER INGESTOR orders_in
        FROM SCHEMA incoming
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_ids FROM orders
        DEDUPLICATE ON input.id MAX TIME 5m BRANCHED BY tenants
        TO distinct_orders INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE HTTP CLIENT publication_api CONFIG {
        'endpoint' = '{{http_receiver.emitter}}', 'timeout_ms' = 60000
      };
      CREATE HTTP REQUEST EMITTER published FROM distinct_orders
        INSTANCES 3
        TO publication_api METHOD 'POST' PATH concat('/orders/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens producer "orders" on ingestor "orders_in" expecting fields "id STRING, amount I64, tenant STRING"
    And producer "orders" submits batch "before labels" with rows
      | id  | amount | tenant |
      | o-1 | 1      | acme   |
      | o-1 | 2      | globex |
    Then HTTP receiver "emitter" eventually receives at least 1 request
    And batch "before labels" remains pending for "200ms"
    When these Plexus commands are executed on the leader node
      """
      BEGIN;
      SET LABELS ON SCHEMA incoming { 'owner' = 'Platform' };
      SET LABELS ON BRANCH tenants { 'owner' = 'Platform' };
      SET LABELS ON SCHEMA tenant_key { 'owner' = 'Platform' };
      SET LABELS ON RELAY orders { 'owner' = 'Platform' };
      SET LABELS ON INGESTOR orders_in { 'owner' = 'Platform' };
      SET LABELS ON DEDUPLICATOR distinct_ids { 'owner' = 'Platform' };
      SET LABELS ON CLIENT publication_api { 'owner' = 'Platform' };
      SET LABELS ON EMITTER published { 'owner' = 'Platform' };
      COMMIT;
      """
    Then batch "before labels" remains pending for "200ms"
    And producer "orders" eventually reports admission "open"
    When HTTP receiver "emitter" releases its held responses with "respond 200"
    Then batch "before labels" completes
    When producer "orders" submits batch "after labels" with rows
      | id  | amount | tenant |
      | o-1 | 10     | acme   |
      | o-2 | 3      | globex |
      | o-1 | 20     | globex |
      | o-2 | 4      | acme   |
    Then batch "after labels" completes
    And HTTP receiver "emitter" has captured exactly 4 requests
    And HTTP receiver "emitter" captured one request that is
      """
      POST /orders/acme/o-1
      """
    And HTTP receiver "emitter" captured one request that is
      """
      POST /orders/globex/o-1
      """
    And HTTP receiver "emitter" captured one request that is
      """
      POST /orders/acme/o-2
      """
    And HTTP receiver "emitter" captured one request that is
      """
      POST /orders/globex/o-2
      """
    And producer "orders" holds no batches

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_labels_restore
  Scenario Outline: Restart and archive restore retain the captured label revision
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations', 'empty' = '' };
      CREATE RELAY labelled_events SCHEMA labelled_event UNBRANCHED LABELS { 'purpose' = 'audit' };
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform', 'team/path' = ' Δ ' };
      """
    When the cluster is restarted
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Platform"
      /labels/empty = ""
      /labels/team~1path = " Δ "
      /view/state = "committed"
      """
    And the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} <state_clause>" from node "{{leader}}" into "labels.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'After capture' };
      """
    And the CLI restores "domain {{domain}} --as {{domain}}_copy" from "labels.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 0 resource versions and 2 models
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Platform"
      /labels/empty = ""
      /labels/team~1path = " Δ "
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON RELAY labelled_events FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/purpose = "audit"
      """

    Examples:
      | cluster_size | state_clause    |
      | 1            |                 |
      | 3            |                 |
      | 1            | --without-state |
      | 3            | --without-state |

  @entity_labels_publication
  Scenario Outline: Label candidates are visible only to their transaction and revert atomically
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      """
    And client "editor" is connected to the leader node
    And client "observer" is connected to the leader node
    When client "editor" executes these Plexus commands
      """
      BEGIN;
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform', 'new' = 'staged' };
      """
    And client "editor" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Platform"
      /labels/new = "staged"
      /view/state = "staged"
      """
    When client "observer" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Operations"
      /view/state = "committed"
      """
    When client "editor" executes these Plexus commands
      """
      REVERT;
      """
    And client "editor" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Operations"
      /view/state = "committed"
      """
    When client "editor" executes these Plexus commands
      """
      BEGIN;
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Platform' };
      COMMIT;
      """
    And client "observer" executes these Plexus commands
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Platform"
      /view/state = "committed"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Creation no-ops preserve labels and drop cleans the entity metadata
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE IF NOT EXISTS SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Other' };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Operations"
      """
    When these Plexus commands are executed on the leader node
      """
      DROP SCHEMA labelled_event;
      CREATE SCHEMA labelled_event ( value U32 );
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Label keys cannot cross domains with identical entity names
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the active domain is saved as placeholder "first_domain"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'First' };
      """
    And the active domain is "second_{{test_id}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Second' };
      """
    When these Plexus commands are executed on the leader node
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Second updated' };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Second updated"
      """
    Given the active domain is "{{first_domain}}"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "First"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Invalid label operations leave the complete committed set intact
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'owner' = 'Operations' };
      """
    When these Plexus commands fail
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'First', 'owner' = 'Second' };
      """
    And these Plexus commands fail
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Changed', 'Uppercase' = 'refused' };
      """
    And these Plexus commands fail
      """
      SET LABELS ON SCHEMA missing_event { 'owner' = 'Nobody' };
      """
    And these Plexus commands fail
      """
      DROP LABELS ON SCHEMA missing_event { 'absent' };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"owner":"Operations"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Committed label limits validate a whole result and refuse invalid reductions
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      ALTER CLUSTER SET ENTITY_LABELS_MAX_COUNT = 2,
        ENTITY_LABELS_MAX_KEY_BYTES = 4B,
        ENTITY_LABELS_MAX_VALUE_BYTES = 4B,
        ENTITY_LABELS_MAX_BYTES = 12B;
      CREATE SCHEMA labelled_event ( value U32 ) LABELS { 'key1' = 'one' };
      """
    When these Plexus commands fail
      """
      SET LABELS ON SCHEMA labelled_event { 'key2' = 'two' };
      """
    And these Plexus commands fail
      """
      SET LABELS ON SCHEMA labelled_event { 'large' = 'one' };
      """
    And these Plexus commands fail
      """
      SET LABELS ON SCHEMA labelled_event { 'key1' = 'three' };
      """
    And these Plexus commands fail
      """
      ALTER CLUSTER SET ENTITY_LABELS_MAX_BYTES = 6B;
      """
    And these Plexus commands are executed on the leader node
      """
      SET LABELS ON SCHEMA labelled_event { 'k2' = 'two' };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"k2":"two","key1":"one"}
      """
    When these Plexus commands fail
      """
      ALTER CLUSTER SET ENTITY_LABELS_MAX_COUNT = 1;
      """
    And these Plexus commands fail
      """
      SET LABELS ON SCHEMA labelled_event { 'k3' = '' };
      """
    And these Plexus commands are executed on the leader node
      """
      DROP LABELS ON SCHEMA labelled_event { 'k2' };
      ALTER CLUSTER SET ENTITY_LABELS_MAX_COUNT = 1;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA labelled_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"key1":"one"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Wire schema label targets retain their exact format identity
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE WIRE JSON SCHEMA payload MODE STRICT ( value INTEGER ) LABELS { 'format' = 'json' };
      CREATE WIRE CBOR SCHEMA payload MODE STRICT ( value INTEGER ) LABELS { 'format' = 'cbor' };
      CREATE WIRE AVRO SCHEMA payload MODE STRICT ( value long ) LABELS { 'format' = 'avro' };
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON WIRE JSON SCHEMA payload FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/format = "json"
      """
    When these Plexus commands are executed on the leader node
      """
      SET LABELS ON WIRE CBOR SCHEMA payload { 'format' = 'CBOR' };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON WIRE CBOR SCHEMA payload FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/format = "CBOR"
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON WIRE AVRO SCHEMA payload FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/format = "avro"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Renaming a placement retains its metadata
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE PLACEMENT preferred DOMAIN WIDE PREFER COLOCATION LABELS { 'owner' = 'Operations' };
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER PLACEMENT preferred RENAME TO renamed;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON PLACEMENT renamed FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/owner = "Operations"
      /entity/node/identifier = "renamed"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
