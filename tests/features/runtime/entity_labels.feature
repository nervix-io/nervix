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
      SET LABELS ON SCHEMA labelled_event {
        'owner' = 'Platform',
        'region' = 'east',
        'empty' DROP,
        'absent' DROP
      };
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

  Scenario Outline: One SET LABELS statement sets and removes keys as one atomic change
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA labelled_event ( value U32 );
      CREATE RESOURCE orders_schema;
      CREATE USER orders_reader WITH PASSWORD 'reader-password';
      ALTER CLUSTER SET ENTITY_LABELS_MAX_COUNT = 2;
      SET LABELS ON <target> { 'one' = '1', 'two' = '2' };
      """
    When these Plexus commands are executed on the leader node
      """
      SET LABELS ON <target> { 'three' = '3', 'two' DROP, 'absent' DROP };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON <target> FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"one":"1","three":"3"}
      """
    When these Plexus commands fail
      """
      SET LABELS ON <target> { 'four' = '4', 'five' = '5', 'one' DROP };
      """
    And these Plexus commands fail
      """
      SET LABELS ON <target> { 'one' = 'again', 'one' DROP };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON <target> FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"one":"1","three":"3"}
      """

    Examples:
      | cluster_size | target                 |
      | 1            | SCHEMA labelled_event  |
      | 3            | SCHEMA labelled_event  |
      | 1            | DOMAIN {{domain}}      |
      | 3            | DOMAIN {{domain}}      |
      | 1            | RESOURCE orders_schema |
      | 3            | RESOURCE orders_schema |
      | 1            | USER orders_reader     |
      | 3            | USER orders_reader     |

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
      CREATE PARAMETRIZER tenants SCHEMA tenant_key TTL 5m;
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
      SET LABELS ON RELAY orders { 'absent' DROP };
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
      CREATE PARAMETRIZER tenants SCHEMA tenant_key TTL 5m;
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
      SET LABELS ON PARAMETRIZER tenants { 'owner' = 'Platform' };
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
      SET LABELS ON SCHEMA missing_event { 'absent' DROP };
      """
    And these Plexus commands fail
      """
      SET LABELS ON SCHEMA labelled_event { 'owner' = 'Changed', 'owner' DROP };
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
      SET LABELS ON SCHEMA labelled_event { 'k2' DROP };
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

  @entity_labels_publication
  Scenario Outline: Resource label edits publish with their transaction and disappear on revert
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE orders_schema;
      """
    And client "editor" is connected to the leader node
    And client "observer" is connected to the leader node
    When client "editor" executes these Plexus commands
      """
      BEGIN;
      SET LABELS ON RESOURCE orders_schema { 'status' = 'checked' };
      CREATE RESOURCE imported_schema LABELS { 'team' = 'Orders' };
      SET LABELS ON RESOURCE imported_schema { 'status' = 'pending', 'team' DROP, 'absent' DROP };
      CREATE SCHEMA orders_event (value U32);
      """
    And client "editor" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE imported_schema FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"status":"pending"}
      /view/state = "staged"
      """
    When client "observer" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE orders_schema FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {}
      /view/state = "committed"
      """
    When client "editor" executes these Plexus commands
      """
      COMMIT;
      """
    And client "observer" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE orders_schema FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/status = "checked"
      /view/state = "committed"
      """
    When client "editor" executes these Plexus commands
      """
      BEGIN;
      SET LABELS ON RESOURCE orders_schema { 'status' DROP };
      REVERT;
      """
    And client "observer" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE orders_schema FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/status = "checked"
      /view/state = "committed"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_labels_publication
  Scenario Outline: Domain, resource and user labels survive edits and a cluster restart
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'team' = 'Orders' };
      CREATE RESOURCE orders_schema LABELS { 'status' = 'draft', 'empty' = '' };
      CREATE USER orders_reader WITH PASSWORD 'reader-password' LABELS { 'purpose' = 'Orders' };
      CREATE SCHEMA label_event ( value U32 );
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON DOMAIN {{domain}} FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /entity/kind = "domain"
      /labels/team = "Orders"
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON SCHEMA label_event FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {}
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON RESOURCE orders_schema FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /entity/kind = "resource"
      /entity/identifier = "orders_schema"
      /labels/status = "draft"
      /labels/empty = ""
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON USER orders_reader FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /entity = {"kind":"user","name":"orders_reader"}
      /labels/purpose = "Orders"
      """
    And the last command output does not contain
      """
      reader-password
      """
    When these Plexus commands are executed on the leader node
      """
      SET LABELS ON DOMAIN {{domain}} { 'team' = 'Platform' };
      SET LABELS ON RESOURCE orders_schema { 'status' = 'approved' };
      SET LABELS ON RESOURCE orders_schema { 'empty' DROP, 'absent' DROP };
      SET LABELS ON USER orders_reader { 'purpose' = 'Audit' };
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: running
      """
    And the last command output contains
      """
      labels: {"team":"Platform"}
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE RESOURCE orders_schema;
      """
    Then the last command output contains
      """
      latest: (none)
      """
    And the last command output contains
      """
      labels: {"status":"approved"}
      """
    When the cluster is restarted
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON DOMAIN {{domain}} FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/team = "Platform"
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON RESOURCE orders_schema FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"status":"approved"}
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON USER orders_reader FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/purpose = "Audit"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Resource labels stay in the selected domain and administrative edits are refused in a transaction
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'team' = 'original' };
      CREATE UNPACED DOMAIN {{domain}}_other;
      CREATE USER labels_reader WITH PASSWORD 'reader-password' LABELS { 'team' = 'original' };
      """
    And client "editor" is connected to the leader node
    And client "other" is connected to the leader node
    When client "editor" selects domain "{{domain}}"
    And client "other" selects domain "{{domain}}_other"
    And client "editor" executes these Plexus commands
      """
      CREATE RESOURCE shared LABELS { 'team' = 'orders' };
      BEGIN;
      """
    And client "other" executes these Plexus commands
      """
      CREATE RESOURCE shared LABELS { 'team' = 'returns' };
      """
    When client "editor" fails to execute these Plexus commands
      """
      SET LABELS ON DOMAIN {{domain}} { 'team' = 'refused' };
      """
    Then the last command error contains
      """
      cannot be queued in a transaction
      """
    When client "editor" fails to execute these Plexus commands
      """
      SET LABELS ON USER labels_reader { 'team' DROP };
      """
    Then the last command error contains
      """
      cannot be queued in a transaction
      """
    When client "editor" executes these Plexus commands
      """
      SET LABELS ON RESOURCE shared { 'team' = 'new-orders' };
      COMMIT;
      """
    And client "other" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE shared FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/team = "returns"
      """
    When client "editor" executes these Plexus commands
      """
      SHOW LABELS ON RESOURCE shared FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/team = "new-orders"
      """
    When client "other" executes these Plexus commands
      """
      SHOW LABELS ON DOMAIN {{domain}} FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/team = "original"
      """
    When client "other" executes these Plexus commands
      """
      SHOW LABELS ON USER labels_reader FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/team = "original"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_labels_restore
  Scenario Outline: A domain backup retains domain and catalog labels within its original scope
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}} LABELS { 'team' = 'Orders' };
      CREATE RESOURCE pending_schema LABELS { 'status' = 'draft' };
      CREATE USER backup_reader WITH PASSWORD 'reader-password' LABELS { 'purpose' = 'Orders' };
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --without-state --without-resources" from node "{{leader}}" into "labelled.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "labelled.nvxb" as json
    Then the described backup holds no users
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "labelled.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 0 resource versions and 0 models
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON DOMAIN {{domain}}_copy FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/team = "Orders"
      """
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON RESOURCE pending_schema FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/status = "draft"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_labels_restore
  Scenario Outline: A cluster backup restores user labels with the same sign-in
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE USER backup_reader WITH PASSWORD 'reader-password' LABELS { 'purpose' = 'Orders' };
      SET LABELS ON USER backup_reader { 'purpose' = 'Audit' };
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster --without-state" from node "{{leader}}" into "labelled-cluster.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "cluster --on-existing-user skip" from "labelled-cluster.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded with users 1 created, 1 skipped and 0 replaced
    Given client "reader" is connected to the leader node as user "backup_reader" with password "reader-password"
    When client "reader" executes these Plexus commands
      """
      SHOW LABELS ON USER backup_reader FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels/purpose = "Audit"
      """
    And the last command output does not contain
      """
      reader-password
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A committed label limit accounts for each administrative owner
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE orders_schema;
      CREATE USER orders_reader WITH PASSWORD 'reader-password';
      SET LABELS ON <target> { 'one' = '1', 'two' = '2' };
      """
    When these Plexus commands fail with "does not exist"
      """
      SET LABELS ON <missing> { 'absent' DROP };
      """
    And these Plexus commands fail
      """
      ALTER CLUSTER SET ENTITY_LABELS_MAX_COUNT = 1;
      """
    And these Plexus commands are executed on the leader node
      """
      SET LABELS ON <target> { 'two' DROP, 'absent' DROP };
      ALTER CLUSTER SET ENTITY_LABELS_MAX_COUNT = 1;
      """
    When these Plexus commands fail
      """
      SET LABELS ON <target> { 'two' = '2' };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW LABELS ON <target> FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /labels = {"one":"1"}
      /policy/limits/count = 1
      """

    Examples:
      | cluster_size | target                 | missing                |
      | 1            | DOMAIN {{domain}}      | DOMAIN absent_domain   |
      | 3            | DOMAIN {{domain}}      | DOMAIN absent_domain   |
      | 1            | RESOURCE orders_schema | RESOURCE absent_schema |
      | 3            | RESOURCE orders_schema | RESOURCE absent_schema |
      | 1            | USER orders_reader     | USER absent_reader     |
      | 3            | USER orders_reader     | USER absent_reader     |

  @entity_labels_descriptions
  Scenario Outline: Every description of a domain Model shows the labels its entity carries
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And node "node-1" has resource directory "lookup_dir" containing
      """
      {
        "lookup.jsonl": "{\"id\":\"one\",\"value\":1}\n"
      }
      """
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE RESOURCE score_model;
      UPLOAD RESOURCE score_model VERSION '{{onnx_model}}';
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      CREATE RESOURCE lookup_bundle;
      UPLOAD RESOURCE lookup_bundle VERSION '{{lookup_dir}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( region STRING, sequence I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( region string, sequence integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA region_key ( region STRING );
      CREATE PARAMETRIZER by_region SCHEMA region_key TTL 5m
        LABELS { 'kind' = 'parametrizer' };
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_region
        LABELS { 'kind' = 'relay' };
      CREATE RELAY settlements SCHEMA event BRANCHED BY by_region;
      CREATE RELAY ordered SCHEMA event BRANCHED BY by_region;
      CREATE RELAY settled SCHEMA event BRANCHED BY by_region;
      CREATE RELAY routed SCHEMA event BRANCHED BY by_region;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_region;
      CREATE RELAY counted SCHEMA event BRANCHED BY by_region;
      CREATE RELAY unkeyed SCHEMA event UNBRANCHED;
      CREATE RELAY regrouped SCHEMA event BRANCHED BY by_region;
      CREATE VHOST edge described-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events'
        LABELS { 'kind' = 'endpoint' };
      CREATE HTTP ENDPOINT INGESTOR intake FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO incoming INHERIT ALL BRANCHED BY by_region SET region = message.region
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
        LABELS { 'kind' = 'ingestor' };
      CREATE REORDERER reorder FROM incoming BY input.sequence MAX TIME 10m
        BRANCHED BY by_region
        TO ordered INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        LABELS { 'kind' = 'reorderer' };
      CREATE CORRELATOR settle
        LEFT FROM incoming
        RIGHT FROM settlements
        CORRELATE WHERE left.sequence = right.sequence
        MATCH EARLIEST
        MAX TIME 10m
        ON CORRELATION TIMEOUT DROP, DROP
        BRANCHED BY by_region
        TO settled SET region = left.region, sequence = left.sequence
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        LABELS { 'kind' = 'correlator' };
      CREATE JUNCTION route FROM incoming BRANCHED BY by_region
        TO routed INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        LABELS { 'kind' = 'junction' };
      CREATE DEDUPLICATOR deduplicate FROM incoming
        DEDUPLICATE ON input.sequence
        MAX TIME 10m
        BRANCHED BY by_region
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        LABELS { 'kind' = 'deduplicator' };
      CREATE WINDOW PROCESSOR count_events FROM incoming
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        BRANCHED BY by_region
        TO counted SET region = FIRST(input.region), sequence = COUNT(input.sequence)
          ON MESSAGE ERROR LOG
        LABELS { 'kind' = 'window processor' };
      CREATE REINGESTOR regroup FROM unkeyed
        TO regrouped INHERIT ALL BRANCHED BY by_region SET region = message.region
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        LABELS { 'kind' = 'reingestor' };
      CREATE HTTP CLIENT api CONFIG { 'endpoint' = 'http://127.0.0.1:1', 'timeout_ms' = 5000 }
        LABELS { 'kind' = 'client' };
      CREATE HTTP REQUEST EMITTER publish FROM routed
        ATTACHED
        TO api METHOD 'POST' PATH concat('/events/', input.region)
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1m WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
        LABELS { 'kind' = 'emitter' };
      CREATE UDF add_one WITH ROTO_0_13 ARGS (value I64) RETURNS I64
        CODE $roto$ fn add_one(value: I64Column) -> I64Column { value.add_s(1) } $roto$
        LABELS { 'kind' = 'udf' };
      CREATE PLACEMENT domain_policy DOMAIN WIDE PREFER COLOCATION
        LABELS { 'kind' = 'placement' };
      CREATE SCHEMA lookup_entry ( id STRING, value I64 );
      CREATE WIRE JSON SCHEMA lookup_entry_wire MODE STRICT ( id string, value integer );
      CREATE WIRE JSON CODEC lookup_entry_codec
        FROM WIRE SCHEMA lookup_entry_wire TO SCHEMA lookup_entry;
      CREATE HASH MAP lookup_by_id
        KEY id
        FROM RESOURCE lookup_bundle VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING lookup_entry_codec
        LABELS { 'kind' = 'hash map' };
      CREATE SCHEMA features ( vector <vector_type> );
      CREATE SCHEMA scored ( score <score_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER score FROM features
        USING RESOURCE score_model VERSION 1
        FILE 'models/simple_score.onnx' WORKING MEMORY RELEASED
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        UNBRANCHED
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        LABELS { 'kind' = 'inferencer' };
      CREATE SCHEMA metric ( value I32 );
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE WASM PROCESSOR filter_even FROM raw_metrics
        USING RESOURCE wasm_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        MAX TABLE ELEMENTS 4096
        UNBRANCHED
        TO filtered_metrics SET value = value
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG
        LABELS { 'kind' = 'wasm processor' };
      """
    Then every one of these descriptions shows its entity's labels
      | description                            | labels                              |
      | DESCRIBE PARAMETRIZER by_region        | labels: {"kind":"parametrizer"}     |
      | DESCRIBE RELAY incoming                | labels: {"kind":"relay"}            |
      | DESCRIBE ENDPOINT ingress              | labels: {"kind":"endpoint"}         |
      | DESCRIBE CLIENT api                    | labels: {"kind":"client"}           |
      | DESCRIBE INGESTOR intake               | labels: {"kind":"ingestor"}         |
      | DESCRIBE EMITTER publish               | labels: {"kind":"emitter"}          |
      | DESCRIBE JUNCTION route                | labels: {"kind":"junction"}         |
      | DESCRIBE DEDUPLICATOR deduplicate      | labels: {"kind":"deduplicator"}     |
      | DESCRIBE REORDERER reorder             | labels: {"kind":"reorderer"}        |
      | DESCRIBE CORRELATOR settle             | labels: {"kind":"correlator"}       |
      | DESCRIBE REINGESTOR regroup            | labels: {"kind":"reingestor"}       |
      | DESCRIBE WINDOW PROCESSOR count_events | labels: {"kind":"window processor"} |
      | DESCRIBE HASH MAP lookup_by_id         | labels: {"kind":"hash map"}         |
      | DESCRIBE INFERENCER score              | 'kind' = 'inferencer'               |
      | DESCRIBE WASM PROCESSOR filter_even    | labels: {"kind":"wasm processor"}   |
      | DESCRIBE UDF add_one                   | labels: {"kind":"udf"}              |
      | DESCRIBE PLACEMENT domain_policy       | labels: {"kind":"placement"}        |

    Examples:
      | cluster_size | tensor_type       | vector_type   | score_type    |
      | 1            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
