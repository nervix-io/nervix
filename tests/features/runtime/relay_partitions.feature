Feature: Relay partitions
  A branch declared with `PARTITIONS n` splits the concrete keys of every relay it branches into n
  partitions, partition = the key fingerprint's seed mod n, and every partition has its own owner.
  The keys below are chosen by partition. With PARTITIONS 4: eta and theta land in partition 0,
  beta and gamma in 1, lambda and rho in 2, acme and delta in 3. With PARTITIONS 2: eta, theta,
  lambda and rho land in partition 0, and acme, beta, gamma and delta in 1.

  Scenario Outline: A branch declares its partition count and SHOW CREATE BRANCH renders every count but the default
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m PARTITIONS 4 MAX INSTANCES 100 EVICT LRU;

      CREATE BRANCH by_tenant_single
        SCHEMA tenant_branch TTL 5m PARTITIONS 1;

      CREATE BRANCH by_tenant_default
        SCHEMA tenant_branch TTL 5m;
      """
    And these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 4 MAX INSTANCES 100 EVICT LRU;
      """
    When these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant_single;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant_single SCHEMA tenant_branch TTL 5m;
      """
    When these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant_default;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant_default SCHEMA tenant_branch TTL 5m;
      """
    When the cluster is restarted
    And these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 4 MAX INSTANCES 100 EVICT LRU;
      """

    Examples:
      | cluster_size | reader |
      | 1            | node-1 |
      | 3            | node-2 |

  Scenario Outline: A partitioned relay delivers every partition to one subscription and keeps each key's order to one emitter
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        seq I64
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m PARTITIONS 4;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_tenant;

      CREATE ZEROMQ CLIENT partitioned_out
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/ingest' TYPE HTTP;

      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE ZEROMQ PUSH EMITTER partitioned_notifications_out
        FROM notifications TO partitioned_out MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    And these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION partitioned_subscription TO notifications;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"eta","seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"lambda","seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"eta","seq":3}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","seq":3}
      """
    Then within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"eta" | "seq":2
      "tenant":"eta" | "seq":3
      "tenant":"acme" | "seq":1
      "tenant":"acme" | "seq":2
      "tenant":"acme" | "seq":3
      "tenant":"beta" | "seq":1
      "tenant":"beta" | "seq":2
      "tenant":"lambda" | "seq":1
      "tenant":"lambda" | "seq":2
      """
    And within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"eta" | "seq":2
      "tenant":"eta" | "seq":3
      "tenant":"acme" | "seq":1
      "tenant":"acme" | "seq":2
      "tenant":"acme" | "seq":3
      "tenant":"beta" | "seq":1
      "tenant":"beta" | "seq":2
      "tenant":"lambda" | "seq":1
      "tenant":"lambda" | "seq":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A branch's relay partitions are scheduled as units spread over the cluster and DESCRIBE RELAY lists each owner
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( tenant STRING, seq I64 );
      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m PARTITIONS 4;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_tenant
        CAPACITY 3;

      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 partitions of relay "notifications"
    And the last cluster status places the partitions of relay "notifications" on at least <owners> distinct owners
    And the owner of every partition of relay "notifications" in the last cluster status is saved with placeholder prefix "relay_owner"
    When these Plexus commands are executed on node "<reader>"
      """
      DESCRIBE RELAY notifications;
      """
    Then the last command output contains
      """
      partitions: 4
      partition 0: owner={{relay_owner_0}} replicas=-
      partition 1: owner={{relay_owner_1}} replicas=-
      partition 2: owner={{relay_owner_2}} replicas=-
      partition 3: owner={{relay_owner_3}} replicas=-
      capacity: 3
      """

    Examples:
      | cluster_size | owners | reader |
      | 1            | 1      | node-1 |
      | 3            | 2      | node-3 |

  Scenario Outline: DESCRIBE RELAY WHERE is answered by the owner of the key's partition
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( tenant STRING, seq I64 );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m PARTITIONS 4;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/ingest' TYPE HTTP;

      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    And the owner of every partition of relay "notifications" in the last cluster status is saved with placeholder prefix "relay_owner"
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","seq":1}
      """
    Then within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'eta');
      """
    And within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'acme');
      """
    And within "30s" node "<reader>" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'theta');
      """
    When these Plexus commands are executed on node "<reader>"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'eta');
      """
    Then the last command output contains
      """
      exists
      partition: 0
      """
    And the last command output owner equals placeholder "relay_owner_0"
    When these Plexus commands are executed on node "<reader>"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'acme');
      """
    Then the last command output contains
      """
      exists
      partition: 3
      """
    And the last command output owner equals placeholder "relay_owner_3"

    Examples:
      | cluster_size | reader |
      | 1            | node-1 |
      | 3            | node-2 |

  Scenario Outline: MAX INSTANCES EVICT LRU and the branch TTL are enforced by each partition owner on its own keys
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( tenant STRING, seq I64 );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m PARTITIONS 2 MAX INSTANCES 1 EVICT LRU;

      CREATE BRANCH by_tenant_short
        SCHEMA tenant_branch TTL 3s PARTITIONS 2;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_tenant;

      CREATE RELAY short_lived
        SCHEMA notification
        BRANCHED BY by_tenant_short;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/ingest' TYPE HTTP;
      CREATE ENDPOINT short_ingress ON edge PATH '/short' TYPE HTTP;

      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE HTTP ENDPOINT INGESTOR http_short_lived
        FROM short_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO short_lived
        INHERIT ALL
        BRANCHED BY by_tenant_short
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","seq":1}
      """
    Then within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'eta');
      """
    And within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'acme');
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"theta","seq":2}
      """
    Then within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'theta');
      """
    And within "30s" node "<reader>" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'eta');
      """
    And within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'acme');
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","seq":3}
      """
    Then within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'beta');
      """
    And within "30s" node "<reader>" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'acme');
      """
    And within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'theta');
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/short"
      """
      {"tenant":"eta","seq":4}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/short"
      """
      {"tenant":"acme","seq":4}
      """
    Then within "60s" node "<reader>" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY short_lived WHERE (tenant = 'eta');
      """
    And within "60s" node "<reader>" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY short_lived WHERE (tenant = 'acme');
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/short"
      """
      {"tenant":"acme","seq":5}
      """
    Then within "30s" node "<reader>" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY short_lived WHERE (tenant = 'acme');
      """

    Examples:
      | cluster_size | reader |
      | 1            | node-1 |
      | 3            | node-3 |

  Scenario Outline: CAPACITY backpressure holds one partition back while another partition keeps flowing
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( tenant STRING, seq I64 );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m PARTITIONS 2;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_tenant
        CAPACITY 1;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT held_ingress ON edge PATH '/held' TYPE HTTP;
      CREATE ENDPOINT flowing_ingress ON edge PATH '/flowing' TYPE HTTP;

      CREATE HTTP ENDPOINT INGESTOR http_held
        FROM held_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE HTTP ENDPOINT INGESTOR http_flowing
        FROM flowing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    And the owner of every partition of relay "notifications" in the last cluster status is saved with placeholder prefix "relay_owner"
    And these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION capacity_subscription TO notifications;
      """
    And relay owner fan-out of partition 0 of relay "notifications" in domain "{{domain}}" is paused before dispatch
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/held"
      """
      {"tenant":"eta","seq":1}
      """
    Then the relay owner fan-out pause of partition 0 of relay "notifications" in domain "{{domain}}" is reached
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/held"
      """
      {"tenant":"eta","seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/held"
      """
      {"tenant":"eta","seq":3}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/flowing"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/flowing"
      """
      {"tenant":"acme","seq":2}
      """
    Then within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "tenant":"acme" | "seq":1
      "tenant":"acme" | "seq":2
      """
    And node "{{relay_owner_1}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="RELAY"
      target="notifications"
      direction="received"
      relay="notifications"
      partition="1"
      """
    When the relay owner fan-out pause of partition 0 of relay "notifications" in domain "{{domain}}" is released
    Then within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"eta" | "seq":2
      "tenant":"eta" | "seq":3
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: ALTER BRANCH SET PARTITIONS is refused while the domain runs and changes the count on a stopped domain
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( tenant STRING, seq I64 );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m PARTITIONS 4;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/ingest' TYPE HTTP;

      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    When these Plexus commands fail with "changes its partition count only while its domain is stopped"
      """
      ALTER BRANCH by_tenant SET PARTITIONS 2;
      """
    When these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 4;
      """
    When these Plexus commands are executed on the leader node
      """
      STOP;
      ALTER BRANCH by_tenant SET PARTITIONS 2;
      START;
      """
    And these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 2;
      """
    When these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of relay "notifications"
    When these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION altered_subscription TO notifications;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","seq":1}
      """
    Then within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"acme" | "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      STOP;
      ALTER BRANCH by_tenant SET PARTITIONS 1;
      START;
      """
    And these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      """

    Examples:
      | cluster_size | reader |
      | 1            | node-1 |
      | 3            | node-2 |
