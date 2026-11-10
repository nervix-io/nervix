@emitter_instances
Feature: Emitter instances follow the partitions they serve
  An emitter declared with INSTANCES runs one placement unit, task, buffer, cadence, retry
  schedule and sink per instance. Instance i serves the input partitions p with p mod n = i, so
  one key lives in one partition and one instance, and each key keeps its order end to end.

  Background:
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured

  Scenario Outline: A PER PARTITION Kafka emitter runs one instance with each partition on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instances_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "instances_out_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge instances-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER events_out FROM events INSTANCES PER PARTITION
        TO broker TOPIC 'instances_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of emitter "events_out" on <owners> distinct owners
    And the last cluster status colocates every instance of emitter "events_out" with the corresponding partition of relay "events"
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    And every scheduled instance of emitter "events_out" exposes its instance label in Prometheus
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER events_out;
      """
    Then the last command output contains
      """
      FROM events
        INSTANCES PER PARTITION
        ATTACHED
      """
    When http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":3}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":3}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
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
    And the observed broker does not receive a payload within "500ms"

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |

  Scenario Outline: INSTANCES 2 serves alternate partitions and ALTER SET INSTANCES replaces the instances on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instances_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "instances_out_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge instances-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER events_out FROM events INSTANCES 2
        TO broker TOPIC 'instances_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 instances of emitter "events_out" on <two_owners> distinct owners
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    And within "30s" instance 0 of emitter "events_out" serves partitions "0,2"
    And within "30s" instance 1 of emitter "events_out" serves partitions "1,3"
    When http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"eta" | "seq":2
      "tenant":"acme" | "seq":1
      "tenant":"acme" | "seq":2
      "tenant":"beta" | "seq":1
      "tenant":"lambda" | "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER KAFKA TOPIC EMITTER events_out SET INSTANCES PER PARTITION;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of emitter "events_out" on <four_owners> distinct owners
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER events_out;
      """
    Then the last command output contains
      """
      INSTANCES PER PARTITION
      """
    When http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":3}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":3}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":2}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":3
      "tenant":"acme" | "seq":3
      "tenant":"beta" | "seq":2
      "tenant":"lambda" | "seq":2
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER KAFKA TOPIC EMITTER events_out SET INSTANCES 6;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 6 instances of emitter "events_out" on <six_owners> distinct owners
    And within "30s" instance 4 of emitter "events_out" reports idle on its scheduled owner
    And within "30s" instance 5 of emitter "events_out" reports idle on its scheduled owner
    And within "30s" instance 3 of emitter "events_out" serves partitions "3"

    Examples:
      | nodes | two_owners | four_owners | six_owners |
      | 1     | 1          | 1           | 1          |
      | 3     | 2          | 3           | 3          |

  Scenario: Drain of one instance owner moves only its instances and keeps every key in order
    Given a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instances_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "instances_out_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge instances-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER events_out FROM events INSTANCES PER PARTITION
        TO broker TOPIC 'instances_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of emitter "events_out" on 3 distinct owners
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    And the current leader node is saved as placeholder "leader"
    And the owner of every instance of emitter "events_out" is saved with placeholder prefix "owner_before"
    And an instance owner of emitter "events_out" other than placeholder "leader" is saved as placeholder "affected_owner"
    When these Plexus commands are executed through the client on node "{{leader}}"
      """
      DRAIN NODE {{affected_owner}};
      """
    Then within "90s" node "{{leader}}" eventually reports emitter "events_out" instances moved from node "{{affected_owner}}" and all other owners unchanged from placeholder prefix "owner_before"
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    When http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2}
      """
    And http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"eta" | "seq":2
      "tenant":"acme" | "seq":1
      "tenant":"acme" | "seq":2
      "tenant":"beta" | "seq":1
      "tenant":"lambda" | "seq":1
      """

  Scenario: Failover of one instance owner moves only its instances and keeps every key in order
    Given a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instances_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "instances_out_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge instances-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER events_out FROM events INSTANCES 2
        TO broker TOPIC 'instances_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 instances of emitter "events_out" on 2 distinct owners
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    And the current leader node is saved as placeholder "leader"
    And the owner of every instance of emitter "events_out" is saved with placeholder prefix "owner_before"
    And an instance owner of emitter "events_out" other than placeholder "leader" is saved as placeholder "affected_owner"
    When node "{{affected_owner}}" is stopped
    Then within "90s" node "{{leader}}" eventually reports emitter "events_out" instances moved from node "{{affected_owner}}" and all other owners unchanged from placeholder prefix "owner_before"
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    When http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"acme" | "seq":1
      "tenant":"beta" | "seq":1
      "tenant":"lambda" | "seq":1
      """

  Scenario Outline: A fiber REQUIRE COLOCATION rule keeps every instance with the partitions it serves on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER events_out FROM events INSTANCES 2
        TO broker TOPIC 'instances_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE PLACEMENT emit_local FROM events TO events_out REQUIRE COLOCATION;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 instances of emitter "events_out" on <owners> distinct owners
    And the last cluster status colocates every instance of emitter "events_out" with the partitions it serves of relay "events"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT emit_local;
      """
    Then the last command output contains
      """
      class=fiber
      """

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 2      |

  Scenario Outline: An emitter that publishes through one writer refuses INSTANCES with its reason on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      """
    When these Plexus statements fail with "<reason>"
      """
      CREATE <entity> EMITTER single_out FROM events INSTANCES <instances>
        TO <destination>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | entity          | instances     | destination                                                                                                                                                                   | reason                                                                                 |
      | 1     | CLIENT CONSUMER | 2             | SCHEMA event MODE ACK ACK TIMEOUT 30s INHERIT ALL                                                                                                                             | CLIENT CONSUMER emitters deliver through one consumer stream, so they refuse INSTANCES |
      | 3     | CLIENT CONSUMER | PER PARTITION | SCHEMA event MODE ACK ACK TIMEOUT 30s INHERIT ALL                                                                                                                             | CLIENT CONSUMER emitters deliver through one consumer stream, so they refuse INSTANCES |
      | 1     | ICEBERG TABLE   | 2             | lake NAMESPACE 'analytics' TABLE 'events' LOCATION 's3://lake/events' CATALOG 'http://127.0.0.1:8181' COMMIT EACH 1s MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s | ICEBERG TABLE emitters commit through one table writer, so they refuse INSTANCES       |
      | 3     | ICEBERG TABLE   | PER PARTITION | lake NAMESPACE 'analytics' TABLE 'events' LOCATION 's3://lake/events' CATALOG 'http://127.0.0.1:8181' COMMIT EACH 1s MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s | ICEBERG TABLE emitters commit through one table writer, so they refuse INSTANCES       |
      | 1     | MQTT TOPIC      | 2             | broker TOPIC 'events' QOS 1 MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec INHERIT ALL                                                                | MQTT TOPIC emitters publish through one client identifier, so they refuse INSTANCES    |
      | 3     | MQTT TOPIC      | PER PARTITION | broker TOPIC 'events' QOS 1 MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec INHERIT ALL                                                                | MQTT TOPIC emitters publish through one client identifier, so they refuse INSTANCES    |

  Scenario Outline: A PER PARTITION emitter follows a live partition count change on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instances_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "instances_out_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge instances-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER events_out FROM events INSTANCES PER PARTITION
        TO broker TOPIC 'instances_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of emitter "events_out" on <four_owners> distinct owners
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    When http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"acme" | "seq":1
      "tenant":"beta" | "seq":1
      "tenant":"lambda" | "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH tenants SET PARTITIONS 6;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 6 instances of emitter "events_out" on <six_owners> distinct owners
    And within "60s" every instance of emitter "events_out" reports running on its scheduled owner
    And within "30s" instance 5 of emitter "events_out" serves partitions "5"
    When http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":2}
      """
    And http payload is posted to node "node-1" with host "instances-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":3}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":2
      "tenant":"eta" | "seq":3
      "tenant":"acme" | "seq":2
      "tenant":"beta" | "seq":2
      "tenant":"lambda" | "seq":2
      """
    And the observed broker does not receive a payload within "500ms"

    Examples:
      | nodes | four_owners | six_owners |
      | 1     | 1           | 1          |
      | 3     | 3           | 3          |
