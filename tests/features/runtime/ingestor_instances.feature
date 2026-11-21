@ingestor_instances
Feature: Ingestor instances are independent placement units
  A declared instance keeps its index when the scheduler places it on a node. Each instance has
  one owner, and competing consumers can use the capacity of every eligible node.

  @ingestor_instance_lifecycle
  Scenario Outline: Kafka competing consumers are scheduled as distinct instances on <nodes> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instance_input_{{test_id}}" exists with 3 partitions
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}'
        };
      CREATE KAFKA TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'instance_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'instance_group_{{test_id}}' START AT EARLIEST INSTANCES 3
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And Kafka consumer group "instance_group_{{test_id}}" eventually has 3 consumers
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    When Kafka message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"alpha","seq":1}
      """
    And Kafka message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |

  @ingestor_instance_lifecycle
  Scenario Outline: MQTT competing consumers are scheduled as distinct instances on <nodes> nodes
    Given MQTT is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE MQTT CLIENT broker
        CONFIG {
          'addr' = '{{mqtt_addr}}'
        };
      CREATE MQTT TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'instance_input_{{test_id}}' INSTANCES 3 SESSION CLEAN QOS 0
        MODE NO ACK
        ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "spread_ingestor" on the leader node contains
      """
      status: running
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    When MQTT message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"alpha","seq":1}
      """
    And MQTT message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |

  @ingestor_instance_lifecycle
  Scenario Outline: RabbitMQ competing consumers are scheduled as distinct instances on <nodes> nodes
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "instance_input_{{test_id}}" exists
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RABBITMQ CLIENT broker
        CONFIG {
          'addr' = '{{rabbitmq_addr}}'
        };
      CREATE RABBITMQ QUEUE INGESTOR spread_ingestor
        FROM broker QUEUE 'instance_input_{{test_id}}' INSTANCES 3
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "spread_ingestor" on the leader node contains
      """
      status: running
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And RabbitMQ queue "instance_input_{{test_id}}" eventually has 3 consumers
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    When RabbitMQ message is published to queue "instance_input_{{test_id}}"
      """
      {"tenant":"alpha","seq":1}
      """
    And RabbitMQ message is published to queue "instance_input_{{test_id}}"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |

  @ingestor_instance_lifecycle
  Scenario Outline: SQS competing consumers are scheduled as distinct instances on <nodes> nodes
    Given SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "instance_input_{{test_id}}" exists
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE SQS CLIENT broker
        CONFIG {
          'endpoint' = '{{sqs_endpoint}}',
          'region' = 'us-east-1',
          'access_key_id' = 'scenario-key',
          'secret_access_key' = 'scenario-secret'
        };
      CREATE SQS QUEUE INGESTOR spread_ingestor
        FROM broker QUEUE 'instance_input_{{test_id}}' INSTANCES 3
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "spread_ingestor" on the leader node contains
      """
      status: running
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    When SQS message is published to queue "instance_input_{{test_id}}"
      """
      {"tenant":"alpha","seq":1}
      """
    And SQS message is published to queue "instance_input_{{test_id}}"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |

  @ingestor_instance_lifecycle
  Scenario Outline: NATS competing consumers are scheduled as distinct instances on <nodes> nodes
    Given NATS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE NATS CLIENT broker
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
      CREATE NATS SUBJECT INGESTOR spread_ingestor
        FROM broker SUBJECT 'instance_input_{{test_id}}' QUEUE GROUP 'instance_group_{{test_id}}' INSTANCES 3
        MODE NO ACK
        ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "spread_ingestor" on the leader node contains
      """
      status: running
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    When 2 sequential NATS messages are published to subject "instance_input_{{test_id}}"
      """
      {"tenant":"tenant{{sequence}}","seq":{{sequence}}}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"tenant1" | "seq":1
      "tenant":"tenant2" | "seq":2
      """

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |

  @ingestor_instance_lifecycle
  Scenario Outline: Pulsar competing consumers are scheduled as distinct instances on <nodes> nodes
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar subscription "instance_group_{{test_id}}" of topic "instance_input_{{test_id}}" is provisioned
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE PULSAR CLIENT broker
        CONFIG {
          'addr' = '{{pulsar_addr}}'
        };
      CREATE PULSAR TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'instance_input_{{test_id}}' SUBSCRIPTION 'instance_group_{{test_id}}' PROVISIONED TYPE SHARED INSTANCES 3
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "spread_ingestor" on the leader node contains
      """
      status: running
      """
    And within "60s" Pulsar subscription "instance_group_{{test_id}}" of topic "instance_input_{{test_id}}" has 3 consumers
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    When Pulsar message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"alpha","seq":1}
      """
    And Pulsar message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |


  @ingestor_instance_lifecycle
  Scenario Outline: Kafka domain-offset instances follow their constructed partitions on <nodes> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instance_input_{{test_id}}" exists with 4 partitions
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}'
        };
      CREATE KAFKA TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'instance_input_{{test_id}}'
        OFFSET BY DOMAIN INSTANCES PER TRANSPORT PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "spread_ingestor" on the leader node contains
      """
      status: running
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    And the last cluster status colocates every instance of ingestor "spread_ingestor" with the corresponding partition of relay "events"
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    When Kafka message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"alpha","seq":1}
      """
    And Kafka message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |

  @ingestor_instance_lifecycle
  Scenario: Drain of one instance owner preserves every other instance owner
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instance_input_{{test_id}}" exists with 3 partitions
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}'
        };
      CREATE KAFKA TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'instance_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'instance_group_{{test_id}}' START AT EARLIEST INSTANCES 3
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_ingestor" on 3 distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And Kafka consumer group "instance_group_{{test_id}}" eventually has 3 consumers
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    And the current leader node is saved as placeholder "leader"
    And the owner of every instance of ingestor "spread_ingestor" is saved with placeholder prefix "owner_before"
    And an instance owner of ingestor "spread_ingestor" other than placeholder "leader" is saved as placeholder "affected_owner"
    When these Plexus commands are executed through the client on node "{{leader}}"
      """
      DRAIN NODE {{affected_owner}};
      """
    Then within "90s" node "{{leader}}" eventually reports ingestor "spread_ingestor" instances moved from node "{{affected_owner}}" and all other owners unchanged from placeholder prefix "owner_before"
    And Kafka consumer group "instance_group_{{test_id}}" eventually has 3 consumers
    When these Plexus commands are executed on node "{{leader}}"
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    And Kafka message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"alpha","seq":1}
      """
    And Kafka message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

  @ingestor_instance_lifecycle
  Scenario: Failover of one instance owner preserves every other instance owner
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "instance_input_{{test_id}}" exists with 3 partitions
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}'
        };
      CREATE KAFKA TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'instance_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'instance_group_{{test_id}}' START AT EARLIEST INSTANCES 3
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_ingestor" on 3 distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And Kafka consumer group "instance_group_{{test_id}}" eventually has 3 consumers
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    And the current leader node is saved as placeholder "leader"
    And the owner of every instance of ingestor "spread_ingestor" is saved with placeholder prefix "owner_before"
    And an instance owner of ingestor "spread_ingestor" other than placeholder "leader" is saved as placeholder "affected_owner"
    When node "{{affected_owner}}" is stopped
    Then within "90s" node "{{leader}}" eventually reports ingestor "spread_ingestor" instances moved from node "{{affected_owner}}" and all other owners unchanged from placeholder prefix "owner_before"
    And Kafka consumer group "instance_group_{{test_id}}" eventually has 3 consumers
    When these Plexus commands are executed on node "{{leader}}"
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    And Kafka message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"alpha","seq":1}
      """
    And Kafka message is published to topic "instance_input_{{test_id}}"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

  Scenario Outline: Entities that run one source refuse an INSTANCES clause
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "found INSTANCES"
      """
      CREATE <entity> INGESTOR single_source
        <input> INSTANCES 2 MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | entity                | input                             |
      | 1     | HTTP REQUEST          | FROM broker METHOD 'GET' EVERY 1s |
      | 3     | HTTP REQUEST          | FROM broker METHOD 'GET' EVERY 1s |
      | 1     | HTTP ENDPOINT         | FROM ingress                      |
      | 3     | HTTP ENDPOINT         | FROM ingress                      |
      | 1     | WEBSOCKETS ENDPOINT   | FROM ingress                      |
      | 3     | WEBSOCKETS ENDPOINT   | FROM ingress                      |
      | 1     | WEBSOCKETS CONNECTION | FROM broker                       |
      | 3     | WEBSOCKETS CONNECTION | FROM broker                       |
      | 1     | REDIS PUBSUB          | FROM broker CHANNEL 'events'      |
      | 3     | REDIS PUBSUB          | FROM broker CHANNEL 'events'      |
      | 1     | PROMETHEUS QUERY      | FROM broker QUERY 'up' EVERY 1s   |
      | 3     | PROMETHEUS QUERY      | FROM broker QUERY 'up' EVERY 1s   |
      | 1     | ZEROMQ PULL           | FROM broker                       |
      | 3     | ZEROMQ PULL           | FROM broker                       |
      | 1     | SYSLOG LISTENER       | FROM broker                       |
      | 3     | SYSLOG LISTENER       | FROM broker                       |
      | 1     | CLIENT PRODUCER       | FROM SCHEMA event                 |
      | 3     | CLIENT PRODUCER       | FROM SCHEMA event                 |

  Scenario Outline: Pulsar single-consumer types refuse a fixed count above one
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE PULSAR CLIENT broker CONFIG { 'addr' = 'pulsar://127.0.0.1:6650' };
      """
    When these Plexus statements fail with "reads through one consumer, so it refuses INSTANCES"
      """
      CREATE PULSAR TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'events' SUBSCRIPTION 'readers' PROVISIONED
        TYPE <kind> INSTANCES 2 MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | kind      |
      | FAILOVER  |
      | EXCLUSIVE |

  Scenario Outline: Transport-partition readers require one constructing branch contract
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE BRANCH by_other SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY other_events SCHEMA event BRANCHED BY by_other;
      CREATE RELAY plain_events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      """
    When these Plexus statements fail with "INSTANCES PER TRANSPORT PARTITION requires constructing routes to one branch"
      """
      CREATE KAFKA TOPIC INGESTOR bad_ingestor FROM broker TOPIC 'events'
        OFFSET BY DOMAIN INSTANCES PER TRANSPORT PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        <routes> FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | routes                                                                                                                                                                                    |
      | 1     | TO plain_events INHERIT ALL UNBRANCHED                                                                                                                                                    |
      | 3     | TO plain_events INHERIT ALL UNBRANCHED                                                                                                                                                    |
      | 1     | TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant FLUSH IMMEDIATE ON MESSAGE ERROR LOG TO other_events INHERIT ALL BRANCHED BY by_other SET tenant = message.tenant |
      | 3     | TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant FLUSH IMMEDIATE ON MESSAGE ERROR LOG TO other_events INHERIT ALL BRANCHED BY by_other SET tenant = message.tenant |

  Scenario Outline: Kafka transport-partition readers require domain offsets
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      """
    When these Plexus statements fail with "KAFKA TOPIC INSTANCES PER TRANSPORT PARTITION requires OFFSET BY DOMAIN"
      """
      CREATE KAFKA TOPIC INGESTOR bad_ingestor FROM broker TOPIC 'events'
        OFFSET BY CONSUMER GROUP 'readers' START AT EARLIEST INSTANCES PER TRANSPORT PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: Transports without partitions accept only a fixed instance count
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE <connector> CLIENT broker CONFIG { <config> };
      """
    When these Plexus statements fail with "invalid integer 'PER'"
      """
      CREATE <entity> INGESTOR spread_ingestor
        FROM broker <source> INSTANCES PER TRANSPORT PARTITION <after>
        <mode> DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | entity         | connector | config                                                                                                                               | source                                 | after               | mode                                                                                     |
      | 1     | NATS SUBJECT   | NATS      | 'addr' = 'nats://127.0.0.1:4222'                                                                                                     | SUBJECT 'events' QUEUE GROUP 'readers' |                     | MODE NO ACK ON QUIESCE DROP                                                              |
      | 3     | NATS SUBJECT   | NATS      | 'addr' = 'nats://127.0.0.1:4222'                                                                                                     | SUBJECT 'events' QUEUE GROUP 'readers' |                     | MODE NO ACK ON QUIESCE DROP                                                              |
      | 1     | RABBITMQ QUEUE | RABBITMQ  | 'addr' = 'amqp://127.0.0.1:5672'                                                                                                     | QUEUE 'events'                         |                     | MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s ON QUIESCE SUSPEND |
      | 3     | RABBITMQ QUEUE | RABBITMQ  | 'addr' = 'amqp://127.0.0.1:5672'                                                                                                     | QUEUE 'events'                         |                     | MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s ON QUIESCE SUSPEND |
      | 1     | SQS QUEUE      | SQS       | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'us-east-1', 'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret' | QUEUE 'events'                         |                     | MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s ON QUIESCE SUSPEND |
      | 3     | SQS QUEUE      | SQS       | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'us-east-1', 'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret' | QUEUE 'events'                         |                     | MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s ON QUIESCE SUSPEND |
      | 1     | MQTT TOPIC     | MQTT      | 'addr' = 'mqtt://127.0.0.1:1883'                                                                                                     | TOPIC 'events'                         | SESSION CLEAN QOS 0 | MODE NO ACK ON QUIESCE DROP                                                              |
      | 3     | MQTT TOPIC     | MQTT      | 'addr' = 'mqtt://127.0.0.1:1883'                                                                                                     | TOPIC 'events'                         | SESSION CLEAN QOS 0 | MODE NO ACK ON QUIESCE DROP                                                              |

  @ingestor_instance_lifecycle
  Scenario Outline: Pulsar <subscription_type> readers deal the partition topics of a hashed branch on <nodes> nodes
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And Pulsar topic "dealt_input_{{test_id}}" exists with 4 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE PULSAR CLIENT broker
        CONFIG {
          'addr' = '{{pulsar_addr}}'
        };
      CREATE PULSAR TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'dealt_input_{{test_id}}' SUBSCRIPTION 'dealt_group_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST
        TYPE <subscription_type> INSTANCES PER TRANSPORT PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    And within "60s" Pulsar subscription "dealt_group_{{test_id}}" of topic "dealt_input_{{test_id}}-partition-0" has 1 consumer
    And within "60s" Pulsar subscription "dealt_group_{{test_id}}" of topic "dealt_input_{{test_id}}-partition-1" has 1 consumer
    And within "60s" Pulsar subscription "dealt_group_{{test_id}}" of topic "dealt_input_{{test_id}}-partition-2" has 1 consumer
    And within "60s" Pulsar subscription "dealt_group_{{test_id}}" of topic "dealt_input_{{test_id}}-partition-3" has 1 consumer
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And the last cluster status colocates every instance of ingestor "spread_ingestor" with the corresponding partition of relay "events"
    And every scheduled instance of ingestor "spread_ingestor" exposes its instance label in Prometheus
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observing TO events;
      """
    When Pulsar message is published to topic "dealt_input_{{test_id}}-partition-3"
      """
      {"tenant":"alpha","seq":1}
      """
    And Pulsar message is published to topic "dealt_input_{{test_id}}-partition-0"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"alpha" | "seq":1
      "tenant":"beta" | "seq":2
      """

    Examples:
      | nodes | subscription_type | owners |
      | 1     | SHARED            | 1      |
      | 3     | SHARED            | 3      |
      | 1     | EXCLUSIVE         | 1      |
      | 3     | EXCLUSIVE         | 3      |
