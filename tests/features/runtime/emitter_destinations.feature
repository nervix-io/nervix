Feature: Explicit emitter record keys and destination partitions
  A key is evaluated before branch identity collapses and is carried as exact external bytes.

  @emitter_destinations
  Scenario Outline: Broker emitters publish the declared byte key on <broker> with <key_clause>
    Given <broker> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And <broker> topic "explicit_keys_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( tenant STRING, value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_fields ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_fields TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge explicit-keys-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE <connector> CLIENT destination CONFIG { <config> };
      CREATE <connector> TOPIC EMITTER keyed FROM events
        TO destination TOPIC 'explicit_keys_{{test_id}}'
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec <key_clause>
        <construction> BATCH MAX MESSAGES 4 MAX SIZE 64KiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "explicit-keys-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":7}
      """
    Then within "30s" the observed broker receives exactly these byte-keyed messages
      | payload                                     | key base64 |
      | [{"tenant":"<published_tenant>","value":7}] | <key>      |

    Examples:
      | broker | connector | config                                 | cluster_size | key_clause               | key      | construction | published_tenant |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 1            | KEY branch.tenant        | YWNtZQ== | INHERIT ALL  | acme             |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 1            | KEY hex_decode('00ff10') | AP8Q     | INHERIT ALL  | acme             |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 1            |                          | none     | INHERIT ALL  | acme             |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 3            | KEY branch.tenant        | YWNtZQ== | INHERIT ALL  | acme             |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 3            | KEY hex_decode('00ff10') | AP8Q     | INHERIT ALL  | acme             |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 3            |                          | none     | INHERIT ALL  | acme             |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 1            | KEY branch.tenant        | YWNtZQ== | INHERIT ALL  | acme             |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 1            | KEY hex_decode('00ff10') | AP8Q     | INHERIT ALL  | acme             |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 1            |                          | none     | INHERIT ALL  | acme             |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 3            | KEY branch.tenant        | YWNtZQ== | INHERIT ALL  | acme             |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 3            | KEY hex_decode('00ff10') | AP8Q     | INHERIT ALL  | acme             |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 3            |                          | none     | INHERIT ALL  | acme             |

      | Kafka  | KAFKA  | 'bootstrap.servers' = '{{kafka_addr}}' | 1 | KEY 'café'         | Y2Fmw6k= | INHERIT ALL                     | acme |
      | Kafka  | KAFKA  | 'bootstrap.servers' = '{{kafka_addr}}' | 1 | KEY ''             |          | INHERIT ALL                     | acme |
      | Kafka  | KAFKA  | 'bootstrap.servers' = '{{kafka_addr}}' | 3 | KEY 'café'         | Y2Fmw6k= | INHERIT ALL                     | acme |
      | Kafka  | KAFKA  | 'bootstrap.servers' = '{{kafka_addr}}' | 3 | KEY ''             |          | INHERIT ALL                     | acme |
      | Pulsar | PULSAR | 'addr' = '{{pulsar_addr}}'             | 1 | KEY 'café'         | Y2Fmw6k= | INHERIT ALL                     | acme |
      | Pulsar | PULSAR | 'addr' = '{{pulsar_addr}}'             | 1 | KEY ''             |          | INHERIT ALL                     | acme |
      | Pulsar | PULSAR | 'addr' = '{{pulsar_addr}}'             | 3 | KEY 'café'         | Y2Fmw6k= | INHERIT ALL                     | acme |
      | Pulsar | PULSAR | 'addr' = '{{pulsar_addr}}'             | 3 | KEY ''             |          | INHERIT ALL                     | acme |
      | Kafka  | KAFKA  | 'bootstrap.servers' = '{{kafka_addr}}' | 1 | KEY message.tenant | d2lyZQ== | INHERIT ALL SET tenant = 'wire' | wire |
      | Kafka  | KAFKA  | 'bootstrap.servers' = '{{kafka_addr}}' | 3 | KEY message.tenant | d2lyZQ== | INHERIT ALL SET tenant = 'wire' | wire |
      | Pulsar | PULSAR | 'addr' = '{{pulsar_addr}}'             | 1 | KEY message.tenant | d2lyZQ== | INHERIT ALL SET tenant = 'wire' | wire |
      | Pulsar | PULSAR | 'addr' = '{{pulsar_addr}}'             | 3 | KEY message.tenant | d2lyZQ== | INHERIT ALL SET tenant = 'wire' | wire |

  @emitter_destinations
  Scenario Outline: Different record keys keep separate payload batches on the same branch on <broker>
    Given <broker> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And <broker> topic "keyed_batches_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( tenant STRING, value I64, wire_key STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, wire_key string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_fields ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_fields TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge keyed-batches-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE <connector> CLIENT destination CONFIG { <config> };
      CREATE <connector> TOPIC EMITTER keyed FROM events
        TO destination TOPIC 'keyed_batches_{{test_id}}'
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec KEY message.wire_key
        INHERIT ALL BATCH MAX MESSAGES 4 MAX SIZE 64KiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "keyed-batches-{{test_id}}.example.com" path "/events"
      """
      [{"tenant":"acme","value":1,"wire_key":"one"},{"tenant":"acme","value":2,"wire_key":"two"}]
      """
    Then within "30s" the observed broker receives exactly these byte-keyed messages
      | payload                                        | key base64 |
      | [{"tenant":"acme","value":1,"wire_key":"one"}] | b25l       |
      | [{"tenant":"acme","value":2,"wire_key":"two"}] | dHdv       |

    Examples:
      | broker | connector | config                                 | cluster_size |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 1            |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 3            |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 1            |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 3            |

  @emitter_destinations
  Scenario Outline: A broker destination follows each branch partition modulo its own count on <broker>
    Given <broker> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And <broker> topic "partition_destination_{{test_id}}" exists with 3 partitions
    And <broker> topic "partition_destination_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( tenant STRING, value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_fields ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_fields TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge explicit-partitions-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE <connector> CLIENT destination CONFIG { <config> };
      CREATE <connector> TOPIC EMITTER partitioned FROM events
        TO destination TOPIC 'partition_destination_{{test_id}}'
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec <key_clause> PARTITION FROM BRANCH
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "explicit-partitions-{{test_id}}.example.com" path "/events"
      """
      [{"tenant":"eta","value":0},{"tenant":"beta","value":1},{"tenant":"lambda","value":2},{"tenant":"acme","value":3}]
      """
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                       | key base64 | partition |
      | {"tenant":"eta","value":0}    | <wire_key> | 0         |
      | {"tenant":"beta","value":1}   | <wire_key> | 1         |
      | {"tenant":"lambda","value":2} | <wire_key> | 2         |
      | {"tenant":"acme","value":3}   | <wire_key> | 0         |

    Examples:
      | broker | connector | config                                 | cluster_size | key_clause     | wire_key     |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 1            | KEY 'wire-key' | d2lyZS1rZXk= |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 3            | KEY 'wire-key' | d2lyZS1rZXk= |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 1            | KEY 'wire-key' | d2lyZS1rZXk= |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 3            | KEY 'wire-key' | d2lyZS1rZXk= |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 1            |                | none         |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 3            |                | none         |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 1            |                | none         |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 3            |                | none         |

  @emitter_destinations
  Scenario Outline: A Pulsar key routes records through the producer policy without a branch override
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "key_routing_{{test_id}}" exists with 3 partitions
    And Pulsar key "wire-key" routes among 3 partitions as "key_partition"
    And Pulsar topic "key_routing_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge key-routing-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT destination CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC EMITTER keyed FROM events
        TO destination TOPIC 'key_routing_{{test_id}}'
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec KEY 'wire-key'
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "key-routing-{{test_id}}.example.com" path "/events"
      """
      [{"value":0},{"value":1},{"value":2},{"value":3}]
      """
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload     | key base64   | partition         |
      | {"value":0} | d2lyZS1rZXk= | {{key_partition}} |
      | {"value":1} | d2lyZS1rZXk= | {{key_partition}} |
      | {"value":2} | d2lyZS1rZXk= | {{key_partition}} |
      | {"value":3} | d2lyZS1rZXk= | {{key_partition}} |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_destinations
  Scenario Outline: Each emitter input keeps its own branch schema and source partition on <broker>
    Given <broker> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And <broker> topic "source_destinations_{{test_id}}" exists with 2 partitions
    And <broker> topic "source_destinations_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( tenant STRING, value I64, zone I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, zone integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_fields ( tenant STRING );
      CREATE SCHEMA location_fields ( zone I64, tenant STRING );
      CREATE BRANCH tenant_source SCHEMA tenant_fields TTL 5m PARTITIONS 4;
      CREATE BRANCH location_source SCHEMA location_fields TTL 5m;
      CREATE RELAY first_events SCHEMA event BRANCHED BY tenant_source;
      CREATE RELAY second_events SCHEMA event BRANCHED BY location_source;
      CREATE VHOST edge source-destinations-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT first_intake ON edge PATH '/a';
      CREATE HTTP ENDPOINT second_intake ON edge PATH '/b';
      CREATE HTTP ENDPOINT INGESTOR first_source FROM first_intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO first_events INHERIT ALL BRANCHED BY tenant_source SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR second_source FROM second_intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO second_events INHERIT ALL BRANCHED BY location_source
        SET zone = message.zone, tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE <connector> CLIENT destination CONFIG { <config> };
      CREATE <connector> TOPIC EMITTER partitioned FROM first_events, second_events
        COLLECT FOR 20ms MAX BATCH SIZE 1MiB
        TO destination TOPIC 'source_destinations_{{test_id}}'
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec KEY branch.tenant PARTITION FROM BRANCH
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "source-destinations-{{test_id}}.example.com" path "/a"
      """
      {"tenant":"acme","value":1,"zone":7}
      """
    And http payload is posted to host "source-destinations-{{test_id}}.example.com" path "/b"
      """
      {"tenant":"beta","value":2,"zone":9}
      """
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                              | key base64 | partition |
      | {"tenant":"acme","value":1,"zone":7} | YWNtZQ==   | 1         |
      | {"tenant":"beta","value":2,"zone":9} | YmV0YQ==   | 0         |

    Examples:
      | broker | connector | config                                 | cluster_size |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 1            |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 3            |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 1            |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 3            |

  @emitter_destinations
  Scenario Outline: A key is checked for exact type nullability and sensitivity on <broker>
    Given <broker> is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming ( tenant STRING SENSITIVE, value I64, label STRING OPTIONAL );
      CREATE SCHEMA public_event ( value I64 );
      CREATE WIRE JSON SCHEMA public_wire MODE STRICT ( value integer );
      CREATE CODEC public_codec FROM WIRE JSON SCHEMA public_wire TO SCHEMA public_event;
      CREATE SCHEMA tenant_fields ( tenant STRING SENSITIVE );
      CREATE BRANCH by_tenant SCHEMA tenant_fields TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA incoming BRANCHED BY by_tenant;
      CREATE <connector> CLIENT destination CONFIG { <config> };
      """
    When these Plexus commands fail with "envelope KEY requires"
      """
      CREATE <connector> TOPIC EMITTER invalid_key FROM events
        TO destination TOPIC 'typed_keys_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING public_codec KEY input.value
        SET value = input.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "envelope KEY requires"
      """
      CREATE <connector> TOPIC EMITTER nullable_key FROM events
        TO destination TOPIC 'typed_keys_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING public_codec KEY input.label
        SET value = input.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "envelope KEY requires"
      """
      CREATE <connector> TOPIC EMITTER sensitive_key FROM events
        TO destination TOPIC 'typed_keys_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING public_codec KEY branch.tenant
        SET value = input.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed
      """
      CREATE <connector> TOPIC EMITTER keyed FROM events
        TO destination TOPIC 'typed_keys_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING public_codec KEY leak_sensitive(branch.tenant)
        SET value = input.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      ALTER <connector> TOPIC EMITTER keyed SET KEY hex_decode('00ff10');
      SHOW CREATE EMITTER keyed;
      """
    Then the last command output contains
      """
      KEY hex_decode('00ff10')
      """
    When these Plexus commands fail with "envelope KEY requires"
      """
      ALTER <connector> TOPIC EMITTER keyed SET KEY input.value;
      """
    When these Plexus commands are executed
      """
      SHOW CREATE EMITTER keyed;
      """
    Then the last command output contains
      """
      KEY hex_decode('00ff10')
      """
    When these Plexus commands are executed
      """
      ALTER <connector> TOPIC EMITTER keyed SET PARTITION FROM BRANCH;
      ALTER <connector> TOPIC EMITTER keyed DROP KEY;
      SHOW CREATE EMITTER keyed;
      """
    Then the last command output contains
      """
      PARTITION FROM BRANCH
      """
    When these Plexus commands are executed
      """
      ALTER <connector> TOPIC EMITTER keyed DROP PARTITION FROM BRANCH;
      CREATE <connector> TOPIC EMITTER plain FROM events
        TO destination TOPIC 'typed_keys_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING public_codec
        SET value = input.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | broker | connector | config                                 | cluster_size |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 1            |
      | Kafka  | KAFKA     | 'bootstrap.servers' = '{{kafka_addr}}' | 3            |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 1            |
      | Pulsar | PULSAR    | 'addr' = '{{pulsar_addr}}'             | 3            |

  @emitter_destinations
  Scenario Outline: An SQS FIFO group expression reads the exact branch field
    Given SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "branch_group_{{test_id}}.fifo" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( tenant STRING, value I64, zone I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, zone integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_fields ( tenant STRING );
      CREATE SCHEMA location_fields ( zone I64, tenant STRING );
      CREATE BRANCH tenant_source SCHEMA tenant_fields TTL 5m PARTITIONS 4;
      CREATE BRANCH location_source SCHEMA location_fields TTL 5m;
      CREATE RELAY first_events SCHEMA event BRANCHED BY tenant_source;
      CREATE RELAY second_events SCHEMA event BRANCHED BY location_source;
      CREATE VHOST edge explicit-group-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT first_intake ON edge PATH '/a';
      CREATE HTTP ENDPOINT second_intake ON edge PATH '/b';
      CREATE HTTP ENDPOINT INGESTOR first_source FROM first_intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO first_events INHERIT ALL BRANCHED BY tenant_source SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR second_source FROM second_intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO second_events INHERIT ALL BRANCHED BY location_source
        SET zone = message.zone, tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SQS CLIENT destination CONFIG {
        'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1',
        'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER grouped FROM first_events, second_events
        COLLECT FOR 20ms MAX BATCH SIZE 1MiB
        TO destination QUEUE 'branch_group_{{test_id}}.fifo' FIFO GROUP branch.tenant
        SEND BATCH MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "explicit-group-{{test_id}}.example.com" path "/a"
      """
      {"tenant":"acme","value":1,"zone":7}
      """
    And http payload is posted to host "explicit-group-{{test_id}}.example.com" path "/b"
      """
      {"tenant":"beta","value":2,"zone":9}
      """
    Then within "30s" the observed broker receives exactly these messages
      | payload                              | key | headers | group |
      | {"tenant":"acme","value":1,"zone":7} |     |         | acme  |
      | {"tenant":"beta","value":2,"zone":9} |     |         | beta  |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
