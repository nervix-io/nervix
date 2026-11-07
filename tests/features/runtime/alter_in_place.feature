Feature: Altering an ingestor or an emitter in place
  An ALTER changes one part of an ingestor or an emitter and keeps the rest of it as it was: the
  client, the locator, a declared option, the mode, the quiesce mode, an envelope clause, the
  construction of a route or of the emitter, and the general error policy. Each operation takes
  only what the entity declares, and the result is validated as the statement that writes it.

  @alter_ingestor_in_place
  Scenario Outline: ALTER INGESTOR changes each part of a KAFKA TOPIC ingestor in place
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA order_event ( seq I64, note STRING );
      CREATE WIRE JSON SCHEMA order_event_wire MODE STRICT ( seq integer, note string );
      CREATE WIRE JSON CODEC order_event_codec FROM WIRE SCHEMA order_event_wire TO SCHEMA order_event;
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE KAFKA CLIENT primary_broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA CLIENT audit_broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9093' };
      CREATE KAFKA TOPIC INGESTOR order_ingestor
        FROM primary_broker TOPIC 'Orders' OFFSET BY DOMAIN
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING order_event_codec
        TO orders INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      ALTER KAFKA TOPIC INGESTOR order_ingestor
        SET CLIENT audit_broker,
        SET TOPIC 'Orders.Audit',
        SET OFFSET BY CONSUMER GROUP 'auditors' START AT LATEST,
        SET START AT EARLIEST,
        SET MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s,
        SET QUIESCE SUSPEND,
        ALTER ROUTE TO orders DROP INHERIT,
        ALTER ROUTE TO orders ADD SET seq = input.seq + 1,
        ALTER ROUTE TO orders ADD SET note = 'audited',
        ALTER ROUTE TO orders ADD SET note = input.note,
        ALTER ROUTE TO orders DROP SET seq,
        ALTER ROUTE TO orders ADD SET seq = input.seq,
        ALTER ROUTE TO orders SET WHERE input.seq > 0,
        SET GENERAL ERROR IGNORE;
      SHOW CREATE INGESTOR order_ingestor;
      """
    Then the last command output contains
      """
      CREATE KAFKA TOPIC INGESTOR order_ingestor
        FROM audit_broker TOPIC 'Orders.Audit' OFFSET BY CONSUMER GROUP 'auditors' START AT EARLIEST
      """
    And the last command output contains
      """
      MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE SUSPEND
      """
    And the last command output contains
      """
      TO orders
          SET note = 'audited',
              note = input.note,
              seq = input.seq
          WHERE input.seq > 0
          UNBRANCHED
      """
    And the last command output contains
      """
        ON GENERAL ERROR IGNORE;
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER KAFKA TOPIC INGESTOR order_ingestor
        SET OFFSET BY DOMAIN,
        ALTER ROUTE TO orders DROP WHERE,
        ALTER ROUTE TO orders SET INHERIT ALL,
        ALTER ROUTE TO orders DROP SET seq;
      """
    And these Plexus commands fail with "found BUFFER"
      """
      ALTER KAFKA TOPIC INGESTOR order_ingestor
        SET QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST;
      """
    And these Plexus commands fail with "the construction writes `note` more than once, so DROP SET cannot tell which"
      """
      ALTER KAFKA TOPIC INGESTOR order_ingestor ALTER ROUTE TO orders DROP SET note;
      """
    And these Plexus commands fail with "route target `audit` is not configured"
      """
      ALTER KAFKA TOPIC INGESTOR order_ingestor ALTER ROUTE TO audit SET INHERIT ALL;
      """
    And these Plexus commands fail with "INVOKE is only available to emitters"
      """
      ALTER KAFKA TOPIC INGESTOR order_ingestor
        ALTER ROUTE TO orders ADD INVOKE write_header('X-Audit', 'altered');
      """
    And these Plexus commands fail with "the construction makes 0 invocations, so it has no invocation 1"
      """
      ALTER KAFKA TOPIC INGESTOR order_ingestor ALTER ROUTE TO orders DROP INVOKE 1;
      """
    And these Plexus commands fail with "cannot set START AT on a KAFKA ingestor whose domain owns its offsets"
      """
      ALTER KAFKA TOPIC INGESTOR order_ingestor SET START AT LATEST;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR order_ingestor;
      """
    Then the last command output contains
      """
      FROM audit_broker TOPIC 'Orders.Audit' OFFSET BY DOMAIN MODE ACK SEQUENTIAL
      """
    And the last command output contains
      """
      TO orders
          INHERIT ALL
          SET note = 'audited',
              note = input.note
          UNBRANCHED
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_ingestor_in_place
  Scenario Outline: A SET MODE the ingestor's entity does not declare is refused where it is written
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event ( seq I64 );
      CREATE WIRE JSON SCHEMA order_event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC order_event_codec FROM WIRE SCHEMA order_event_wire TO SCHEMA order_event;
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:4222' };
      CREATE NATS SUBJECT INGESTOR order_ingestor
        FROM nats_main SUBJECT 'orders.created' QUEUE GROUP 'workers' INSTANCES 1
        MODE NO ACK ON QUIESCE DROP DECODE USING order_event_codec
        TO orders INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "found ACK"
      """
      ALTER NATS SUBJECT INGESTOR order_ingestor
        SET MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s;
      """
    And these Plexus commands are executed on the leader node
      """
      ALTER NATS SUBJECT INGESTOR order_ingestor
        SET SUBJECT 'orders.audited',
        SET QUEUE GROUP 'auditors',
        SET QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST;
      SHOW CREATE INGESTOR order_ingestor;
      """
    Then the last command output contains
      """
      FROM nats_main SUBJECT 'orders.audited' QUEUE GROUP 'auditors' INSTANCES 1
      """
    And the last command output contains
      """
      ON QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_emitter_in_place
  Scenario Outline: ALTER EMITTER changes each part of an SQS QUEUE emitter in place
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, value STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:1',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret',
        'timeout_ms' = '5000'
      };
      CREATE SQS CLIENT sqs_audit CONFIG {
        'endpoint' = 'http://127.0.0.1:2',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret',
        'timeout_ms' = '5000'
      };
      CREATE SQS QUEUE EMITTER publishing FROM events
        TO sqs_main QUEUE 'orders' SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      ALTER SQS QUEUE EMITTER publishing
        SET CLIENT sqs_audit,
        SET QUEUE 'orders.fifo',
        SET FIFO GROUP input.tenant,
        SET SEND BATCH,
        SET MODE ACK RETRY POLICY BACKOFF 1s MAX 1m,
        SET DEDUPLICATION ID input.value,
        DROP INHERIT,
        ADD SET tenant = lower(input.tenant),
        ADD SET value = input.value,
        ADD INVOKE write_header('tenant', input.tenant),
        SET WHERE input.value != '',
        SET GENERAL ERROR IGNORE;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TO sqs_audit QUEUE 'orders.fifo' FIFO GROUP input.tenant SEND BATCH
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1m
          ENCODE USING event_codec
          DEDUPLICATION ID input.value
        SET tenant = lower(input.tenant),
            value = input.value
        WHERE input.value != ''
        INVOKE write_header('tenant', input.tenant)
      """
    And the last command output contains
      """
        ON GENERAL ERROR IGNORE;
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER SQS QUEUE EMITTER publishing
        SET QUEUE 'orders',
        DROP FIFO GROUP,
        DROP DEDUPLICATION ID,
        SET DELAY 5s,
        DROP SET tenant,
        DROP SET value,
        SET INHERIT ALL,
        DROP INVOKE 1,
        DROP WHERE;
      """
    And these Plexus commands fail with "the stored SQS QUEUE emitter writes no FIFO GROUP, so there is no FIFO GROUP to drop"
      """
      ALTER SQS QUEUE EMITTER publishing DROP FIFO GROUP;
      """
    And these Plexus commands fail with "the construction makes 0 invocations, so it has no invocation 1"
      """
      ALTER SQS QUEUE EMITTER publishing DROP INVOKE 1;
      """
    And these Plexus commands fail with "found NO"
      """
      ALTER SQS QUEUE EMITTER publishing SET MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s;
      """
    And these Plexus commands fail with "found PROPERTIES"
      """
      ALTER SQS QUEUE EMITTER publishing SET PROPERTIES {'content_type' = 'application/json'};
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TO sqs_audit QUEUE 'orders' SEND BATCH
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1m
          ENCODE USING event_codec
          DELAY 5s
        INHERIT ALL
        FLUSH IMMEDIATE
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_emitter_in_place
  Scenario Outline: SET TO takes an emitter's complete body and SHOW CREATE renders it back
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, value STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON CODEC audit_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:1',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret',
        'timeout_ms' = '5000'
      };
      CREATE SQS QUEUE EMITTER publishing FROM events
        TO sqs_main QUEUE 'orders' SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      ALTER SQS QUEUE EMITTER publishing
        SET TO sqs_main QUEUE 'orders.fifo' FIFO GROUP input.tenant SEND BATCH
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1m
          ENCODE USING audit_codec
          DEDUPLICATION ID input.value
          SET tenant = input.tenant, value = input.value
          INVOKE write_header('tenant', input.tenant);
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TO sqs_main QUEUE 'orders.fifo' FIFO GROUP input.tenant SEND BATCH
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1m
          ENCODE USING audit_codec
          DEDUPLICATION ID input.value
        SET tenant = input.tenant,
            value = input.value
        INVOKE write_header('tenant', input.tenant)
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_in_place_entity_pause
  Scenario Outline: An in-place construction change pauses only its entity and the next records carry it
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA altered_event ( seq I64 );
      CREATE WIRE JSON SCHEMA altered_event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC altered_event_codec
        FROM WIRE SCHEMA altered_event_wire TO SCHEMA altered_event;
      CREATE RELAY altered_events SCHEMA altered_event UNBRANCHED;
      CREATE VHOST altered_edge http-{{test_id}}-alter-in-place.example.com;
      CREATE ENDPOINT altered_ingress ON altered_edge PATH '/alter-in-place' TYPE HTTP;
      CREATE ZEROMQ CLIENT altered_sink CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE HTTP ENDPOINT INGESTOR altered_ingestor
        FROM altered_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING altered_event_codec
        TO altered_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER altered_out FROM altered_events
        TO altered_sink MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING altered_event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-alter-in-place.example.com" path "/alter-in-place"
      """
      {"seq":1}
      """
    Then the observed broker receives a payload
      """
      "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER HTTP ENDPOINT INGESTOR altered_ingestor
        ALTER ROUTE TO altered_events DROP INHERIT,
        ALTER ROUTE TO altered_events ADD SET seq = input.seq + 100;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-in-place.example.com" path "/alter-in-place"
      """
      {"seq":2}
      """
    Then the observed broker receives a payload
      """
      "seq":102
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER altered_out
        DROP INHERIT,
        ADD SET seq = input.seq + 1000;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-in-place.example.com" path "/alter-in-place"
      """
      {"seq":3}
      """
    Then the observed broker receives a payload
      """
      "seq":1103
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
