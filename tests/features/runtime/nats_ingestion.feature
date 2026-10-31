Feature: NATS ingestion
  Scenario Outline: NATS ingestor delivers JSON payloads to a subscribed relay
    Given NATS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_nats_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_nats_notifications;
        CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
        CREATE NATS SUBJECT INGESTOR nats_notifications
        FROM nats_main SUBJECT 'notifications_{{test_id}}' QUEUE GROUP 'nats_notifications_group_{{test_id}}' INSTANCES <instances> MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_nats_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And NATS message is published to subject "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    And NATS message is published to subject "notifications_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "10s" the relay subscription receives payloads
      """
      "user_id":42
      "user_id":43
      """

    Examples:
      | cluster_size | instances | replica_count |
      | 1            | 1         | 0             |
      | 1            | 2         | 0             |
      | 3            | 2         | 1             |

  Scenario Outline: NATS ingestor reports transient source failures and recovers
    Given NATS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "nats_notifications" enters fault mode
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_nats_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_nats_notifications;
        CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
        CREATE NATS SUBJECT INGESTOR nats_notifications
        FROM nats_main SUBJECT 'notifications_reconnect_{{test_id}}' QUEUE GROUP 'nats_notifications_reconnect_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_nats_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "nats_notifications" leaves fault mode
    Then within "10s" repeatedly publishing NATS message to subject "notifications_reconnect_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":43}
      """
    And the last relay subscription payload contains key fragment '{"user_id":43}'
    And within "5s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # The ingestor reads through an endpoint the scenario controls, so the NATS server can go down for
  # this ingestor alone while the other scenarios keep sharing it. A lost connection ends the
  # subscription and the host reopens it on its reconnect cadence: DESCRIBE INGESTOR shows the failed
  # reconnects and a growing delay. A successful subscription makes the source ready; the previous
  # failure and backoff remain until the next accepted batch proves progress.
  @nats_ingestor_lost_connection
  Scenario Outline: NATS ingestor reports a lost server connection and reconnects on its reconnect cadence
    Given NATS is running
    And a NATS endpoint the scenario controls is configured
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_controlled_addr}}'
        };
      CREATE NATS SUBJECT INGESTOR nats_notifications
        FROM nats_main SUBJECT 'notifications_lost_{{test_id}}' QUEUE GROUP 'nats_notifications_lost_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      ready: true
      """
    When NATS message is published to subject "notifications_lost_{{test_id}}"
      """
      {"user_id":41}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "user_id":41
      """
    When the controlled NATS endpoint goes down
    Then within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: expected INFO, got nothing
      """
    And within "60s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      reconnect backoff: 8s
      """
    When the controlled NATS endpoint forwards everything
    Then within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      ready: true
      """
    And within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: expected INFO, got nothing
      """
    When NATS message is published to subject "notifications_lost_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "user_id":43
      """
    And within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # A NATS STREAM ingestor reads a durable pull consumer an operator provisions; Nervix creates
  # neither the stream nor the consumer. Each delivery is acknowledged on the stream once its record
  # is delivered, so the consumer's acknowledgement floor reaches every stored message, including one
  # stored before the ingestor existed, and the consumer holds nothing more.
  @nats_stream_ingestion
  Scenario Outline: NATS STREAM ingestor delivers and acknowledges what its durable consumer holds under <delivery>
    Given NATS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS JetStream stream "orders_{{test_id}}" exists for subject "orders_{{test_id}}"
    And NATS JetStream durable pull consumer "nervix_{{test_id}}" exists on stream "orders_{{test_id}}"
    And NATS JetStream message is published to subject "orders_{{test_id}}"
      """
      {"user_id":41}
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = '{{nats_addr}}'
      };
      CREATE NATS STREAM INGESTOR nats_orders
        FROM nats_main STREAM 'orders_{{test_id}}' CONSUMER 'nervix_{{test_id}}'
          MODE <mode>
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And NATS JetStream message is published to subject "orders_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "user_id":41
      "user_id":42
      """
    And within "30s" NATS JetStream consumer "nervix_{{test_id}}" of stream "orders_{{test_id}}" acknowledges through stream sequence 2 with nothing pending
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR nats_orders;
      """
    Then the last command output contains
      """
      from: NATS STREAM nats_main STREAM 'orders_{{test_id}}'
      options: CONSUMER 'nervix_{{test_id}}'
      mode: <mode>
      quiesce: SUSPEND
      """
    When these Plexus commands are executed
      """
      SHOW CREATE INGESTOR nats_orders;
      """
    Then the last command output contains
      """
      STREAM 'orders_{{test_id}}' CONSUMER 'nervix_{{test_id}}' MODE <mode>
      """

    Examples:
      | cluster_size | replica_count | delivery       | mode                                                                                    |
      | 1            | 0             | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s                        |
      | 1            | 0             | ACK PARALLEL   | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s |
      | 3            | 1             | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s                        |
      | 3            | 1             | ACK PARALLEL   | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s |

  # A delivery whose record cannot be emitted is rejected once its ACK TIMEOUT passes. The ingestor
  # negatively acknowledges it on the stream, so the consumer delivers it again when the ingestor
  # reads on its RETRY POLICY, and the consumer acknowledges it once the emitter recovers.
  @nats_stream_ingestion
  Scenario Outline: NATS STREAM ingestor negatively acknowledges a rejected delivery so its consumer delivers it again
    Given NATS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS JetStream stream "redelivered_{{test_id}}" exists for subject "redelivered_{{test_id}}"
    And NATS JetStream durable pull consumer "nervix_{{test_id}}" exists on stream "redelivered_{{test_id}}"
    And NATS subject "redelivered_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = '{{nats_addr}}'
      };
      CREATE NATS STREAM INGESTOR nats_orders
        FROM nats_main STREAM 'redelivered_{{test_id}}' CONSUMER 'nervix_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER nats_forward FROM notifications
        TO nats_main SUBJECT 'redelivered_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING notification_codec
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    When emitter "nats_forward" enters fault mode
    And NATS JetStream message is published to subject "redelivered_{{test_id}}"
      """
      {"user_id":44}
      """
    Then the relay subscription receives a payload
      """
      "user_id":44
      """
    And within "10s" the relay subscription receives payloads
      """
      "user_id":44
      """
    When emitter "nats_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":44}
      """
    And within "30s" NATS JetStream consumer "nervix_{{test_id}}" of stream "redelivered_{{test_id}}" acknowledges through stream sequence 1 with nothing pending

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # Nervix creates no stream: an ingestor whose stream an operator has not provisioned reports the
  # stream by name and keeps reporting it while it retries.
  @nats_stream_ingestion
  Scenario Outline: A NATS STREAM ingestor whose stream does not exist reports the stream by name
    Given NATS is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = '{{nats_addr}}'
      };
      CREATE NATS STREAM INGESTOR nats_orders
        FROM nats_main STREAM 'absent_{{test_id}}' CONSUMER 'nervix_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "nats_orders" on the leader node contains
      """
      transient error: NATS stream 'absent_{{test_id}}' does not exist
      """
    And NATS JetStream holds no stream "absent_{{test_id}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Nervix creates no consumer either: an ingestor whose consumer an operator has not provisioned on
  # an existing stream reports the consumer and its stream by name.
  @nats_stream_ingestion
  Scenario Outline: A NATS STREAM ingestor whose consumer does not exist reports the consumer by name
    Given NATS is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS JetStream stream "unconsumed_{{test_id}}" exists for subject "unconsumed_{{test_id}}"
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = '{{nats_addr}}'
      };
      CREATE NATS STREAM INGESTOR nats_orders
        FROM nats_main STREAM 'unconsumed_{{test_id}}' CONSUMER 'missing_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "nats_orders" on the leader node contains
      """
      transient error: NATS consumer 'missing_{{test_id}}' of stream 'unconsumed_{{test_id}}' does not exist
      """
    And NATS JetStream stream "unconsumed_{{test_id}}" holds no consumer "missing_{{test_id}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # While a change to its domain holds its ingestor suspended, the consumer delivers nothing: what
  # is published meanwhile stays in the stream, pending for the consumer, and the ingestor delivers
  # and acknowledges it once it resumes.
  @nats_stream_ingestion @nats_stream_suspend @deloxide_stress
  Scenario Outline: A suspended NATS STREAM ingestor leaves what is published in its stream and delivers it once it resumes
    Given NATS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And entity gate deadline is configured as "120s"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS JetStream stream "suspended_{{test_id}}" exists for subject "suspended_{{test_id}}"
    And NATS JetStream durable pull consumer "nervix_{{test_id}}" exists on stream "suspended_{{test_id}}"
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE CODEC notification_codec_v2 FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = '{{nats_addr}}'
      };
      CREATE NATS STREAM INGESTOR nats_orders
        FROM nats_main STREAM 'suspended_{{test_id}}' CONSUMER 'nervix_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And NATS JetStream message is published to subject "suspended_{{test_id}}"
      """
      {"user_id":1}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "user_id":1
      """
    And within "30s" NATS JetStream consumer "nervix_{{test_id}}" of stream "suspended_{{test_id}}" acknowledges through stream sequence 1 with nothing pending
    When these Plexus commands begin executing in the background
      """
      ALTER NATS STREAM INGESTOR nats_orders SET DECODE USING notification_codec_v2;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When NATS JetStream message is published to subject "suspended_{{test_id}}"
      """
      {"user_id":2}
      """
    And NATS JetStream message is published to subject "suspended_{{test_id}}"
      """
      {"user_id":3}
      """
    Then within "10s" NATS JetStream consumer "nervix_{{test_id}}" of stream "suspended_{{test_id}}" holds 2 messages for delivery and none awaiting acknowledgement
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And within "30s" the relay subscription receives payloads
      """
      "user_id":2
      "user_id":3
      """
    And within "30s" NATS JetStream consumer "nervix_{{test_id}}" of stream "suspended_{{test_id}}" acknowledges through stream sequence 3 with nothing pending

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The consumer's position lives in the stream, so an ingestor that moves to another node when its
  # owner stops resumes after the last message it acknowledged: nothing it acknowledged is delivered
  # again and nothing published while it moved is lost.
  @nats_stream_ingestion
  Scenario: A NATS STREAM ingestor that fails over to another node resumes from its consumer's position
    Given NATS is running
    And the production sticky scheduler is configured
    And runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS JetStream stream "failover_{{test_id}}" exists for subject "failover_{{test_id}}"
    And NATS JetStream durable pull consumer "nervix_{{test_id}}" exists on stream "failover_{{test_id}}"
    And NATS subject "failover_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = '{{nats_addr}}'
      };
      CREATE NATS STREAM INGESTOR nats_orders
        FROM nats_main STREAM 'failover_{{test_id}}' CONSUMER 'nervix_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER nats_forward FROM notifications
        TO nats_main SUBJECT 'failover_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING notification_codec
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      DESCRIBE INGESTOR nats_orders;
      """
    Then the last command output owner is saved as placeholder "failed_owner"
    When NATS JetStream message is published to subject "failover_{{test_id}}"
      """
      {"user_id":1}
      """
    And NATS JetStream message is published to subject "failover_{{test_id}}"
      """
      {"user_id":2}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"user_id":1}
      {"user_id":2}
      """
    And within "30s" NATS JetStream consumer "nervix_{{test_id}}" of stream "failover_{{test_id}}" acknowledges through stream sequence 2 with nothing pending
    When node "{{failed_owner}}" is stopped
    And NATS JetStream message is published to subject "failover_{{test_id}}"
      """
      {"user_id":3}
      """
    And NATS JetStream message is published to subject "failover_{{test_id}}"
      """
      {"user_id":4}
      """
    Then within "90s" the observed broker receives exactly these payloads
      """
      {"user_id":3}
      {"user_id":4}
      """
    And within "30s" NATS JetStream consumer "nervix_{{test_id}}" of stream "failover_{{test_id}}" acknowledges through stream sequence 4 with nothing pending
