@wasm_state_qualification
Feature: WASM guest-state qualification across crash windows
  A WASM guest-state checkpoint passes through fixed windows between the moment its callback has
  dispatched output and the moment the acknowledgements the callback decided are released: before
  the guest saves, after the save is captured, after it is on the owner's stable storage, and after
  its whole boundary is confirmed. These scenarios end the owner, or the whole cluster, while one
  checkpoint is held in each window and prove which state the branch continues from and how the
  withheld input replays.

  The fixture guest numbers the rows of each branch and keeps even-numbered rows, and its row count
  is the only state it saves. The sources ingest from Kafka in `ACK SEQUENTIAL` mode, so an input
  whose acknowledgement was withheld is redelivered from the consumer group's committed offset. The
  first alpha row it emits after recovery therefore names the row count the branch continued from:
  `"value":4` means the recovered state did not reflect the replayed input, and `"value":3` means
  it did, so the replayed input was counted twice.

  # A single node is the whole cluster, so stopping it ends every task at once and restarting it
  # redelivers the withheld input exactly once from the committed offset. Stopping one node of a
  # larger cluster drains its ownership to the nodes still running instead; the owner-loss outline
  # below covers that path.
  Scenario Outline: A node stopped inside a checkpoint window replays the withheld input from the documented state
    Given Kafka is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "wasm_qualification_restart_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_qualification_filter;
      UPLOAD RESOURCE wasm_qualification_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest',
        'session.timeout.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC wasm_qualification_restart_in_{{test_id}}
          OFFSET BY CONSUMER GROUP wasm_qualification_restart_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING metric_codec
        TO raw_metrics
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_qualification_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    Then Kafka consumer group "wasm_qualification_restart_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "wasm_qualification_restart_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_qualification_restart_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    Then within "60s" Kafka consumer group "wasm_qualification_restart_group_{{test_id}}" next offset for topic "wasm_qualification_restart_in_{{test_id}}" partition 0 is "at least 2"
    When the next guest-state checkpoint of WASM processor "filter_even_rows" pauses <window>
    And Kafka message is published to topic "wasm_qualification_restart_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    Then a guest-state checkpoint of WASM processor "filter_even_rows" is held <window>
    # Output is dispatched before its checkpoint completes; only the acknowledgement waits for it.
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      """
    And within "4s" Kafka consumer group "wasm_qualification_restart_group_{{test_id}}" next offset for topic "wasm_qualification_restart_in_{{test_id}}" partition 0 is "below 3"
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      """
    Then Kafka consumer group "wasm_qualification_restart_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "wasm_qualification_restart_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    And Kafka message is published to topic "wasm_qualification_restart_in_{{test_id}}"
      """
      {"value":3,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_qualification_restart_in_{{test_id}}"
      """
      {"value":4,"tenant":"alpha"}
      """
    Then within "90s" Kafka consumer group "wasm_qualification_restart_group_{{test_id}}" next offset for topic "wasm_qualification_restart_in_{{test_id}}" partition 0 is "at least 6"
    And within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "value":12
      key={"tenant":"alpha"} | "tenant":"alpha" | <alpha_after_recovery>
      """

    Examples:
      | cluster_size | replica_count | window                 | alpha_after_recovery |
      | 1            | 0             | before_capture         | "value":4            |
      | 1            | 0             | after_capture          | "value":4            |
      | 1            | 0             | after_local_durability | "value":3            |
      | 1            | 0             | before_acknowledgement | "value":3            |

  @exclusive
  Scenario Outline: An owner lost inside a checkpoint window continues each branch from its replica
    Given Kafka is running
    And runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "wasm_qualification_failover_in_{{test_id}}" exists with 1 partitions
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-1;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_qualification_failover_filter;
      UPLOAD RESOURCE wasm_qualification_failover_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest',
        'session.timeout.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC wasm_qualification_failover_in_{{test_id}}
          OFFSET BY CONSUMER GROUP wasm_qualification_failover_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING metric_codec
        TO raw_metrics
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_qualification_failover_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      START;
      """
    # Keep node-1 cordoned so the surviving replica owns recovery through the output assertion.
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "wasm_processor" "filter_even_rows" is saved as placeholder "failed_owner"
    And the first replica for scheduled "wasm_processor" "filter_even_rows" in the last cluster status is saved as placeholder "promoted_replica"
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      """
    Then Kafka consumer group "wasm_qualification_failover_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "wasm_qualification_failover_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_qualification_failover_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    Then within "60s" Kafka consumer group "wasm_qualification_failover_group_{{test_id}}" next offset for topic "wasm_qualification_failover_in_{{test_id}}" partition 0 is "at least 2"
    When the next guest-state checkpoint of WASM processor "filter_even_rows" pauses <window>
    And Kafka message is published to topic "wasm_qualification_failover_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    Then a guest-state checkpoint of WASM processor "filter_even_rows" is held <window>
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      """
    And within "4s" Kafka consumer group "wasm_qualification_failover_group_{{test_id}}" next offset for topic "wasm_qualification_failover_in_{{test_id}}" partition 0 is "below 3"
    When node "{{failed_owner}}" is stopped
    Then node "{{promoted_replica}}" eventually observes a stable leader
    And within "60s" node "{{promoted_replica}}" eventually reports scheduled "wasm_processor" "filter_even_rows" owner equals placeholder "promoted_replica"
    When Kafka message is published to topic "wasm_qualification_failover_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    And Kafka message is published to topic "wasm_qualification_failover_in_{{test_id}}"
      """
      {"value":3,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_qualification_failover_in_{{test_id}}"
      """
      {"value":4,"tenant":"alpha"}
      """
    Then within "90s" Kafka consumer group "wasm_qualification_failover_group_{{test_id}}" next offset for topic "wasm_qualification_failover_in_{{test_id}}" partition 0 is "at least 6"
    And within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta"
      key={"tenant":"alpha"} | "tenant":"alpha"
      """

    # The source keeps redelivering the withheld alpha input while no owner accepts it, and the
    # promoted owner has no replacement replica to confirm checkpoints against, so either branch
    # can apply a redelivered input more than once and its row numbering after recovery is not
    # fixed. Every input is still acknowledged exactly when a checkpoint covering it completes, and
    # both branches keep processing. The single-node outline above pins which state each window
    # recovers. The two windows at either end of the boundary are the ones owner loss can tell
    # apart: the promoted replica holds nothing of the first and all of the second.
    Examples:
      | window                 |
      | before_capture         |
      | before_acknowledgement |

  # Stopping a node ends each processor task within its shutdown grace. A branch task still waiting
  # for its checkpoint when its processor task is ended is ended with it, so it cannot outlive its
  # node, keep the node's state store open, or settle acknowledgements after the node stopped.
  Scenario: A cluster stopped while its owner holds a checkpoint ends the held branch with its node
    Given Kafka is running
    And runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "wasm_qualification_cluster_restart_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_qualification_filter;
      UPLOAD RESOURCE wasm_qualification_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest',
        'session.timeout.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC wasm_qualification_cluster_restart_in_{{test_id}}
          OFFSET BY CONSUMER GROUP wasm_qualification_cluster_restart_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING metric_codec
        TO raw_metrics
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_qualification_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    Then Kafka consumer group "wasm_qualification_cluster_restart_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "wasm_qualification_cluster_restart_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_qualification_cluster_restart_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    Then within "60s" Kafka consumer group "wasm_qualification_cluster_restart_group_{{test_id}}" next offset for topic "wasm_qualification_cluster_restart_in_{{test_id}}" partition 0 is "at least 2"
    When the next guest-state checkpoint of WASM processor "filter_even_rows" pauses before_acknowledgement
    And Kafka message is published to topic "wasm_qualification_cluster_restart_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    Then a guest-state checkpoint of WASM processor "filter_even_rows" is held before_acknowledgement
    # Output is dispatched before its checkpoint completes; only the acknowledgement waits for it.
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      """
    And within "4s" Kafka consumer group "wasm_qualification_cluster_restart_group_{{test_id}}" next offset for topic "wasm_qualification_cluster_restart_in_{{test_id}}" partition 0 is "below 3"
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      """
    Then Kafka consumer group "wasm_qualification_cluster_restart_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "wasm_qualification_cluster_restart_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    And Kafka message is published to topic "wasm_qualification_cluster_restart_in_{{test_id}}"
      """
      {"value":3,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_qualification_cluster_restart_in_{{test_id}}"
      """
      {"value":4,"tenant":"alpha"}
      """
    Then within "90s" Kafka consumer group "wasm_qualification_cluster_restart_group_{{test_id}}" next offset for topic "wasm_qualification_cluster_restart_in_{{test_id}}" partition 0 is "at least 6"
    And within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "value":12
      key={"tenant":"alpha"} | "tenant":"alpha"
      """


  Scenario Outline: A reset committed but not usable when the cluster stops resumes its one lifetime
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a node-1-owned branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When WASM guest-state checkpoints fail to reach stable storage on every node
    And WASM processor "counting_guest" state reset for branch fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      reset was committed but its new lifetime is not usable
      """
    And within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: PUBLISHING, branch, generation 2
      """
    When WASM guest-state checkpoints reach stable storage again on every node
    And the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    And within "30s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: PUBLISHING, branch, generation 2
      """
    When WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    Then within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: READY, branch, generation 2
      state reset reason: OPERATOR
      state reset readiness: READY
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION counted_events_subscription TO counted_events;
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
