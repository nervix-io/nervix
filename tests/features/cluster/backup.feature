Feature: Configuration backup into a public archive

  Scenario Outline: A configuration-only backup needs no domain state cut
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA item ( id I64 );
      CREATE RELAY items SCHEMA item UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --without-state" from node "{{leader}}" into "configuration-only.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "configuration-only.nvxb" as text
    Then the CLI output contains "cut: without state"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A stopped domain backup records a stopped cut without a freeze
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "stopped.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "stopped.nvxb" as text
    Then the CLI output contains "cut: stopped"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A live backup reports a crash-consistent cut
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --without-pause" from node "{{leader}}" into "live.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "live.nvxb" as text
    Then the CLI output contains "cut: live"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A running domain backup quiesces and resumes its domain
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "quiesced.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "quiesced.nvxb" as text
    Then the CLI output contains "cut: quiesced"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output contains
      """
      {{domain}} pace=UNPACED status=RUNNING
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A quiesced cut leaves another domain and its sessions active
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the active domain is saved as placeholder "primary_domain"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      CREATE UNPACED DOMAIN {{domain}}_other;
      """
    Given the active domain is "{{primary_domain}}_other"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge backup-other-{{test_id}}.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Given the active domain is "{{primary_domain}}"
    And the backup cut for domain "{{primary_domain}}" will pause after draining
    Then the current leader node is saved as placeholder "leader"
    When the CLI begins backing up "domain {{primary_domain}} --timeout 30s" from node "{{leader}}" into "isolated-cut.nvxb" in the background
    Then the backup cut for domain "{{primary_domain}}" has reached its pause
    Given the active domain is "{{primary_domain}}_other"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA still_writable ( id I64 );
      CREATE SUBSCRIPTION events_subscription TO events;
      """
    Then node "{{leader}}" eventually reports status containing "{{primary_domain}}_other status=Running pace=UNPACED"
    When http payload is posted to host "backup-other-{{test_id}}.example.com" path "/events"
      """
      {"id":42}
      """
    Then the relay subscription receives a payload
      """
      {"id":42}
      """
    When the backup cut pause for domain "{{primary_domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{primary_domain}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A quiesced backup reports records dropped by its NATS source
    Given NATS is running
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT drop_nats TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE INGESTOR event_source FROM NATS drop_nats
        SUBJECT backup_drop_{{test_id}} QUEUE GROUP backup_drop_group_{{test_id}}
        INSTANCES 1 MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    Given the backup cut for domain "{{domain}}" will pause after draining
    Then the current leader node is saved as placeholder "leader"
    When the CLI begins backing up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "dropped.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    When NATS message is published to subject "backup_drop_{{test_id}}"
      """
      {"id":1}
      """
    Then the relay subscription does not receive a payload within "1s"
    When the backup cut pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And the CLI backup reports at least 1 records dropped during quiesce
    When NATS message is published to subject "backup_drop_{{test_id}}"
      """
      {"id":2}
      """
    Then the relay subscription receives a payload
      """
      {"id":2}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A parked required wait does not hold the cut and its Kafka input redelivers
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And Kafka topic "backup_wait_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wait_event ( id I64, source STRING );
      CREATE WIRE JSON SCHEMA wait_event_wire MODE STRICT ( id integer, source string );
      CREATE CODEC wait_event_codec FROM WIRE JSON SCHEMA wait_event_wire TO SCHEMA wait_event;
      CREATE RELAY wait_state SCHEMA wait_event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY waiting_input SCHEMA wait_event UNBRANCHED;
      CREATE RELAY waiting_output SCHEMA wait_event UNBRANCHED;
      CREATE CLIENT wait_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR wait_event_source
        FROM KAFKA wait_kafka TOPIC backup_wait_{{test_id}}
          OFFSET BY DOMAIN MODE ACK SEQUENTIAL ACK TIMEOUT 5m
            RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING wait_event_codec
        TO waiting_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE VHOST edge backup-wait-{{test_id}}.example.com;
      CREATE ENDPOINT wait_state_ingress ON edge PATH '/state' TYPE HTTP;
      CREATE INGESTOR wait_state_source
        FROM ENDPOINT wait_state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING wait_event_codec
        TO wait_state INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION wait_for_state FROM waiting_input UNBRANCHED
        USING MATERIALIZED STATE wait_state REQUIRED WAIT
        TO waiting_output INHERIT ALL
          SET source = relay_state.wait_state.source
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION waiting_input_seen TO waiting_input;
      START;
      """
    When Kafka message is published to topic "backup_wait_{{test_id}}"
      """
      {"id":7,"source":"input"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"id":7,"source":"input"}
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 10s" from node "{{leader}}" into "parked.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored-"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "parked.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 0 resource versions and 12 models
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION waiting_output_seen TO waiting_output;
      START;
      """
    Then the relay subscription does not receive a payload within "2s"
    When http payload is posted to node "{{leader}}" with host "backup-wait-{{test_id}}.example.com" path "/state"
      """
      {"id":0,"source":"state"}
      """
    Then within "60s" the relay subscription receives a payload
      """
      {"id":7,"source":"state"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An unavailable emitter times out a backup drain with outstanding work
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      hold response until released
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (event_id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge backup-stalled-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api_client TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 60000
      };
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP api_client METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "backup-stalled-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"}]
      """
    Then HTTP receiver "api" eventually receives at least 1 requests
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 3s" from node "{{leader}}" into "stalled.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "publishing_emitters"
    And backup archive "stalled.nvxb" does not exist
    When HTTP receiver "api" releases its held responses with "respond 204"
    And http payload is posted to host "backup-stalled-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A backup waits for a committing transaction's domain lease
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "owner" is connected to node "{{leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER SCHEMA event ADD FIELD note STRING OPTIONAL;
      """
    Given transaction commit on node "{{leader}}" pauses after 1 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "{{leader}}" after 1 statement is reached
    When the CLI begins backing up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "after-commit.nvxb" in the background
    Then the background CLI backup remains pending for "1s"
    When the transaction commit pause on node "{{leader}}" after 1 statement is released
    Then the background NSPL execution succeeds
    And the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{domain}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A coordinator leadership change in the cut resumes the domain and retries the backup
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "survivor"
    Given the backup cut for domain "{{domain}}" will pause after draining
    When the CLI begins backing up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "interrupted.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    When leadership is transferred from node "{{leader}}" to node "{{survivor}}"
    Then node "{{survivor}}" eventually reports a leader other than "{{leader}}"
    And node "{{leader}}" eventually reports a leader other than "{{leader}}"
    When the backup cut pause for domain "{{domain}}" is released
    Then node "{{survivor}}" eventually reports status containing "{{domain}} status=Running pace=UNPACED"
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "interrupted.nvxb" as text
    Then the CLI output contains "cut: quiesced"

  Scenario: A stale restore coordinator cannot republish after a new leader starts the domain
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "10m"
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And Kafka topic "backup_wasm_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
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
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC backup_wasm_in_{{test_id}}
          OFFSET BY DOMAIN MODE ACK SEQUENTIAL ACK TIMEOUT 30s
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
        USING RESOURCE wasm_filter VERSION 1
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
    When Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "value":2
      key={"tenant":"beta"} | "value":12
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And a node other than placeholder "leader" is saved as placeholder "survivor"
    Given restoring domain "{{domain}}_copy" by coordinator "{{leader}}" pauses before state publication
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "stateful.nvxb" is streamed to node "{{leader}}" under execution reference "restore_reference" in the background
    Then restoring domain "{{domain}}_copy" by coordinator "{{leader}}" has reached state publication
    When leadership is transferred from node "{{leader}}" to node "{{survivor}}"
    Then node "{{survivor}}" eventually reports a leader other than "{{leader}}"
    And node "{{leader}}" eventually reports a leader other than "{{leader}}"
    And restore "domain {{domain}} AS {{domain}}_copy" of backup archive "stateful.nvxb" completes on node "{{survivor}}" under execution reference "restore_reference"
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    When Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":3,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":13,"tenant":"beta"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":4,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":14,"tenant":"beta"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "value":4
      key={"tenant":"beta"} | "value":14
      """
    When the CLI backs up "domain {{domain}}" from node "{{survivor}}" into "before-stale.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When state publication of domain "{{domain}}" by coordinator "{{leader}}" is released
    Then state publication of domain "{{domain}}" by coordinator "{{leader}}" is refused
    When the CLI backs up "domain {{domain}}" from node "{{survivor}}" into "after-stale.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "before-stale.nvxb" and "after-stale.nvxb" have identical guest checkpoints and source offsets

  Scenario Outline: A quiesced backup omits guest checkpoints after WASM branch TTL eviction
    Given branched relay expiration scan interval is configured as "100ms"
    And Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "10m"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And Kafka topic "backup_wasm_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5s;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC backup_wasm_in_{{test_id}}
          OFFSET BY DOMAIN MODE ACK SEQUENTIAL ACK TIMEOUT 30s
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
        USING RESOURCE wasm_filter VERSION 1
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
    When Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "value":2
      key={"tenant":"beta"} | "value":12
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "wasm_processor" "filter_even_rows" is saved as placeholder "wasm_owner"
    And node "{{wasm_owner}}" observability metric "nervix_branch_evictions_total" with labels eventually reaches at least 2
      """
      domain="{{domain}}"
      branch="by_tenant"
      physical_node_id="{{wasm_owner}}"
      reason="ttl"
      """
    And node "{{wasm_owner}}" observability metric "nervix_branch_instances" with labels eventually equals 0
      """
      domain="{{domain}}"
      branch="by_tenant"
      physical_node_id="{{wasm_owner}}"
      """
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "expired.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "expired.nvxb" as json
    Then the described backup has exactly 0 "wasm_processor" state sections

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A quiesced backup restores two WASM branches and Kafka domain offsets
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "10m"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And Kafka topic "backup_wasm_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
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
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC backup_wasm_in_{{test_id}}
          OFFSET BY DOMAIN MODE ACK SEQUENTIAL ACK TIMEOUT 30s
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
        USING RESOURCE wasm_filter VERSION 1
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
    When Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "value":2
      key={"tenant":"beta"} | "value":12
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "stateful.nvxb" as text
    Then the CLI output contains "wasm_processor=filter_even_rows"
    And the CLI output contains "kafka_ingestor=metric_source"
    And the CLI output contains "branch_lifecycle=metric_source"
    And the CLI output contains "branch_lifecycle=filter_even_rows"
    Given the active domain is saved as placeholder "source_domain"
    And restoring domain "{{domain}}_incomplete" fails before installing its first WASM checkpoint
    When the CLI restores "domain {{domain}} --as {{domain}}_incomplete" from "stateful.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "runtime state"
    Given the active domain is "{{domain}}_incomplete"
    When these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    Given the active domain is "{{source_domain}}"
    When the CLI backs up "domain {{domain}} --without-state" from node "{{leader}}" into "configuration-stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "configuration-stateful.nvxb" as json
    Then the described backup has exactly 0 "any" state sections
    When the CLI backs up "domain {{domain}} --without-pause" from node "{{leader}}" into "live-stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "live-stateful.nvxb" as text
    Then the CLI output contains "cut: live"
    And the CLI output contains "wasm_processor=filter_even_rows"
    Given backup archive "stateful.nvxb" is copied to "mismatched-state.nvxb" with mismatched WASM state schemas
    When the CLI restores "domain {{domain}} --as {{domain}}_mismatched" from "mismatched-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_mismatched" with 1 resource versions and 10 models
    And the CLI restore warns that saved state has a mismatched schema
    Given backup archive "stateful.nvxb" is copied to "unsupported-state.nvxb" with an unsupported Kafka state version
    When the CLI restores "domain {{domain}} --as {{domain}}_unsupported" from "unsupported-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_unsupported" with 1 resource versions and 10 models
    And the CLI output contains "unsupported record version"
    When the CLI restores "domain {{domain}} --as {{domain}}_no_offsets --without-source-offsets" from "stateful.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_no_offsets" with 1 resource versions and 10 models
    When the CLI backs up "domain {{domain}}_no_offsets" from node "{{leader}}" into "no-offsets.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}_no_offsets"
    When the CLI describes backup archive "no-offsets.nvxb" as json
    Then the described backup has exactly 0 "kafka_offsets" state sections
    And the described backup has exactly 2 "wasm_processor" state sections
    When the CLI restores "domain {{domain}} --as {{domain}}_without_state --without-state" from "stateful.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_without_state" with 1 resource versions and 10 models
    When the CLI backs up "domain {{domain}}_without_state" from node "{{leader}}" into "no-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}_without_state"
    When the CLI describes backup archive "no-state.nvxb" as json
    Then the described backup has exactly 0 "any" state sections
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored-"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "stateful.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 1 resource versions and 10 models
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      kafka observed partitions: 0
      """
    And within "10s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      kafka instance 0 partitions: 0
      """
    And within "10s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      transient error: -
      """
    And within "10s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      ready: true
      """
    And the relay subscription does not receive a payload within "2s"
    When Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":3,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":13,"tenant":"beta"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":4,"tenant":"alpha"}
      """
    And Kafka message is published to topic "backup_wasm_in_{{test_id}}"
      """
      {"value":14,"tenant":"beta"}
      """
    Then within "10s" DESCRIBE WASM PROCESSOR "filter_even_rows" on the leader node contains
      """
      state structures: 2
      """
    Then within "10s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals 4
    Then within "10s" DESCRIBE DOMAIN section "processed" metric "messages_total" "sent" relay "filtered_metrics" across physical nodes totals 2
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "value":4
      key={"tenant":"beta"} | "value":14
      """
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "restored-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "stateful.nvxb" and "restored-state.nvxb" keep the WASM processor branch incarnations

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A cluster backup archives every domain's models, users and resource versions
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      CREATE SCHEMA order_event ( id I64, amount F64 );
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE RELAY large_orders SCHEMA order_event UNBRANCHED;
      CREATE JUNCTION large_order_filter FROM orders UNBRANCHED TO large_orders INHERIT ALL WHERE output.amount > 100.0 FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster" from node "{{leader}}" into "cluster.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archive "cluster.nvxb" holds for domain "{{domain}}" exactly the models these NSPL commands create
      """
      CREATE SCHEMA order_event ( id I64, amount F64 );
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE RELAY large_orders SCHEMA order_event UNBRANCHED;
      CREATE JUNCTION large_order_filter FROM orders UNBRANCHED TO large_orders INHERIT ALL WHERE output.amount > 100.0 FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When the CLI describes backup archive "cluster.nvxb" as json
    Then the described backup lists the scenario user
    And the described backup lists version 1 of resource "proto" in domain "{{domain}}" with the checksums DESCRIBE RESOURCE reports on node "{{leader}}"
    When the CLI describes backup archive "cluster.nvxb" as text
    Then the CLI output contains "scope: cluster"
    And the CLI output contains "resource=proto version=1 state=completed"
    And the CLI output contains "archive=included"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A domain backup without resources records every version's checksums without its bytes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --without-resources" from node "{{leader}}" into "domain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "domain.nvxb" as json
    Then the described backup holds no users
    And the described backup lists version 1 of resource "proto" in domain "{{domain}}" with the checksums DESCRIBE RESOURCE reports on node "{{leader}}" and without its archive
    When the CLI describes backup archive "domain.nvxb" as text
    Then the CLI output contains "users: not included"
    And the CLI output contains "archive=omitted"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" to standard output
    Then the archive the CLI wrote to standard output verifies and its report went to standard error

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An archive larger than 64 MiB streams to the client completely
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "bulk_dir" holding a 65 MiB file
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE bulk;
      UPLOAD RESOURCE bulk VERSION '{{bulk_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster" from node "{{leader}}" into "bulk.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archive "bulk.nvxb" is larger than 64 MiB

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client that loses its download fetches the archive again until a download collects it
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "bulk_dir" holding a 4 MiB file
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE bulk;
      UPLOAD RESOURCE bulk VERSION '{{bulk_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's archive download from node "{{leader}}" is abandoned after 2 chunks
    And the backup's archive is downloaded from node "{{leader}}"
    Then the downloaded archive matches the backup's summary
    When the backup's archive is downloaded from node "{{leader}}"
    Then the download is refused as "NotRetained"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An archive is refused once its execution reference's retry validity ends
    Given command retry identities are valid for "5s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's retry validity has ended
    And the backup's archive is downloaded from node "{{leader}}"
    Then the download is refused as "Expired"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A backup sent to a follower runs on the leader, and a follower sends its download there
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event ( id I64 );
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the CLI backs up "cluster" from node "{{follower}}" into "follower.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's archive is downloaded from node "{{follower}}"
    Then the download is redirected to node "{{leader}}"

  Scenario Outline: A backup taken while models change holds one coherent revision
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA burst_event ( value I64 );
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "writer" is connected to the leader node
    When client "writer" begins executing these NSPL commands in the background
      """
      CREATE RELAY burst_1 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_2 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_3 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_4 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_5 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_6 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_7 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_8 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_9 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_10 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_11 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_12 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_13 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_14 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_15 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_16 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_17 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_18 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_19 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_20 SCHEMA burst_event UNBRANCHED;
      """
    And the CLI backs up "cluster --without-state" from node "{{leader}}" into "concurrent.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archive "concurrent.nvxb" holds for domain "{{domain}}" schema "burst_event" and relays "burst_" numbered contiguously from 1
    And the background NSPL execution succeeds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: An archive with a foreign record kind or format version is refused at its header
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster" from node "{{leader}}" into "valid.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When backup archive "valid.nvxb" is copied to "foreign_kind.nvxb" with its manifest's record kind set to 9
    And the CLI describes backup archive "foreign_kind.nvxb" as text
    Then the CLI fails with "holds a record of kind 9 where a manifest record belongs"
    When backup archive "valid.nvxb" is copied to "foreign_version.nvxb" with its manifest's record version set to 3
    And the CLI describes backup archive "foreign_version.nvxb" as text
    Then the CLI fails with "holds a manifest record of format version 3; this reader supports version 2"

  Scenario: The CLI exits with a failure and a JSON error when a backup is refused
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain missing_{{test_id}}" from node "{{leader}}" into "missing.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And backup archive "missing.nvxb" does not exist
    When the CLI backs up "cluster {{domain}}" from node "{{leader}}" into "named.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "INVALID_ARGUMENTS"
    When the CLI describes backup archive "absent.nvxb" as json
    Then the CLI backup failed with JSON error code "INVALID_ARCHIVE"

  Scenario: BACKUP runs alone, outside a transaction, and DESCRIBE BACKUP never reaches a server
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "operator" is connected to the leader node
    When client "operator" executes these NSPL commands
      """
      BEGIN;
      """
    And client "operator" fails to execute these NSPL commands
      """
      BACKUP CLUSTER TO 'in-transaction.nvxb';
      """
    Then the last command error contains
      """
      BACKUP cannot be queued in a transaction
      """
    When the session on node "{{leader}}" runs "DESCRIBE BACKUP 'cluster.nvxb';"
    Then the session's command failed with "DESCRIBE BACKUP reads an archive on the client's machine"

  Scenario Outline: Downloads of another user's backup, or under a reference without an archive, are refused
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE USER backup_reader WITH PASSWORD 'reader-password';
      """
    Then the current leader node is saved as placeholder "leader"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's archive is downloaded from node "{{leader}}" as user "backup_reader" with password "reader-password"
    Then the download is refused as "NotOwner"
    When an archive is downloaded from node "{{leader}}" under an execution reference no command used
    Then the download is refused as "NotRetained"
    When the session on node "{{leader}}" runs "CREATE SCHEMA plain_event ( id I64 );"
    And an archive is downloaded from node "{{leader}}" under the session command's execution reference
    Then the download is refused as "NotRetained"
    When the backup's archive is downloaded from node "{{leader}}"
    Then the downloaded archive matches the backup's summary

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A download without credentials is refused
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's archive is downloaded from node "{{leader}}" without credentials
    Then the download is refused as unauthenticated
