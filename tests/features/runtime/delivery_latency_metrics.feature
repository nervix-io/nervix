Feature: Delivery latency metrics

  # The domain clock is started at 2000-01-01T00:01:00Z with a time rate so small that it never
  # leaves that instant, so every processor and reingestor measures delivery latency against the
  # same domain time and each row's latency is exactly its distance from it. Emitters measure
  # against the wall clock, so every row they receive is decades old.
  Scenario Outline: Node inputs record each batch's delivery latency from its rows' ingestion watermarks
    Given a <cluster_size> node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1m SKEW 1m;
      CREATE SCHEMA reading ( tenant STRING, sequence I64, occurred_at DATETIME );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH tenants SCHEMA tenant_key TTL 1h;
      CREATE CODEC reading_batch_codec FROM JSON TO SCHEMA reading
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( tenant string, sequence integer, occurred_at string );
      CREATE CODEC reading_wire_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY raw_readings SCHEMA reading BRANCHED BY tenants;
      CREATE RELAY admitted_readings SCHEMA reading BRANCHED BY tenants;
      CREATE RELAY reingested_readings SCHEMA reading BRANCHED BY tenants;
      CREATE RELAY emitted_readings SCHEMA reading BRANCHED BY tenants;
      CREATE VHOST edge delivery-latency-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_batch_codec
        TIMESTAMP AT occurred_at
        TO raw_readings INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION admit FROM raw_readings
        FILTER WHERE input.sequence < 100
        BRANCHED BY tenants
        TO admitted_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE REINGESTOR reingest FROM admitted_readings
        TO reingested_readings INHERIT ALL BRANCHED BY tenants
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION forward FROM reingested_readings
        BRANCHED BY tenants
        TO emitted_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
      CREATE EMITTER publish FROM emitted_readings TO ZEROMQ zeromq_main MODE NO_ACK
        RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING reading_wire_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:01:00Z' TIME RATE 1e-300;
      """
    # Each tenant's first batch holds one row 100 ms or 2.5 s before the domain time and one row
    # after it, whose negative latency is not recorded. The junction drops the later rows.
    When http payload is posted to host "delivery-latency-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"acme","sequence":1,"occurred_at":"2000-01-01T00:00:59.900Z"},{"tenant":"globex","sequence":2,"occurred_at":"2000-01-01T00:00:57.500Z"},{"tenant":"acme","sequence":901,"occurred_at":"2000-01-01T00:01:00.500Z"},{"tenant":"globex","sequence":902,"occurred_at":"2000-01-01T00:01:00.250Z"}]
      """
    Then within "30s" the observed broker receives payloads
      """
      "sequence":1
      "sequence":2
      """
    # The second batches are 45 s old: older than the watermark every input has already seen, and
    # beyond the 30 s top of the recorded latency range.
    When http payload is posted to host "delivery-latency-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"acme","sequence":3,"occurred_at":"2000-01-01T00:00:15Z"},{"tenant":"globex","sequence":4,"occurred_at":"2000-01-01T00:00:15Z"}]
      """
    Then within "30s" the observed broker receives payloads
      """
      "sequence":3
      "sequence":4
      """
    When these NSPL commands are executed
      """
      DESCRIBE JUNCTION admit;
      """
    Then the last command output owner is saved as placeholder "admit_owner"
    And the last command output metric "messages_total" "received" relay "raw_readings" physical node "{{admit_owner}}" has values
      """
      total=6
      domain_rate_per_sec=0.131868
      """
    And the last command output metric "batches_total" "received" relay "raw_readings" physical node "{{admit_owner}}" has values
      """
      total=4
      """
    And the last command output metric "delivery_latency_seconds" "received" relay "raw_readings" physical node "{{admit_owner}}" has values
      """
      p50_15m=2.5
      p90_15m=30.1
      p99_15m=30.1
      domain_p50_1m=0.1
      domain_p90_1m=2.5
      domain_p99_1m=2.5
      domain_p50_15m=0.1
      domain_p90_15m=2.5
      domain_p99_15m=2.5
      """
    And node "{{admit_owner}}" observability metric "nervix_delivery_latency_seconds_count" with labels eventually equals 4
      """
      target="admit"
      direction="received"
      relay="raw_readings"
      """
    And node "{{admit_owner}}" observability metric "nervix_delivery_latency_seconds_bucket" with labels eventually equals 1
      """
      target="admit"
      relay="raw_readings"
      le="0.1"
      """
    And node "{{admit_owner}}" observability metric "nervix_delivery_latency_seconds_bucket" with labels eventually equals 2
      """
      target="admit"
      relay="raw_readings"
      le="30"
      """
    And node "{{admit_owner}}" observability metric "nervix_delivery_latency_seconds_bucket" with labels eventually equals 4
      """
      target="admit"
      relay="raw_readings"
      le="+Inf"
      """
    When these NSPL commands are executed
      """
      DESCRIBE REINGESTOR reingest;
      """
    Then the last command output owner is saved as placeholder "reingest_owner"
    And the last command output metric "messages_total" "received" relay "admitted_readings" physical node "{{reingest_owner}}" has values
      """
      total=4
      domain_rate_per_sec=0.089087
      """
    And the last command output metric "delivery_latency_seconds" "received" relay "admitted_readings" physical node "{{reingest_owner}}" has values
      """
      p50_15m=2.5
      p90_15m=30.1
      p99_15m=30.1
      domain_p50_1m=0.1
      domain_p90_1m=2.5
      domain_p99_1m=2.5
      domain_p50_15m=2.5
      domain_p90_15m=30.1
      domain_p99_15m=30.1
      """
    And node "{{reingest_owner}}" observability metric "nervix_delivery_latency_seconds_count" with labels eventually equals 4
      """
      target="reingest"
      direction="received"
      relay="admitted_readings"
      """
    And node "{{reingest_owner}}" observability metric "nervix_delivery_latency_seconds_bucket" with labels eventually equals 2
      """
      target="reingest"
      relay="admitted_readings"
      le="5"
      """
    When these NSPL commands are executed
      """
      DESCRIBE JUNCTION forward;
      """
    Then the last command output owner is saved as placeholder "forward_owner"
    And the last command output metric "messages_total" "received" relay "reingested_readings" physical node "{{forward_owner}}" has values
      """
      total=4
      domain_rate_per_sec=0.089087
      """
    And the last command output metric "delivery_latency_seconds" "received" relay "reingested_readings" physical node "{{forward_owner}}" has values
      """
      p50_15m=2.5
      p90_15m=30.1
      p99_15m=30.1
      domain_p50_1m=0.1
      domain_p90_1m=2.5
      domain_p99_1m=2.5
      domain_p50_15m=2.5
      domain_p90_15m=30.1
      domain_p99_15m=30.1
      """
    And node "{{forward_owner}}" observability metric "nervix_delivery_latency_seconds_bucket" with labels eventually equals 1
      """
      target="forward"
      relay="reingested_readings"
      le="0.1"
      """
    When these NSPL commands are executed
      """
      DESCRIBE EMITTER publish;
      """
    Then the last command output owner is saved as placeholder "publish_owner"
    And the last command output metric "messages_total" "received" relay "emitted_readings" physical node "{{publish_owner}}" has values
      """
      total=4
      domain_rate_per_sec=0.089087
      """
    And the last command output metric "delivery_latency_seconds" "received" relay "emitted_readings" physical node "{{publish_owner}}" has values
      """
      p50_15m=30.1
      p99_15m=30.1
      domain_p50_1m=30.1
      domain_p99_1m=30.1
      domain_p50_15m=30.1
      domain_p99_15m=30.1
      """
    And node "{{publish_owner}}" observability metric "nervix_delivery_latency_seconds_bucket" with labels eventually equals 0
      """
      target="publish"
      relay="emitted_readings"
      le="30"
      """
    And node "{{publish_owner}}" observability metric "nervix_delivery_latency_seconds_bucket" with labels eventually equals 4
      """
      target="publish"
      relay="emitted_readings"
      le="+Inf"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
