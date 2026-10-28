Feature: Redis command pool minimum
  A Redis client's command pool keeps POOL SIZE MIN established command connections in total,
  counting the ones its publishers have checked out together with the idle ones, and opens another
  only when a publisher finds none idle. A stallable endpoint in front of Redis counts this
  client's connections, which the shared Redis server cannot tell apart from other scenarios'.

  Scenario Outline: A publishing Redis client keeps POOL SIZE MIN <minimum> command connections in total
    Given Redis is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And a stallable endpoint "redis_counted" stands in front of "redis_addr"
    And Redis channel "pool_minimum_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA metric (value I64);
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT (value integer);
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge pool-minimum-{{test_id}}.example.com;
      CREATE ENDPOINT metrics_endpoint ON edge PATH '/metrics' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_metrics
        FROM metrics_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO metrics INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN <minimum> MAX 4 CONFIG {
        'addr' = '{{redis_counted}}'
      };
      CREATE REDIS PUBSUB EMITTER redis_out FROM metrics
        TO redis_main CHANNEL 'pool_minimum_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING metric_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the stallable endpoint "redis_counted" eventually accepts <minimum> connections
    And node "node-1" eventually accepts http traffic for host "pool-minimum-{{test_id}}.example.com" path "/metrics"
      """
      {"value":0}
      """
    When 20 sequential metric http payloads are posted to host "pool-minimum-{{test_id}}.example.com" path "/metrics"
    Then within "60s" the observed broker receives exactly 21 messages
    And the stallable endpoint "redis_counted" accepts no connection beyond <minimum> within "2s"

    Examples:
      | cluster_size | minimum |
      | 1            | 1       |
      | 3            | 1       |
      | 1            | 2       |
      | 3            | 2       |
