Feature: Redis command pool lifetimes
  A Redis client's command pool retires its connections at the lifetimes the client declares and
  opens replacements to keep POOL SIZE MIN. A stallable endpoint in front of Redis counts this
  client's connections, which the shared Redis server cannot tell apart from other scenarios'.

  Scenario Outline: A Redis command pool retires its connections at a declared lifetime
    Given Redis is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And a stallable endpoint "redis_counted" stands in front of "redis_addr"
    And Redis channel "pool_lifetime_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge pool-lifetime-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{redis_counted}}',
        '<key>' = '1000'
      };
      CREATE REDIS PUBSUB EMITTER redis_out FROM notifications
        TO redis_main CHANNEL 'pool_lifetime_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the stallable endpoint "redis_counted" eventually accepts 2 connections
    And the stallable endpoint "redis_counted" accepted connection 2 at least "1s" after connection 1
    When http payload is posted to host "pool-lifetime-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | key                  |
      | 1            | pool_max_lifetime_ms |
      | 3            | pool_max_lifetime_ms |
      | 1            | pool_idle_timeout_ms |
      | 3            | pool_idle_timeout_ms |
