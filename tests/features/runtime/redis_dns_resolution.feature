Feature: Redis name resolution

  Scenario Outline: Redis source and sink use the node DNS fixture over <scheme>
    Given Redis is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the Redis endpoints are published under fixture DNS name "redis.nervix.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Redis channel "dns_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT redis_main TYPE REDIS POOL SIZE MIN 1 MAX 1 MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{<address>}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE INGESTOR redis_in
        FROM REDIS PUBSUB redis_main CHANNEL dns_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER redis_out FROM notifications
        TO REDIS PUBSUB redis_main CHANNEL dns_out_{{test_id}}
        MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Redis channel "dns_in_{{test_id}}" eventually has 1 subscribers
    When Redis message is published to channel "dns_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And the DNS fixture eventually receives a question for "redis.nervix.test"

    Examples:
      | scheme | address            | cluster_size |
      | Redis  | redis_dns_addr     | 1            |
      | Redis  | redis_dns_addr     | 3            |
      | Rediss | redis_tls_dns_addr | 1            |
      | Rediss | redis_tls_dns_addr | 3            |

  Scenario Outline: A Rediss subscription rejects a certificate for another DNS hostname
    Given Redis is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the Redis endpoints are published under fixture DNS name "redis.unlisted.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT redis_main TYPE REDIS POOL SIZE MIN 0 MAX 1 MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{redis_tls_dns_addr}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE INGESTOR redis_in
        FROM REDIS PUBSUB redis_main CHANNEL dns_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "redis_in" on the leader node contains
      """
      certificate not valid for name "redis.unlisted.test"
      """
    And the DNS fixture eventually receives a question for "redis.unlisted.test"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Redis subscriptions and command pools follow a changed answer after <answer>
    Given Redis is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And Redis is forwarded as "redis.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "redis.nervix.test" with addresses "127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Redis channel "dns_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT redis_main TYPE REDIS POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{redis_forwarded_addr}}'
      };
      CREATE INGESTOR redis_in
        FROM REDIS PUBSUB redis_main CHANNEL dns_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER redis_out FROM notifications
        TO REDIS PUBSUB redis_main CHANNEL dns_out_{{test_id}}
        MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 1s ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Redis channel "dns_in_{{test_id}}" eventually has 1 subscribers
    When the DNS fixture answers "redis.nervix.test" with "<answer>"
    And the TCP forwarder at "127.0.5.1" stops
    Then Redis channel "dns_in_{{test_id}}" eventually has 0 subscribers
    When the DNS fixture answers "redis.nervix.test" with addresses "127.0.5.2"
    Then Redis channel "dns_in_{{test_id}}" eventually has 1 subscribers
    When Redis message is published to channel "dns_in_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the observed broker receives a payload
      """
      {"user_id":43}
      """
    And the TCP forwarder at "127.0.5.2" eventually accepts a connection
    When the cluster is restarted
    Then Redis channel "dns_in_{{test_id}}" eventually has 1 subscribers
    When Redis message is published to channel "dns_in_{{test_id}}"
      """
      {"user_id":44}
      """
    Then the observed broker receives a payload
      """
      {"user_id":44}
      """

    Examples:
      | cluster_size | answer         |
      | 1            | name not found |
      | 3            | name not found |
      | 1            | silence        |
      | 3            | silence        |
