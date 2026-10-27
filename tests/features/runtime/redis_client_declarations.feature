Feature: Redis client declarations
  Redis client keys are checked by the shared CONFIG schema before the domain runs.
  URL options remain owned by the Redis driver rather than a second Nervix key catalog.

  Scenario Outline: Redis client declarations refuse invalid configuration at CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE REDIS CLIENT refused_client POOL SIZE MIN 0 MAX 1 CONFIG {
        <config>
      };
      """
    Then the last command error contains
      """
      <key>
      """

    Examples:
      | cluster_size | config                                                                                                                             | key                          |
      | 1            | 'idle_timeout_ms' = '30000', 'subscription_buffer_messages' = '16'                                                                 | addr                         |
      | 3            | 'idle_timeout_ms' = '30000', 'subscription_buffer_messages' = '16'                                                                 | addr                         |
      | 1            | 'addr' = 'redis://127.0.0.1:1', 'idle_timeout_ms' = '30000', 'subscription_buffer_messages' = '16', 'adrr' = 'redis://127.0.0.1:1' | adrr                         |
      | 3            | 'addr' = 'redis://127.0.0.1:1', 'idle_timeout_ms' = '30000', 'subscription_buffer_messages' = '16', 'adrr' = 'redis://127.0.0.1:1' | adrr                         |
      | 1            | 'addr' = 'http://127.0.0.1:1', 'idle_timeout_ms' = '30000', 'subscription_buffer_messages' = '16'                                  | addr                         |
      | 3            | 'addr' = 'http://127.0.0.1:1', 'idle_timeout_ms' = '30000', 'subscription_buffer_messages' = '16'                                  | addr                         |
      | 1            | 'addr' = 'redis://127.0.0.1:1', 'idle_timeout_ms' = '30000', 'subscription_buffer_messages' = 'not-a-count'                        | subscription_buffer_messages |
      | 3            | 'addr' = 'redis://127.0.0.1:1', 'idle_timeout_ms' = '30000', 'subscription_buffer_messages' = 'not-a-count'                        | subscription_buffer_messages |
