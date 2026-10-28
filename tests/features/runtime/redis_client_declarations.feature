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

  Scenario Outline: Redis client declarations refuse invalid command pool lifetimes at CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE REDIS CLIENT refused_client POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1', <config>
      };
      """
    Then the last command error contains
      """
      <error>
      """

    Examples:
      | cluster_size | config                          | error                                                                                               |
      | 1            | 'pool_max_lifetime_ms' = '0'    | invalid client config key 'pool_max_lifetime_ms': expected positive, schedulable whole milliseconds |
      | 3            | 'pool_max_lifetime_ms' = '0'    | invalid client config key 'pool_max_lifetime_ms': expected positive, schedulable whole milliseconds |
      | 1            | 'pool_idle_timeout_ms' = '1.5s' | invalid client config key 'pool_idle_timeout_ms': expected positive, schedulable whole milliseconds |
      | 3            | 'pool_idle_timeout_ms' = '1.5s' | invalid client config key 'pool_idle_timeout_ms': expected positive, schedulable whole milliseconds |
      | 1            | 'pool_max_lifetime' = '1000'    | unknown client config key 'pool_max_lifetime'                                                       |
      | 3            | 'pool_max_lifetime' = '1000'    | unknown client config key 'pool_max_lifetime'                                                       |

  Scenario Outline: Redis client inspection reports its default command pool lifetimes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE REDIS CLIENT checked POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1'
      };
      """
    When these Plexus commands are executed
      """
      DESCRIBE CLIENT checked;
      """
    Then the last command output contains
      """
      pool_idle_timeout_ms: 600000 (default)
      pool_max_lifetime_ms: 1800000 (default)
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Redis client inspection reports declared command pool lifetimes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE REDIS CLIENT checked POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1',
        'pool_idle_timeout_ms' = '45000', 'pool_max_lifetime_ms' = '90000'
      };
      DESCRIBE CLIENT checked;
      """
    Then the last command output contains
      """
      pool_idle_timeout_ms: 45000
      pool_max_lifetime_ms: 90000
      """
    And the last command output does not contain
      """
      (default)
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
