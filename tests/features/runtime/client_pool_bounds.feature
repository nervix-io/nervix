Feature: Database client connection pool bounds
  Postgres, MySQL, MongoDB and Redis clients declare their connection capacity as part of the
  client itself, with `POOL SIZE MIN <n> MAX <n>` between the type and the optional mount.
  Both counts are structured client configuration rather than connector strings, so they survive
  persistence and are reported back by `SHOW CREATE CLIENT`.

  Scenario Outline: Pool-capable clients keep both declared bounds across a restart
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE POSTGRES CLIENT postgres_main
        POOL SIZE MIN 2 MAX 8
        CONFIG {
          'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable'
        };
      CREATE MYSQL CLIENT mysql_main
        POOL SIZE MIN 0 MAX 4
        CONFIG {
          'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
        };
      CREATE MONGODB CLIENT mongodb_main
        POOL SIZE MIN 3 MAX 3
        CONFIG {
          'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
          'database' = 'nervix'
        };
      CREATE REDIS CLIENT redis_main
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
      """
    And the cluster is restarted
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE CLIENT postgres_main;
      """
    Then the last command output contains
      """
      CREATE POSTGRES CLIENT postgres_main
        POOL SIZE MIN 2 MAX 8
        CONFIG {
          'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable'
        };
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CLIENT mysql_main;
      """
    Then the last command output contains
      """
      CREATE MYSQL CLIENT mysql_main
        POOL SIZE MIN 0 MAX 4
        CONFIG {
          'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
        };
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CLIENT mongodb_main;
      """
    Then the last command output contains
      """
      CREATE MONGODB CLIENT mongodb_main
        POOL SIZE MIN 3 MAX 3
        CONFIG {
          'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
          'database' = 'nervix'
        };
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CLIENT redis_main;
      """
    Then the last command output contains
      """
      CREATE REDIS CLIENT redis_main
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Pool bounds are checked where the client is declared
    Given a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "expected POOL SIZE"
      """
      CREATE POSTGRES CLIENT no_bounds
        CONFIG { 'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable' };
      """
    And these NSPL commands fail with "expected MAX"
      """
      CREATE MYSQL CLIENT minimum_only POOL SIZE MIN 2
        CONFIG { 'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix' };
      """
    And these NSPL commands fail with "expected MIN"
      """
      CREATE MONGODB CLIENT reversed POOL SIZE MAX 8 MIN 2
        CONFIG { 'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix', 'database' = 'nervix' };
      """
    And these NSPL commands fail with "expected POOL SIZE"
      """
      CREATE MYSQL CLIENT bare_bounds MIN 2 MAX 8
        CONFIG { 'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix' };
      """
    And these NSPL commands fail with "expected MAX, found MIN"
      """
      CREATE REDIS CLIENT repeated_minimum POOL SIZE MIN 1 MIN 2 MAX 4
        CONFIG { 'addr' = 'redis://127.0.0.1:6379/' };
      """
    And these NSPL commands fail with "expected MOUNT | CONFIG, found POOL"
      """
      CREATE REDIS CLIENT repeated_clause POOL SIZE MIN 1 MAX 4 POOL SIZE MIN 2 MAX 8
        CONFIG { 'addr' = 'redis://127.0.0.1:6379/' };
      """
    And these NSPL commands fail with "expected POOL SIZE, found MOUNT"
      """
      CREATE REDIS CLIENT mount_first MOUNT dev_tls VERSION 1 POOL SIZE MIN 1 MAX 4
        CONFIG { 'addr' = 'rediss://127.0.0.1:6380/' };
      """
    And these NSPL commands fail with "maximum pool size must be greater than zero"
      """
      CREATE REDIS CLIENT zero_maximum POOL SIZE MIN 0 MAX 0
        CONFIG { 'addr' = 'redis://127.0.0.1:6379/' };
      """
    And these NSPL commands fail with "minimum pool size 9 must not exceed maximum pool size 8"
      """
      CREATE POSTGRES CLIENT inverted POOL SIZE MIN 9 MAX 8
        CONFIG { 'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable' };
      """
    And these NSPL commands fail with "invalid integer '4294967296'; expected 0 through 4294967295"
      """
      CREATE MYSQL CLIENT above_range POOL SIZE MIN 2 MAX 4294967296
        CONFIG { 'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix' };
      """
    And these NSPL commands fail with "invalid integer '1.5'"
      """
      CREATE MONGODB CLIENT fractional POOL SIZE MIN 1.5 MAX 8
        CONFIG { 'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix', 'database' = 'nervix' };
      """
    And these NSPL commands fail with "expected MOUNT | CONFIG, found POOL"
      """
      CREATE KAFKA CLIENT kafka_bounds POOL SIZE MIN 2 MAX 8
        CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      """
    And these NSPL commands are executed
      """
      CREATE POSTGRES CLIENT widest_bounds POOL SIZE MIN 4294967295 MAX 4294967295
        CONFIG { 'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable' };
      """
    And these NSPL commands are executed
      """
      SHOW CREATE CLIENT widest_bounds;
      """
    Then the last command output contains
      """
      CREATE POSTGRES CLIENT widest_bounds
        POOL SIZE MIN 4294967295 MAX 4294967295
        CONFIG {
          'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable'
        };
      """
