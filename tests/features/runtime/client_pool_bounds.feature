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
      CREATE CLIENT postgres_main
        TYPE POSTGRES
        POOL SIZE MIN 2 MAX 8
        CONFIG {
          'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable'
        };
      CREATE CLIENT mysql_main
        TYPE MYSQL
        POOL SIZE MIN 0 MAX 4
        CONFIG {
          'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
        };
      CREATE CLIENT mongodb_main
        TYPE MONGODB
        POOL SIZE MIN 3 MAX 3
        CONFIG {
          'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
          'database' = 'nervix'
        };
      CREATE CLIENT redis_main
        TYPE REDIS
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
      CREATE CLIENT postgres_main
        TYPE POSTGRES
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
      CREATE CLIENT mysql_main
        TYPE MYSQL
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
      CREATE CLIENT mongodb_main
        TYPE MONGODB
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
      CREATE CLIENT redis_main
        TYPE REDIS
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
      CREATE CLIENT no_bounds TYPE POSTGRES
        CONFIG { 'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable' };
      """
    And these NSPL commands fail with "expected MAX"
      """
      CREATE CLIENT minimum_only TYPE MYSQL POOL SIZE MIN 2
        CONFIG { 'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix' };
      """
    And these NSPL commands fail with "expected MIN"
      """
      CREATE CLIENT reversed TYPE MONGODB POOL SIZE MAX 8 MIN 2
        CONFIG { 'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix', 'database' = 'nervix' };
      """
    And these NSPL commands fail with "expected POOL SIZE"
      """
      CREATE CLIENT bare_bounds TYPE MYSQL MIN 2 MAX 8
        CONFIG { 'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix' };
      """
    And these NSPL commands fail with "expected MAX, found MIN"
      """
      CREATE CLIENT repeated_minimum TYPE REDIS POOL SIZE MIN 1 MIN 2 MAX 4
        CONFIG { 'addr' = 'redis://127.0.0.1:6379/' };
      """
    And these NSPL commands fail with "expected MOUNT | CONFIG, found POOL"
      """
      CREATE CLIENT repeated_clause TYPE REDIS POOL SIZE MIN 1 MAX 4 POOL SIZE MIN 2 MAX 8
        CONFIG { 'addr' = 'redis://127.0.0.1:6379/' };
      """
    And these NSPL commands fail with "expected POOL SIZE, found MOUNT"
      """
      CREATE CLIENT mount_first TYPE REDIS MOUNT dev_tls POOL SIZE MIN 1 MAX 4
        CONFIG { 'addr' = 'rediss://127.0.0.1:6380/' };
      """
    And these NSPL commands fail with "maximum pool size must be greater than zero"
      """
      CREATE CLIENT zero_maximum TYPE REDIS POOL SIZE MIN 0 MAX 0
        CONFIG { 'addr' = 'redis://127.0.0.1:6379/' };
      """
    And these NSPL commands fail with "minimum pool size 9 must not exceed maximum pool size 8"
      """
      CREATE CLIENT inverted TYPE POSTGRES POOL SIZE MIN 9 MAX 8
        CONFIG { 'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable' };
      """
    And these NSPL commands fail with "invalid integer '4294967296'; expected 0 through 4294967295"
      """
      CREATE CLIENT above_range TYPE MYSQL POOL SIZE MIN 2 MAX 4294967296
        CONFIG { 'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix' };
      """
    And these NSPL commands fail with "invalid integer '1.5'"
      """
      CREATE CLIENT fractional TYPE MONGODB POOL SIZE MIN 1.5 MAX 8
        CONFIG { 'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix', 'database' = 'nervix' };
      """
    And these NSPL commands fail with "expected MOUNT | CONFIG, found POOL"
      """
      CREATE CLIENT kafka_bounds TYPE KAFKA POOL SIZE MIN 2 MAX 8
        CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      """
    And these NSPL commands are executed
      """
      CREATE CLIENT widest_bounds TYPE POSTGRES POOL SIZE MIN 4294967295 MAX 4294967295
        CONFIG { 'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable' };
      """
    And these NSPL commands are executed
      """
      SHOW CREATE CLIENT widest_bounds;
      """
    Then the last command output contains
      """
      CREATE CLIENT widest_bounds
        TYPE POSTGRES
        POOL SIZE MIN 4294967295 MAX 4294967295
        CONFIG {
          'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable'
        };
      """
