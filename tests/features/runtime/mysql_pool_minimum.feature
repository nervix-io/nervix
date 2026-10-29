Feature: MySQL pool minimum
  A MySQL client's pool keeps POOL SIZE MIN established connections in total while it has local
  users, counting the ones its emitters have checked out together with the idle ones, and an idle
  pool that holds them opens no other. A stallable endpoint in front of MySQL counts this client's
  connections, which the shared MySQL server cannot tell apart from other scenarios'.

  Scenario Outline: An idle MySQL client keeps POOL SIZE MIN <minimum> connections established
    Given MySQL is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "Pool_minimum_{{test_id}}" exists
    And a stallable endpoint "mysql_counted" stands in front of "mysql_addr"
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MYSQL CLIENT mysql_main POOL SIZE MIN <minimum> MAX 4 CONFIG {
        'addr' = '{{mysql_counted}}'
      };
      CREATE MYSQL TABLE EMITTER to_mysql FROM notifications
        TO mysql_main TABLE 'nervix.Pool_minimum_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        VALUES { "mysql_user_id" = input.user_id, "mysql_action" = input.action }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the stallable endpoint "mysql_counted" eventually accepts <minimum> connections
    And the stallable endpoint "mysql_counted" accepts no connection beyond <minimum> within "3s"

    Examples:
      | cluster_size | minimum |
      | 1            | 2       |
      | 3            | 2       |
      | 1            | 3       |
      | 3            | 3       |
