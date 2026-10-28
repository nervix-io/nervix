Feature: MongoDB collection declarations
  Stopped declarations validate names and driver configuration without connecting to MongoDB.

  Scenario Outline: MongoDB refuses NUL bytes in VALUES keys
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MONGODB CLIENT mongo_client POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017/events'
      };
      """
    And a NUL character placeholder "nul" is prepared
    When these Plexus commands fail with "NUL"
      """
      CREATE MONGODB COLLECTION EMITTER output FROM events
        TO mongo_client COLLECTION 'events' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'bad{{nul}}key' = input.value } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: MongoDB refuses NUL bytes when altering VALUES keys
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MONGODB CLIENT mongo_client POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017/events'
      };
      CREATE MONGODB COLLECTION EMITTER output FROM events
        TO mongo_client COLLECTION 'events' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'good.key' = input.value } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And a NUL character placeholder "nul" is prepared
    When these Plexus commands fail with "NUL"
      """
      ALTER MONGODB COLLECTION EMITTER output
        SET TO mongo_client COLLECTION 'events' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'bad{{nul}}key' = input.value };
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER output;
      """
    Then the last command output contains
      """
      'good.key' = input.value
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: MongoDB collection locators preserve external spelling
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MONGODB CLIENT mongo_client POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017/events'
      };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE MONGODB COLLECTION EMITTER output FROM events
        TO mongo_client COLLECTION 'Orders café 2026' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'value' = input.value } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER output;
      """
    Then the last command output contains
      """
      COLLECTION 'Orders café 2026'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: MongoDB validates driver options before committing a client
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      CREATE MONGODB CLIENT mongo_client POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017/events?<option>'
      };
      """

    Examples:
      | cluster_size | option          | diagnostic                       |
      | 1            | maxConnecting=0 | invalid client config key 'addr' |
      | 3            | maxConnecting=0 | invalid client config key 'addr' |
      | 1            | maxPoolSize=2   | POOL SIZE                        |
      | 3            | minPoolSize=1   | POOL SIZE                        |

  Scenario Outline: MongoDB refuses declarations outside its collection contract
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MONGODB CLIENT mongo_client POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017/events'
      };
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      CREATE MONGODB COLLECTION EMITTER output FROM events
        TO mongo_client COLLECTION '<collection>' MODE <mode>
        VALUES { 'value' = input.value <extra_value> } <construction>
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | collection | mode                                  | extra_value             | construction                       | diagnostic   |
      | 1            | bad$name   | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         |                                    | $            |
      | 3            | bad$name   | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         |                                    | $            |
      | 1            | Orders     | NO ACK                                |                         |                                    | ACK          |
      | 3            | Orders     | NO ACK                                |                         |                                    | ACK          |
      | 1            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s | , 'value' = input.value |                                    | duplicate    |
      | 3            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s | , 'value' = input.value |                                    | duplicate    |
      | 1            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         | INVOKE write_header('x', 'value')  | INVOKE       |
      | 3            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         | INVOKE write_header('x', 'value')  | INVOKE       |
      | 1            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         | ON CONFLICT DO NOTHING             | DO           |
      | 3            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         | ON CONFLICT DO NOTHING             | DO           |
      | 1            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         | ON CONFLICT ('missing') DO NOTHING | not mapped   |
      | 3            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         | ON CONFLICT ('missing') DO NOTHING | not mapped   |
      | 1            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         | ON CONFLICT ('value') DO UPDATE    | non-conflict |
      | 3            | Orders     | ACK RETRY POLICY BACKOFF 100ms MAX 1s |                         | ON CONFLICT ('value') DO UPDATE    | non-conflict |
