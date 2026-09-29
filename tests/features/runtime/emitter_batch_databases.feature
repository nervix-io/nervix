Feature: Database emitter batches
  ClickHouse, Postgres, MySQL and MongoDB write each batch as one insert or bulk write that carries
  its members as rows or documents. MAX MESSAGES bounds the rows of every write, MAX SIZE bounds the
  exact bytes of what the write carries, and one write may take rows from successive Arrow carriers
  of one source relay and branch. The destination's own limits bound every write as well, and a row
  the destination or MAX SIZE rejects never costs the rest of its batch its write.

  @emitter_batch_databases @database_batch_messages
  Scenario Outline: A <sink> write carries at most MAX MESSAGES rows taken across successive carriers
    Given <sink> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<sink>" batch table "batched_{{test_id}}" recording its writes exists
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING, tags <tags_type>, raw STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge databases-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      <client>
      CREATE EMITTER batched FROM events
        TO <target> batched_{{test_id}}
        VALUES {
          "seq" = input.seq,
          "note" = input.note,
          "tags" = input.tags,
          "pair" = array(input.seq, input.seq * 10),
          "raw" = hex_decode(input.raw)
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 3 MAX SIZE 1MiB
        FLUSH EACH 20s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    # Four requests are four Arrow carriers of one relay; the flush that releases them packs rows
    # across them, three to a write.
    And http payload is posted to host "databases-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"one","tags":[[1]],"raw":"01"},{"seq":2,"note":"two","tags":[[2],[20,21]],"raw":"00ff"}]
      """
    And http payload is posted to host "databases-{{test_id}}.example.com" path "/events"
      """
      [{"seq":3,"note":"three","tags":[],"raw":""}]
      """
    And http payload is posted to host "databases-{{test_id}}.example.com" path "/events"
      """
      [{"seq":4,"note":"four","tags":[[4,40]],"raw":"7f"},{"seq":5,"note":"five","tags":[[5],[]],"raw":"80"},{"seq":6,"note":"six","tags":[[6]],"raw":"0a0d22"}]
      """
    And http payload is posted to host "databases-{{test_id}}.example.com" path "/events"
      """
      [{"seq":7,"note":"seven","tags":[[7]],"raw":"5c"}]
      """
    Then within "45s" the "<sink>" batch table "batched_{{test_id}}" records writes of "3, 3, 1" rows
    And the "<sink>" batch table "batched_{{test_id}}" eventually holds exactly these rows
      """
      {"seq":1,"note":"one","tags":[[1]],"pair":[1,10],"raw":"01"}
      {"seq":2,"note":"two","tags":[[2],[20,21]],"pair":[2,20],"raw":"00ff"}
      {"seq":3,"note":"three","tags":[],"pair":[3,30],"raw":""}
      {"seq":4,"note":"four","tags":[[4,40]],"pair":[4,40],"raw":"7f"}
      {"seq":5,"note":"five","tags":[[5],[]],"pair":[5,50],"raw":"80"}
      {"seq":6,"note":"six","tags":[[6]],"pair":[6,60],"raw":"0a0d22"}
      {"seq":7,"note":"seven","tags":[[7]],"pair":[7,70],"raw":"5c"}
      """

    Examples:
      | cluster_size | sink       | tags_type     | client                                                                                                                 | target                          |
      | 1            | ClickHouse | VEC<VEC<I64>> | CREATE CLIENT db TYPE CLICKHOUSE CONFIG { 'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix' }; | CLICKHOUSE db INSERT TO TABLE   |
      | 3            | ClickHouse | VEC<VEC<I64>> | CREATE CLIENT db TYPE CLICKHOUSE CONFIG { 'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix' }; | CLICKHOUSE db INSERT TO TABLE   |
      | 1            | Postgres   | VEC<VEC<I64>> | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                          | POSTGRES db INSERT TO TABLE     |
      | 3            | Postgres   | VEC<VEC<I64>> | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                          | POSTGRES db INSERT TO TABLE     |
      | 1            | MySQL      | VEC<VEC<I64>> | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                                | MYSQL db INSERT TO TABLE        |
      | 3            | MySQL      | VEC<VEC<I64>> | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                                | MYSQL db INSERT TO TABLE        |
      | 1            | MongoDB    | VEC<VEC<I64>> | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' };     | MONGODB db INSERT TO COLLECTION |
      | 3            | MongoDB    | VEC<VEC<I64>> | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' };     | MONGODB db INSERT TO COLLECTION |

  @emitter_batch_databases @database_batch_size
  Scenario Outline: MAX SIZE bounds the exact bytes of every <sink> write
    Given <sink> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<sink>" batch table "limit_a_{{test_id}}" recording its writes exists
    And the "<sink>" batch table "limit_b_{{test_id}}" recording its writes exists
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge database-sizes-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      <client>
      CREATE EMITTER at_limit FROM events
        TO <target> limit_a_{{test_id}}
        VALUES { "seq" = input.seq, "note" = input.note }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 3 MAX SIZE <three_rows>
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER below_limit FROM events
        TO <target> limit_b_{{test_id}}
        VALUES { "seq" = input.seq, "note" = input.note }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 3 MAX SIZE <one_byte_less>
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    # Every row measures the same, so a write of three rows measures exactly <three_rows>. It fits
    # the first emitter's limit. It is one byte over the second's, which halves each candidate of
    # three into a write of two and returns the third row to the front of the next candidate.
    And http payload is posted to host "database-sizes-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"abc"},{"seq":2,"note":"abc"},{"seq":3,"note":"abc"},{"seq":4,"note":"abc"},{"seq":5,"note":"abc"},{"seq":6,"note":"abc"},{"seq":7,"note":"abc"}]
      """
    Then within "30s" the "<sink>" batch table "limit_a_{{test_id}}" records writes of "3, 3, 1" rows
    And within "30s" the "<sink>" batch table "limit_b_{{test_id}}" records writes of "2, 2, 2, 1" rows
    And the "<sink>" batch table "limit_b_{{test_id}}" eventually holds exactly these rows
      """
      {"seq":1,"note":"abc"}
      {"seq":2,"note":"abc"}
      {"seq":3,"note":"abc"}
      {"seq":4,"note":"abc"}
      {"seq":5,"note":"abc"}
      {"seq":6,"note":"abc"}
      {"seq":7,"note":"abc"}
      """

    Examples:
      | cluster_size | sink       | three_rows | one_byte_less | client                                                                                                                 | target                          |
      | 1            | ClickHouse | 69B        | 68B           | CREATE CLIENT db TYPE CLICKHOUSE CONFIG { 'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix' }; | CLICKHOUSE db INSERT TO TABLE   |
      | 3            | ClickHouse | 69B        | 68B           | CREATE CLIENT db TYPE CLICKHOUSE CONFIG { 'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix' }; | CLICKHOUSE db INSERT TO TABLE   |
      | 1            | Postgres   | 242B       | 241B          | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                          | POSTGRES db INSERT TO TABLE     |
      | 3            | Postgres   | 242B       | 241B          | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                          | POSTGRES db INSERT TO TABLE     |
      | 1            | MySQL      | 137B       | 136B          | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                                | MYSQL db INSERT TO TABLE        |
      | 3            | MySQL      | 137B       | 136B          | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                                | MYSQL db INSERT TO TABLE        |
      | 1            | MongoDB    | 147B       | 146B          | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' };     | MONGODB db INSERT TO COLLECTION |
      | 3            | MongoDB    | 147B       | 146B          | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' };     | MONGODB db INSERT TO COLLECTION |

  @emitter_batch_databases @database_batch_rejections
  Scenario Outline: Rows <sink> or MAX SIZE rejects follow ON MESSAGE ERROR and the rest of their batch is written
    Given <sink> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<sink>" batch table "rejecting_{{test_id}}" rejecting poison notes exists
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64, error_code STRING, error_operation STRING, error_message STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge database-rejections-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      <client>
      CREATE EMITTER batched FROM events
        TO <target> rejecting_{{test_id}}
        VALUES {
          "seq" = input.seq,
          "note" = CASE WHEN input.note = 'huge' THEN repeat('x', 1000) ELSE input.note END
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 10 MAX SIZE 600B
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              error_operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    # The four rows do not fit 600 bytes together, so the write is halved. The first half fails on
    # the poison row and is written again row by row; the huge row alone is still over 600 bytes.
    And http payload is posted to host "database-rejections-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"healthy_a"},{"seq":2,"note":"poison"},{"seq":3,"note":"huge"},{"seq":4,"note":"healthy_b"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":2 | "error_code":"external" | "error_operation":"publish"
      "seq":3 | "error_code":"validation" | "error_operation":"encode" | MAX SIZE 600B
      """
    And the "<sink>" batch table "rejecting_{{test_id}}" eventually holds exactly these rows
      """
      {"seq":1,"note":"healthy_a"}
      {"seq":4,"note":"healthy_b"}
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | sink       | client                                                                                                                 | target                          |
      | 1            | ClickHouse | CREATE CLIENT db TYPE CLICKHOUSE CONFIG { 'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix' }; | CLICKHOUSE db INSERT TO TABLE   |
      | 3            | ClickHouse | CREATE CLIENT db TYPE CLICKHOUSE CONFIG { 'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix' }; | CLICKHOUSE db INSERT TO TABLE   |
      | 1            | Postgres   | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                          | POSTGRES db INSERT TO TABLE     |
      | 3            | Postgres   | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                          | POSTGRES db INSERT TO TABLE     |
      | 1            | MySQL      | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                                | MYSQL db INSERT TO TABLE        |
      | 3            | MySQL      | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                                | MYSQL db INSERT TO TABLE        |
      | 1            | MongoDB    | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' };     | MONGODB db INSERT TO COLLECTION |
      | 3            | MongoDB    | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' };     | MONGODB db INSERT TO COLLECTION |

  @emitter_batch_databases @database_batch_conflicts
  Scenario Outline: A <sink> conflict policy resolves a key repeated inside one batch
    Given <sink> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<sink>" batch table "updated_{{test_id}}" keyed by seq exists
    And the "<sink>" batch table "kept_{{test_id}}" keyed by seq exists
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge database-conflicts-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      <client>
      CREATE EMITTER updating FROM events
        TO <target> updated_{{test_id}}
        VALUES { "seq" = input.seq, "note" = input.note }
        <update_policy>
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER keeping FROM events
        TO <target> kept_{{test_id}}
        VALUES { "seq" = input.seq, "note" = input.note }
        <keep_policy>
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    # One batch carries both rows for key 1: DO UPDATE leaves the later one, DO NOTHING the first.
    And http payload is posted to host "database-conflicts-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"first"},{"seq":1,"note":"second"},{"seq":2,"note":"only"}]
      """
    Then the "<sink>" batch table "updated_{{test_id}}" eventually holds exactly these rows
      """
      {"seq":1,"note":"second"}
      {"seq":2,"note":"only"}
      """
    And the "<sink>" batch table "kept_{{test_id}}" eventually holds exactly these rows
      """
      {"seq":1,"note":"first"}
      {"seq":2,"note":"only"}
      """

    Examples:
      | cluster_size | sink     | update_policy                 | keep_policy                    | client                                                                                                             | target                          |
      | 1            | Postgres | ON CONFLICT ("seq") DO UPDATE | ON CONFLICT ("seq") DO NOTHING | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                      | POSTGRES db INSERT TO TABLE     |
      | 3            | Postgres | ON CONFLICT ("seq") DO UPDATE | ON CONFLICT ("seq") DO NOTHING | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                      | POSTGRES db INSERT TO TABLE     |
      | 1            | MySQL    | ON CONFLICT DO UPDATE         | ON CONFLICT DO NOTHING         | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                            | MYSQL db INSERT TO TABLE        |
      | 3            | MySQL    | ON CONFLICT DO UPDATE         | ON CONFLICT DO NOTHING         | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                            | MYSQL db INSERT TO TABLE        |
      | 1            | MongoDB  | ON CONFLICT ("seq") DO UPDATE | ON CONFLICT ("seq") DO NOTHING | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' }; | MONGODB db INSERT TO COLLECTION |
      | 3            | MongoDB  | ON CONFLICT ("seq") DO UPDATE | ON CONFLICT ("seq") DO NOTHING | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' }; | MONGODB db INSERT TO COLLECTION |

  @emitter_batch_databases @database_batch_retries
  Scenario Outline: An ambiguous <sink> write is retried with exactly the rows it carried
    Given <sink> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<sink>" batch table "retried_{{test_id}}" recording its writes exists
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge database-retries-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      <client>
      CREATE EMITTER batched FROM events
        TO <target> retried_{{test_id}}
        VALUES { "seq" = input.seq, "note" = input.note }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 3 MAX SIZE 64KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    # The destination takes all three writes, but the emitter learns only that the first carried
    # its rows. The rows of the other two are packed again into the same two writes.
    When the sink of emitter "batched" stalls after resolving 3 records of its next publish
    And http payload is posted to host "database-retries-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"c"},{"seq":4,"note":"d"},{"seq":5,"note":"e"},{"seq":6,"note":"f"},{"seq":7,"note":"g"}]
      """
    Then within "30s" the "<sink>" batch table "retried_{{test_id}}" records writes of "3, 3, 1, 3, 1" rows
    And the "<sink>" batch table "retried_{{test_id}}" eventually holds exactly these rows
      """
      {"seq":1,"note":"a"}
      {"seq":2,"note":"b"}
      {"seq":3,"note":"c"}
      {"seq":4,"note":"d"}
      {"seq":4,"note":"d"}
      {"seq":5,"note":"e"}
      {"seq":5,"note":"e"}
      {"seq":6,"note":"f"}
      {"seq":6,"note":"f"}
      {"seq":7,"note":"g"}
      {"seq":7,"note":"g"}
      """

    Examples:
      | cluster_size | sink       | client                                                                                                                 | target                          |
      | 1            | ClickHouse | CREATE CLIENT db TYPE CLICKHOUSE CONFIG { 'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix' }; | CLICKHOUSE db INSERT TO TABLE   |
      | 3            | ClickHouse | CREATE CLIENT db TYPE CLICKHOUSE CONFIG { 'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix' }; | CLICKHOUSE db INSERT TO TABLE   |
      | 1            | Postgres   | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                          | POSTGRES db INSERT TO TABLE     |
      | 3            | Postgres   | CREATE CLIENT db TYPE POSTGRES POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{postgres_addr}}' };                          | POSTGRES db INSERT TO TABLE     |
      | 1            | MySQL      | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                                | MYSQL db INSERT TO TABLE        |
      | 3            | MySQL      | CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };                                | MYSQL db INSERT TO TABLE        |
      | 1            | MongoDB    | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' };     | MONGODB db INSERT TO COLLECTION |
      | 3            | MongoDB    | CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mongodb_addr}}', 'database' = 'nervix' };     | MONGODB db INSERT TO COLLECTION |

  @emitter_batch_databases @mysql_batch_placeholders
  Scenario Outline: A MySQL write never binds more placeholders than one statement accepts
    Given MySQL is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "MySQL" batch table "placeholders_{{test_id}}" recording its writes exists
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION 'range(.count) | {seq: ., note: "n"}';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge mysql-placeholders-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT db TYPE MYSQL POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{mysql_addr}}' };
      CREATE EMITTER batched FROM events
        TO MYSQL db INSERT TO TABLE placeholders_{{test_id}}
        VALUES {
          "seq" = input.seq,
          "note" = input.note,
          "tags" = array(array(input.seq)),
          "pair" = array(input.seq, input.seq),
          "raw" = hex_decode('00')
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 20000 MAX SIZE 16MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    # Five mapped columns bind five placeholders a row, and one statement binds at most 65,535, so
    # a write carries at most 13,107 rows however many MAX MESSAGES allows.
    And http payload is posted to host "mysql-placeholders-{{test_id}}.example.com" path "/events"
      """
      {"count":20000}
      """
    Then within "60s" the "MySQL" batch table "placeholders_{{test_id}}" records writes of "13107, 6893" rows
    And the "MySQL" batch table "placeholders_{{test_id}}" eventually holds 20000 rows

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_databases @mongodb_batch_document_limit
  Scenario Outline: A MongoDB document above the server's document limit is rejected before its write
    Given MongoDB is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "MongoDB" batch table "documents_{{test_id}}" recording its writes exists
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64, error_code STRING, error_operation STRING, error_message STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge mongodb-documents-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT db TYPE MONGODB POOL SIZE MIN 1 MAX 4 CONFIG {
        'addr' = '{{mongodb_addr}}', 'database' = 'nervix'
      };
      CREATE EMITTER batched FROM events
        TO MONGODB db INSERT TO COLLECTION documents_{{test_id}}
        VALUES {
          "seq" = input.seq,
          "note" = CASE WHEN input.note = 'huge' THEN repeat('x', 17000000) ELSE input.note END
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 10 MAX SIZE 64MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              error_operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    # The huge row fits MAX SIZE but no MongoDB document may exceed 16 MiB, so it never reaches the
    # write that carries the rows around it.
    And http payload is posted to host "mongodb-documents-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"healthy_a"},{"seq":2,"note":"huge"},{"seq":3,"note":"healthy_b"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":2 | "error_code":"external" | "error_operation":"publish" | 16777216
      """
    And within "30s" the "MongoDB" batch table "documents_{{test_id}}" records writes of "2" rows
    And the "MongoDB" batch table "documents_{{test_id}}" eventually holds exactly these rows
      """
      {"seq":1,"note":"healthy_a"}
      {"seq":3,"note":"healthy_b"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
