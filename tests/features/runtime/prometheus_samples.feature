Feature: Prometheus samples
  # A PROMETHEUS QUERY ingestor delivers each sample as the object the query API holds it in. The
  # pinned Prometheus image answers every shape a query can ask of it. The mock endpoint answers the
  # two a scenario cannot ask for: a native histogram, which takes a scrape of one, and the page of a
  # proxy in front of the API.

  @prometheus_sample_labels
  Scenario Outline: A sample's labels stay under metric whatever they are named
    Given Prometheus is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample (
        value_label STRING,
        timestamp_label STRING,
        value F64,
        timestamp STRING
      );
      CREATE CODEC sample_codec FROM JSON TO SCHEMA sample
        WITH JAQ TRANSFORMATIONS ON INGESTION
        '{value_label: .metric.value, timestamp_label: .metric.timestamp, value, timestamp}';
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{prometheus_addr}}',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main
        QUERY 'label_replace(label_replace(vector(42.5), "value", "label-value", "", ""), "timestamp", "label-time", "", "")'
        EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION samples_subscription TO samples;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      "timestamp_label":"label-time","value":42.5,"value_label":"label-value"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @prometheus_sample_values
  Scenario Outline: A <written> sample reaches its codec as that string, which a number field refuses
    Given Prometheus is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA text_sample ( value STRING );
      CREATE WIRE JSON SCHEMA text_sample_wire MODE LOOSE ( value string );
      CREATE CODEC text_sample_codec FROM WIRE JSON SCHEMA text_sample_wire TO SCHEMA text_sample;
      CREATE SCHEMA number_sample ( value F64 );
      CREATE WIRE JSON SCHEMA number_sample_wire MODE LOOSE ( value number );
      CREATE CODEC number_sample_codec FROM WIRE JSON SCHEMA number_sample_wire TO SCHEMA number_sample;
      CREATE RELAY text_samples SCHEMA text_sample UNBRANCHED;
      CREATE RELAY number_samples SCHEMA number_sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{prometheus_addr}}',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR as_text
        FROM prom_main QUERY '<query>' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING text_sample_codec
        TO text_samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE PROMETHEUS QUERY INGESTOR as_number
        FROM prom_main QUERY '<query>' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING number_sample_codec
        TO number_samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION text_subscription TO text_samples;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      "value":"<written>"
      """
    And within "30s" the active session observes a server error containing
      """
      ingestor 'as_number' general error in domain '{{domain}}': decode rejection
      """
    And the last server error contains
      """
      codec 'number_sample_codec' refused the payload: codec 'number_sample_codec' failed to parse field 'value'
      """

    Examples:
      | cluster_size | query        | written |
      | 1            | vector(0/0)  | NaN     |
      | 1            | vector(1/0)  | +Inf    |
      | 1            | vector(-1/0) | -Inf    |
      | 3            | vector(0/0)  | NaN     |
      | 3            | vector(1/0)  | +Inf    |
      | 3            | vector(-1/0) | -Inf    |

  @prometheus_sample_values
  Scenario Outline: A sample its codec refuses leaves the samples beside it delivered
    Given Prometheus is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample ( value F64 );
      CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{prometheus_addr}}',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main
        QUERY 'vector(0/0) or label_replace(vector(2.5), "series", "finite", "", "")'
        EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION samples_subscription TO samples;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      "value":2.5
      """
    And within "30s" the active session observes a server error containing
      """
      ingestor 'prom_samples' general error in domain '{{domain}}': decode rejection
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @prometheus_unsupported_results
  Scenario Outline: A <result_type> answer fails its poll and names its type
    Given Prometheus is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample ( value F64 );
      CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{prometheus_addr}}',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY '<query>' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: failed to read from prometheus ingestor: Prometheus query returned unsupported result type '<result_type>'
      """

    Examples:
      | cluster_size | query             | result_type |
      | 1            | vector(1)[1m:15s] | matrix      |
      | 1            | 1 + 1             | scalar      |
      | 1            | "text"            | string      |
      | 3            | vector(1)[1m:15s] | matrix      |
      | 3            | 1 + 1             | scalar      |
      | 3            | "text"            | string      |

  @prometheus_unsupported_results
  Scenario Outline: One native histogram sample fails its whole poll and names itself
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample ( value F64 );
      CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{mock_http_addr}}/prometheus-answer/{{test_id}}/native-histogram',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'histograms' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: failed to read from prometheus ingestor: Prometheus query returned unsupported result type 'native histogram'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @prometheus_rejected_queries
  Scenario Outline: A query the API refuses reports the API's own errorType and error
    Given Prometheus is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample ( value F64 );
      CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{prometheus_addr}}',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'sum(' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: failed to read from prometheus ingestor: Prometheus rejected the query with HTTP status 400 Bad Request: bad_data: invalid parameter "query": 1:5: parse error: unclosed left parenthesis
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @prometheus_rejected_queries
  Scenario Outline: A proxy's page in front of the API reports its HTTP status
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample ( value F64 );
      CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{mock_http_addr}}/prometheus-answer/{{test_id}}/proxy-page',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'up' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: failed to read from prometheus ingestor: Prometheus query failed with HTTP status 503 Service Unavailable
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @prometheus_request_redaction
  Scenario Outline: A failed query's status names no part of its request URL
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample ( value F64 );
      CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'http://reader:not-a-real-secret@127.0.0.1:9/behind-a-proxy',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'up' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: failed to read from prometheus ingestor: failed to send Prometheus query
      """
    And the last command output does not contain
      """
      api/v1/query
      """
    And the last command output does not contain
      """
      not-a-real-secret
      """
    And the last command output does not contain
      """
      behind-a-proxy
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @prometheus_query_metadata
  Scenario Outline: A route reads the query a sample answers and the instant its poll evaluated it at
    Given Prometheus is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 1s;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample ( value F64 );
      CREATE SCHEMA evaluated_sample ( value F64, query STRING OPTIONAL, evaluated_at DATETIME OPTIONAL );
      CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA evaluated_sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{prometheus_addr}}',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'vector(7)' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TIMESTAMP NOW
        TO samples INHERIT ALL
        SET query = metadata.query, evaluated_at = metadata.evaluated_at
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION samples_subscription TO samples;
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    Then within "60s" the relay subscription receives a payload
      """
      "evaluated_at":"2000-01-01T00:
      """
    And the relay subscription receives a payload
      """
      "query":"vector(7)","value":7.0
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A window as wide as the cadence holds one sample each: the one its poll evaluated in that second
  # of domain time, however late the answer arrived.
  @prometheus_query_metadata
  Scenario Outline: A window takes each sample's event time from metadata.evaluated_at
    Given Prometheus is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 1s;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample ( value F64 );
      CREATE SCHEMA evaluated_sample ( value F64, evaluated_at DATETIME OPTIONAL );
      CREATE SCHEMA summary ( samples I64, evaluated_at DATETIME OPTIONAL );
      CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA evaluated_sample UNBRANCHED;
      CREATE RELAY summaries SCHEMA summary UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{prometheus_addr}}',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'vector(7)' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TIMESTAMP AT metadata.evaluated_at
        TO samples INHERIT ALL SET evaluated_at = metadata.evaluated_at
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR per_second FROM samples
        WIDTH 1s DURATION STEP 1s DURATION UNBRANCHED
        TO summaries SET samples = COUNT(input.value), evaluated_at = FIRST(input.evaluated_at)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION summaries_subscription TO summaries;
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    Then within "60s" the relay subscription receives a payload
      """
      "evaluated_at":"2000-01-01T00:
      """
    And the relay subscription receives a payload
      """
      "samples":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
