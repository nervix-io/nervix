Feature: Runtime report chains
  Scenario Outline: A source cadence event preserves the clock arithmetic cause
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA cadence_record ( value I64 );
      CREATE WIRE JSON SCHEMA cadence_wire MODE STRICT ( value integer );
      CREATE CODEC cadence_codec FROM WIRE JSON SCHEMA cadence_wire TO SCHEMA cadence_record;
      CREATE RELAY cadence_records SCHEMA cadence_record UNBRANCHED;
      CREATE CLIENT cadence_http TYPE HTTP CONFIG {
        'endpoint' = 'http://127.0.0.1:1/events',
        'method' = 'GET'
      };
      CREATE INGESTOR cadence_reader
        FROM HTTP cadence_http EVERY 1d
        ON QUIESCE SUSPEND DECODE USING cadence_codec
        TIMESTAMP NOW
        TO cadence_records
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION cadence_subscription TO cadence_records;
      START AT '2262-04-11T23:00:00Z' TIME RATE 0.000001;
      """
    Then within "30s" the active session observes a server error containing
      """
      could not advance source cadence:
      """
    And the last server error contains
      """
      clock arithmetic failed during cadence scheduling:
      """
    And the last server error contains
      """
      timestamp arithmetic leaves the signed Unix-nanosecond range
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
