Feature: Domain clock attachment

  @retained_task_handles
  Scenario Outline: A session attaches to a domain clock once and follows every generation
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    And a clock session is opened on the <session_node> node
    When the clock session attaches to the clock of domain "{{domain}}"
    Then the clock session is attached at generation 1 to a paced clock with period "100ms", skew "10ms", logical origin "2030-01-01T00:00:00Z" and time rate "2"
    And within "10s" the clock session receives 3 increasing ticks for generation 1 with period "100ms" and time rate "2"
    When the clock session attaches to the clock of domain "{{domain}}"
    Then the clock session is refused because it already follows the clock of domain "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Then within "10s" the clock session observes domain "{{domain}}" stopped at generation 1
    When the domain clock is started at now with time rate "1.0" on the leader node
    Then within "10s" the clock session observes domain "{{domain}}" at generation 2 as the paced clock that start established with period "100ms" and skew "10ms"
    And within "10s" the clock session receives a tick for generation 2 after its state frame
    When the clock session detaches from the clock of domain "{{domain}}"
    Then the clock session is detached from the clock of domain "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Then the clock session receives no frame about domain "{{domain}}" within "2s"
    When the clock session detaches from the clock of domain "{{domain}}"
    Then the clock session is refused because it does not follow the clock of domain "{{domain}}"
    When the clock session attaches to the clock of domain "{{domain}}"
    Then the clock session is attached at generation 2 to a stopped clock

    Examples:
      | cluster_size | session_node |
      | 1            | leader       |
      | 3            | follower     |

  Scenario Outline: A client paces simulated events by the domain clock it is attached to
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 100ms;
      CREATE SCHEMA event ( sequence I64, occurred_at DATETIME );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( sequence integer, occurred_at string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge attached-clock-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TIMESTAMP AT occurred_at
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    And client "driver" is connected to node "node-1"
    When client "driver" executes these NSPL commands
      """
      ATTACH DOMAIN CLOCK;
      """
    Then within "10s" client "driver" receives a tick for its attached domain clock
    When client "driver" posts event 1 to host "attached-clock-{{test_id}}.example.com" path "/events" at the newest tick center its attached clock admits
    Then within "10s" the relay subscription receives a payload
      """
      "sequence":1
      """
    When client "driver" posts event 2 to host "attached-clock-{{test_id}}.example.com" path "/events" one nanosecond past the skew after its attached clock's frontier
    Then within "10s" client "driver" observes a server error containing
      """
      outside any reached logical tick window
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
