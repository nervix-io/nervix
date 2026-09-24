Feature: Session subscription lifecycle

  Scenario Outline: A relay redefined under a subscription ends it before a row of its new definition reaches it
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64, secret STRING );
      CREATE SCHEMA secret_event ( id I64, secret STRING SENSITIVE );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer, secret string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge redefined-{{test_id}}.example.com;
      CREATE ENDPOINT event_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_ingestor
        FROM ENDPOINT event_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        SET id = message.id, secret = message.secret
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION watch TO events;
      """
    And http payload is posted to host "redefined-{{test_id}}.example.com" path "/events"
      """
      {"id":1,"secret":"public"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "secret":"public"
      """
    When this NSPL command request is executed on the leader node
      """
      ALTER RELAY events SET SCHEMA secret_event;
      """
    And http payload is posted to host "redefined-{{test_id}}.example.com" path "/events"
      """
      {"id":2,"secret":"leaked"}
      """
    Then within "30s" subscription "watch" of the active session ends because its relay was redefined
    And no row the active session received contains "leaked"
    And node "node-1" observability metric "nervix_session_subscriptions" with labels eventually equals 0
      """
      domain="{{domain}}"
      relay="events"
      """
    When these NSPL commands are executed on the active session
      """
      CREATE SUBSCRIPTION watch TO events;
      """
    And http payload is posted to host "redefined-{{test_id}}.example.com" path "/events"
      """
      {"id":3,"secret":"hidden"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "id":3
      """
    And the last relay subscription payload masks field "secret"
    And no row the active session received contains "hidden"
    And the active session received no frame about a subscription outside its lifetime

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Removing a relay ends its subscriptions, which stay deletable
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      START;
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION watch TO events;
      """
    Then node "node-1" observability metric "nervix_session_subscriptions" with labels eventually equals 1
      """
      domain="{{domain}}"
      relay="events"
      """
    When this NSPL command request is executed on the leader node
      """
      DROP RELAY events;
      """
    Then within "30s" subscription "watch" of the active session ends because its relay was removed
    And node "node-1" observability metric "nervix_session_subscriptions" with labels eventually equals 0
      """
      domain="{{domain}}"
      relay="events"
      """
    When these NSPL commands are executed on the active session
      """
      DELETE SUBSCRIPTION watch;
      """
    Then the last command output contains
      """
      deleted subscription 'watch' from domain '{{domain}}'
      """
    And the active session received no frame about a subscription outside its lifetime

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Stopping and starting a domain keeps its subscriptions delivering
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge restarted-{{test_id}}.example.com;
      CREATE ENDPOINT event_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_ingestor
        FROM ENDPOINT event_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION watch TO events;
      """
    And http payload is posted to host "restarted-{{test_id}}.example.com" path "/events"
      """
      {"id":1}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"id":1}
      """
    When this NSPL command request is executed on the leader node
      """
      STOP;
      """
    And this NSPL command request is executed on the leader node
      """
      START;
      """
    And http payload is posted to host "restarted-{{test_id}}.example.com" path "/events"
      """
      {"id":2}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"id":2}
      """
    And node "node-1" observability metric "nervix_session_subscriptions" with labels eventually equals 1
      """
      domain="{{domain}}"
      relay="events"
      """
    And the active session received no frame about a subscription outside its lifetime

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A node's interest in a relay follows every subscription of every session exactly
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge interest-{{test_id}}.example.com;
      CREATE ENDPOINT event_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_ingestor
        FROM ENDPOINT event_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And client "neighbor" is connected to node "<subscriber>"
    When these NSPL commands are executed on node "<subscriber>"
      """
      CREATE SUBSCRIPTION first TO events;
      """
    And these NSPL commands are executed on the active session
      """
      CREATE SUBSCRIPTION second TO events;
      """
    And client "neighbor" executes these NSPL commands
      """
      CREATE SUBSCRIPTION third TO events;
      """
    And these NSPL commands fail on the active session
      """
      CREATE SUBSCRIPTION second TO events;
      """
    Then the last command error contains
      """
      session subscription 'second' already exists
      """
    When these NSPL commands fail on the active session
      """
      CREATE SUBSCRIPTION unfiltered TO events WHERE missing_field > 0;
      """
    Then the last command error contains
      """
      failed to compile session subscription 'unfiltered'
      """
    When these NSPL commands fail on the active session
      """
      CREATE SUBSCRIPTION absent TO absent_relay;
      """
    Then the last command error contains
      """
      stream 'absent_relay' does not exist in domain '{{domain}}'
      """
    And node "<subscriber>" observability metric "nervix_session_subscriptions" with labels eventually equals 3
      """
      domain="{{domain}}"
      relay="events"
      """
    When these NSPL commands are executed on the active session
      """
      DELETE SUBSCRIPTION first;
      """
    Then node "<subscriber>" observability metric "nervix_session_subscriptions" with labels eventually equals 2
      """
      domain="{{domain}}"
      relay="events"
      """
    When http payload is posted to host "interest-{{test_id}}.example.com" path "/events"
      """
      {"id":1}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"id":1}
      """
    And within "30s" client "neighbor" receives a subscription payload
      """
      {"id":1}
      """
    When these NSPL commands are executed on the active session
      """
      DELETE SUBSCRIPTION second;
      """
    Then node "<subscriber>" observability metric "nervix_session_subscriptions" with labels eventually equals 1
      """
      domain="{{domain}}"
      relay="events"
      """
    When http payload is posted to host "interest-{{test_id}}.example.com" path "/events"
      """
      {"id":2}
      """
    Then within "30s" client "neighbor" receives a subscription payload
      """
      {"id":2}
      """
    When client "neighbor" closes its session cleanly
    Then node "<subscriber>" observability metric "nervix_session_subscriptions" with labels eventually equals 0
      """
      domain="{{domain}}"
      relay="events"
      """
    And the active session received no frame about a subscription outside its lifetime

    Examples:
      | cluster_size | subscriber |
      | 1            | node-1     |
      | 3            | node-3     |

  Scenario Outline: A subscription whose client reads nothing is deleted without that client, and nothing about it follows its deletion
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA seed ( text STRING );
      CREATE SCHEMA filler ( text STRING );
      CREATE WIRE JSON SCHEMA seed_wire MODE STRICT ( text string );
      CREATE CODEC seed_codec FROM WIRE JSON SCHEMA seed_wire TO SCHEMA seed;
      CREATE RELAY seeds SCHEMA seed UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY floods SCHEMA filler UNBRANCHED;
      CREATE RELAY probes SCHEMA filler UNBRANCHED;
      CREATE VHOST edge saturated-{{test_id}}.example.com;
      CREATE ENDPOINT seed_endpoint ON edge PATH '/seeds' TYPE HTTP;
      CREATE INGESTOR seed_ingestor
        FROM ENDPOINT seed_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING seed_codec
        TIMESTAMP NOW
        TO seeds
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE GENERATOR flood_generator
        USING MATERIALIZED STATE seeds
        EACH 10ms
        UNBRANCHED
        TO floods
          SET text = repeat(relay_state.seeds.text, 4096)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE GENERATOR probe_generator
        USING MATERIALIZED STATE seeds
        EACH 10ms
        UNBRANCHED
        TO probes
          SET text = repeat(relay_state.seeds.text, 4096)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      START;
      CREATE UNPACED DOMAIN neighbor_{{test_id}};
      """
    And client "neighbor" is connected to node "node-1"
    When client "neighbor" selects domain "neighbor_{{test_id}}"
    And client "neighbor" executes these NSPL commands
      """
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge neighbor-{{test_id}}.example.com;
      CREATE ENDPOINT event_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_ingestor
        FROM ENDPOINT event_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      CREATE SUBSCRIPTION neighbor_events TO events;
      """
    And these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION flood TO floods BLOCKING;
      """
    And these NSPL commands are executed on the active session
      """
      CREATE SUBSCRIPTION probe TO probes DROPPING;
      """
    And http payload is posted to node "node-1" with host "saturated-{{test_id}}.example.com" path "/seeds"
      """
      {"text":"x"}
      """
    Then within "60s" node "node-1" observability metric "nervix_session_subscription_dropped_rows_total" with labels eventually reaches at least 1
      """
      domain="{{domain}}"
      relay="probes"
      """
    When http payload is posted to node "node-1" with host "neighbor-{{test_id}}.example.com" path "/events"
      """
      {"id":7}
      """
    Then within "30s" client "neighbor" receives a subscription payload
      """
      {"id":7}
      """
    When the active session sends request "delete-flood" deleting subscription "flood"
    Then node "node-1" observability metric "nervix_session_subscriptions" with labels eventually equals 0
      """
      domain="{{domain}}"
      relay="floods"
      """
    And request "delete-flood" deleted subscription "flood"
    When the active session reads its frames for "2s"
    Then the active session received no frame about a subscription outside its lifetime
    When the active session closes its session cleanly
    Then node "node-1" observability metric "nervix_session_subscriptions" with labels eventually equals 0
      """
      domain="{{domain}}"
      relay="probes"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
