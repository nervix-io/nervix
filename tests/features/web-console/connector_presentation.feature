Feature: Visual connector choices follow the entity declarations

  @connector_presentation_console
  Scenario Outline: An endpoint editor exposes its declared acknowledged request mode
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-ingestor-source [data-source='<entity>']" is clicked
    Then selector ".create-ingestor-mode [data-mode='<mode>']" exists
    When selector ".create-ingestor-mode [data-mode='<mode>']" is clicked
    Then selector ".create-ingestor-ack-max" exists

    Examples:
      | cluster_size | entity              | mode |
      | 1            | http-endpoint       | ack  |
      | 3            | http-endpoint       | ack  |
      | 1            | websockets-endpoint | ack  |
      | 3            | websockets-endpoint | ack  |

  @connector_presentation_console
  Scenario Outline: A query editor exposes its declared response size option
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-ingestor-source [data-source='prometheus-query']" is clicked
    Then selector ".create-ingestor-max-body-size" exists

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @connector_presentation_console
  Scenario Outline: Each transport editor offers exactly its declared delivery modes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-ingestor-source [data-source='<entity>']" is clicked
    Then selector ".create-ingestor-mode [data-mode='<first>']" exists
    And selector ".create-ingestor-mode [data-mode='<second>']" exists
    And selector ".create-ingestor-mode [data-mode='<third>']" exists
    And selector ".create-ingestor-mode button" has at most <count> elements
    And selector "<control>" exists

    Examples:
      | cluster_size | entity                | first          | second         | third          | count | control                        |
      | 1            | http-request          | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-every         |
      | 3            | http-request          | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-every         |
      | 1            | http-endpoint         | no-ack         | ack            | no-ack         | 2     | .create-ingestor-source-ref    |
      | 3            | http-endpoint         | no-ack         | ack            | no-ack         | 2     | .create-ingestor-source-ref    |
      | 1            | kafka-topic           | no-ack         | ack-sequential | ack-parallel   | 3     | .create-ingestor-topic         |
      | 3            | kafka-topic           | no-ack         | ack-sequential | ack-parallel   | 3     | .create-ingestor-topic         |
      | 1            | pulsar-topic          | no-ack         | ack-sequential | ack-parallel   | 3     | .create-ingestor-subscription  |
      | 3            | pulsar-topic          | no-ack         | ack-sequential | ack-parallel   | 3     | .create-ingestor-subscription  |
      | 1            | mqtt-topic            | no-ack         | ack-sequential | ack-parallel   | 3     | .create-ingestor-mqtt-session  |
      | 3            | mqtt-topic            | no-ack         | ack-sequential | ack-parallel   | 3     | .create-ingestor-mqtt-session  |
      | 1            | nats-subject          | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-queue-group   |
      | 3            | nats-subject          | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-queue-group   |
      | 1            | rabbitmq-queue        | no-ack         | ack-sequential | ack-parallel   | 3     | .create-ingestor-queue         |
      | 3            | rabbitmq-queue        | no-ack         | ack-sequential | ack-parallel   | 3     | .create-ingestor-queue         |
      | 1            | redis-pubsub          | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-channel       |
      | 3            | redis-pubsub          | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-channel       |
      | 1            | redis-pubsub          | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-pattern       |
      | 3            | redis-pubsub          | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-pattern       |
      | 1            | prometheus-query      | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-max-body-size |
      | 3            | prometheus-query      | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-max-body-size |
      | 1            | zeromq-pull           | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-source-ref    |
      | 3            | zeromq-pull           | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-source-ref    |
      | 1            | sqs-queue             | ack-sequential | ack-parallel   | ack-sequential | 2     | .create-ingestor-queue         |
      | 3            | sqs-queue             | ack-sequential | ack-parallel   | ack-sequential | 2     | .create-ingestor-queue         |
      | 1            | websockets-connection | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-source-ref    |
      | 3            | websockets-connection | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-source-ref    |
      | 1            | websockets-endpoint   | no-ack         | ack            | no-ack         | 2     | .create-ingestor-source-ref    |
      | 3            | websockets-endpoint   | no-ack         | ack            | no-ack         | 2     | .create-ingestor-source-ref    |
      | 1            | syslog-listener       | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-source-ref    |
      | 3            | syslog-listener       | no-ack         | no-ack         | no-ack         | 1     | .create-ingestor-source-ref    |

  @connector_presentation_switches
  Scenario Outline: Switching sources refreshes the declared quiesce and option controls
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-ingestor-source [data-source='<first>']" is clicked
    And selector ".create-ingestor-source [data-source='<second>']" is clicked
    Then selector "<current_control>" exists

    Examples:
      | cluster_size | first       | second        | current_control                                          |
      | 1            | mqtt-topic  | http-endpoint | .create-ingestor-quiesce [data-quiesce='buffer']:enabled |
      | 3            | mqtt-topic  | http-endpoint | .create-ingestor-quiesce [data-quiesce='buffer']:enabled |
      | 1            | kafka-topic | nats-subject  | .create-ingestor-instances[placeholder='']               |
      | 3            | kafka-topic | nats-subject  | .create-ingestor-instances[placeholder='']               |

  @connector_presentation_current_options
  Scenario Outline: A connection editor exposes the declared literal upgrade headers
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT reader CONFIG { 'endpoint' = 'ws://127.0.0.1:9/stream' };
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "headed_connection"
    And selector ".create-ingestor-source [data-source='websockets-connection']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "reader"
    Then selector ".create-ingestor-source-ref [data-value='reader']" exists
    When selector ".create-ingestor-source-ref [data-value='reader']" is clicked
    Then selector ".create-ingestor-upgrade-headers-add" exists
    When selector ".create-ingestor-upgrade-headers-add" is clicked
    And selector ".create-ingestor-upgrade-header-name" is filled with "x-trace"
    And selector ".create-ingestor-upgrade-header-value" is filled with "stream"
    Then selector ".create-ingestor-upgrade-header-name" has value "x-trace"
    And selector ".create-ingestor-upgrade-header-value" has value "stream"
    When selector ".create-ingestor-quiesce [data-quiesce='drop']" is clicked
    And selector ".create-ingestor-codec .create-choice-search" is filled with "event_codec"
    Then selector ".create-ingestor-codec [data-value='event_codec']" exists
    When selector ".create-ingestor-codec [data-value='event_codec']" is clicked
    And selector ".create-ingestor-timestamp [data-timestamp='now']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branching [data-branch='unbranched']" is clicked
    And selector ".create-ingestor-route .create-ingestor-relay .create-choice-search" is filled with "events"
    Then selector ".create-ingestor-route .create-ingestor-relay [data-value='events']" exists
    When selector ".create-ingestor-route .create-ingestor-relay [data-value='events']" is clicked
    And selector ".create-ingestor-route .create-ingestor-inherit [data-inherit='all']" is clicked
    And selector ".create-ingestor-route .create-ingestor-flush [data-flush='immediate']" is clicked
    And selector ".create-ingestor-route .create-ingestor-message-error [data-error='log']" is clicked
    And selector ".create-ingestor-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "HEADERS { 'x-trace' = 'stream' }"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus statements are executed on the leader node
      """
      DESCRIBE INGESTOR headed_connection;
      """
    Then the last command output contains
      """
      options: HEADERS { 'x-trace' = 'stream' }
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @connector_presentation_current_options
  Scenario Outline: A queue editor retains its declared parallel acknowledgement window
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = 'amqp://127.0.0.1:5672' };
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "parallel_queue"
    And selector ".create-ingestor-source [data-source='rabbitmq-queue']" is clicked
    And selector ".create-ingestor-source-ref .create-choice-search" is filled with "broker"
    Then selector ".create-ingestor-source-ref [data-value='broker']" exists
    When selector ".create-ingestor-source-ref [data-value='broker']" is clicked
    And selector ".create-ingestor-queue" is filled with "Orders"
    And selector ".create-ingestor-mode [data-mode='ack-parallel']" is clicked
    And selector ".create-ingestor-ack-max" is filled with "12"
    And selector ".create-ingestor-batch-timeout" is filled with "250ms"
    And selector ".create-ingestor-ack-timeout" is filled with "5s"
    And selector ".create-ingestor-retry-backoff" is filled with "100ms"
    And selector ".create-ingestor-retry-max" is filled with "1s"
    And selector ".create-ingestor-quiesce [data-quiesce='suspend']" is clicked
    And selector ".create-ingestor-codec .create-choice-search" is filled with "event_codec"
    Then selector ".create-ingestor-codec [data-value='event_codec']" exists
    When selector ".create-ingestor-codec [data-value='event_codec']" is clicked
    And selector ".create-ingestor-timestamp [data-timestamp='now']" is clicked
    And selector ".create-ingestor-route .create-ingestor-branching [data-branch='unbranched']" is clicked
    And selector ".create-ingestor-route .create-ingestor-relay .create-choice-search" is filled with "events"
    Then selector ".create-ingestor-route .create-ingestor-relay [data-value='events']" exists
    When selector ".create-ingestor-route .create-ingestor-relay [data-value='events']" is clicked
    And selector ".create-ingestor-route .create-ingestor-inherit [data-inherit='all']" is clicked
    And selector ".create-ingestor-route .create-ingestor-flush [data-flush='immediate']" is clicked
    And selector ".create-ingestor-route .create-ingestor-message-error [data-error='log']" is clicked
    And selector ".create-ingestor-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "MODE ACK PARALLEL MAX 12 BATCH TIMEOUT 250ms ACK TIMEOUT 5s"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus statements are executed on the leader node
      """
      DESCRIBE INGESTOR parallel_queue;
      """
    Then the last command output contains
      """
      mode: ACK PARALLEL MAX 12 BATCH TIMEOUT 250ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
