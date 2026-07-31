Feature: Web console documentation screenshots
  # The subject is the documentation artifact, not runtime behaviour, so this feature is
  # deliberately single-topology instead of a one-node/three-node outline: an outline would
  # capture every image twice and the second topology would silently overwrite the first. One
  # node also matches the single-node local setup the published screenshots illustrate.
  #
  # The seeded graph is the quickstart pipeline from docs/src/quickstart-first-pipeline.md and
  # docs/src/quickstart-conditional-routing.md, so a reader recognises it. The domain is left
  # stopped: an unreachable Kafka broker would put the ingestor into ERROR with a live reconnect
  # countdown, which is both unstable to capture and a poor illustration.
  Scenario: Console screenshots for the Client Tools documentation
    Given a 1 node nervix cluster is started
    And the active domain is "quickstart"
    And node "node-1" has resource directory "order_model_bundle" containing
      """
      {
        "scoring.roto": "filter tier_score(order) { accept }",
        "labels/classes.txt": "new\npaid\nheld"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN quickstart;
      CREATE RESOURCE order_model;
      CREATE SCHEMA order_record (
        order_id STRING,
        customer STRING,
        status STRING,
        amount I64,
        quantity I64
      );
      CREATE SCHEMA order_tiered (
        order_id STRING,
        customer STRING,
        status STRING,
        amount I64,
        quantity I64,
        tier STRING
      );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT (
        order_id string,
        customer string,
        status string,
        amount integer,
        quantity integer
      );
      CREATE WIRE JSON SCHEMA order_tiered_wire MODE STRICT (
        order_id string,
        customer string,
        status string,
        amount integer,
        quantity integer,
        tier string
      );
      CREATE CODEC order_codec FROM WIRE JSON SCHEMA order_wire TO SCHEMA order_record;
      CREATE CODEC order_tiered_codec FROM WIRE JSON SCHEMA order_tiered_wire TO SCHEMA order_tiered;
      CREATE RELAY orders SCHEMA order_record UNBRANCHED;
      CREATE RELAY high_value_orders SCHEMA order_tiered UNBRANCHED;
      CREATE RELAY routine_orders SCHEMA order_record UNBRANCHED;
      CREATE CLIENT kafka_local TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT redis_local TYPE REDIS CONFIG { 'addr' = 'redis://127.0.0.1:6379/' };
      CREATE INGESTOR kafka_orders
        FROM KAFKA kafka_local
        TOPIC orders
        OFFSET BY CONSUMER GROUP quickstart
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        DECODE USING order_codec
        TO orders
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_orders
        FROM orders
        UNBRANCHED
        TO high_value_orders
          INHERIT ALL
          SET tier = CASE
                WHEN input.amount >= 10000 THEN "vip"
                ELSE "high"
              END
          WHERE output.amount >= 1000
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO routine_orders
          INHERIT ALL
          WHERE output.amount < 1000
          FLUSH EACH 1s MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
      CREATE EMITTER redis_orders
        FROM orders
        TO REDIS PUBSUB redis_local CHANNEL orders_out ENCODE USING order_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER redis_high_value
        FROM high_value_orders
        TO REDIS PUBSUB redis_local CHANNEL high_value_out ENCODE USING order_tiered_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node for documentation screenshots
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".graph-hit-layer" contains "kafka_orders"
    And selector ".graph-hit-layer" contains "route_orders"
    And selector ".graph-hit-layer" contains "redis_high_value"
    And selector ".nav-list" contains "order_record"
    And selector ".nav-list" contains "order_codec"
    And selector ".nav-list" contains "order_model"

    # The console shell: sidebar entities, the live execution graph, and the REPL.
    When selector ".prompt-row input" is filled with "DESCRIBE RELAY orders;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "relay: orders"
    When selector ".prompt-row input" is filled with "LIST DOMAINS;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "quickstart pace=UNPACED status=STOPPED"
    # The whole pipeline is wider than the panel at full zoom, so frame it first.
    When selector ".zoom-group button[title='Reset zoom']" is clicked
    And selector ".zoom-group button[title='Zoom out']" is clicked
    And selector ".zoom-group button[title='Zoom out']" is clicked
    And selector ".zoom-group button[title='Zoom out']" is clicked
    Then selector ".zoom-group" contains "70%"
    When the web console is captured as documentation screenshot "console-overview.png"
    Then documentation screenshot "console-overview.png" is a PNG at least 3000 by 1900 pixels

    # The execution graph on its own. Fullscreen in a short viewport frames the pipeline without
    # the empty band a full-height stage leaves above and below a single-row graph.
    When selector ".fullscreen-button" is clicked
    And the browser viewport is resized to 1920 by 620
    And selector ".zoom-group button[title='Reset zoom']" is clicked
    And selector ".zoom-group button[title='Zoom out']" is clicked
    And selector ".zoom-group button[title='Zoom out']" is clicked
    Then selector ".graph-panel.fullscreen" contains "EXECUTION GRAPH"
    And selector ".zoom-group" contains "80%"
    When selector ".graph-panel" is captured as documentation screenshot "console-graph.png"
    Then documentation screenshot "console-graph.png" is a PNG at least 3000 by 900 pixels
    When selector ".fullscreen-button" is clicked
    And the browser viewport is resized to 1920 by 1200
    Then selector ".graph-panel.fullscreen" does not exist

    # Clicking a graph item opens its action menu.
    When selector ".relay-hit[data-label='orders']" is clicked by script
    Then selector ".graph-action-menu" contains "orders"
    And selector ".graph-action-menu" contains "SUBSCRIBE"
    When selector ".graph-action-menu" is captured as documentation screenshot "console-graph-actions.png"
    Then documentation screenshot "console-graph-actions.png" is a PNG at least 600 by 250 pixels

    # The guided subscription dialog reached from that menu.
    When selector ".graph-action-list button:has-text('SUBSCRIBE')" is clicked
    Then selector ".subscribe-dialog" contains "order_record"
    When selector ".schema-field-button:has-text('amount')" is clicked
    Then selector ".subscribe-dialog input" has value "input.amount"
    When selector ".subscribe-dialog input" is filled with "input.amount >= 1000"
    And selector ".sample-options button:has-text('10%')" is clicked
    And selector ".subscribe-dialog" is captured as documentation screenshot "console-subscribe-dialog.png"
    Then documentation screenshot "console-subscribe-dialog.png" is a PNG at least 900 by 500 pixels
    When selector ".subscribe-actions button:has-text('CANCEL')" is clicked
    Then selector ".subscribe-dialog" does not exist

    # The REPL with server-driven completion offered for a partial statement.
    When selector ".prompt-row input" is filled with "DESCRIBE RELAY "
    Then selector ".suggestions" contains "high_value_orders"
    When selector ".repl-panel" is captured as documentation screenshot "console-repl.png"
    Then documentation screenshot "console-repl.png" is a PNG at least 2400 by 500 pixels

    # Resource versions uploaded from the browser.
    When selector ".prompt-row input" is filled with ""
    And selector ".nav-item.resources:has-text('order_model')" is clicked
    Then selector ".resource-dialog" contains "order_model"
    When selector ".resource-dialog .file-upload-input" uploads resource directory "order_model_bundle"
    Then selector ".resource-version-list" contains "version 1"
    And selector ".resource-version-list" contains "2 files"
    When selector ".resource-dialog" is captured as documentation screenshot "console-resource-dialog.png"
    Then documentation screenshot "console-resource-dialog.png" is a PNG at least 900 by 400 pixels
