@graceful_swap_stops
Feature: A change stops the parts it replaces without cutting what they hold
  A change replaces the parts it alters: it stops the previous ones and starts new ones. A part
  that holds rows when it stops delivers them before it ends, however long that takes, and a change
  stops the parts it replaces upstream first, so a part whose reader the same change replaces still
  delivers what it holds before that reader's input is held. Each scenario makes rows rest in a
  part that a retried swap stops: a fault fails the change's swap on the part's node after it
  stopped the part, so the node keeps applying the change again on its own and putting the part
  back, with no drain before any of those swaps, and rows of two branches rest in the part's
  one-hour flush meanwhile. Once the fault clears, a retry replaces the part. The subscription reads
  the part's own output relay, so it sees every row the part delivered and any it repeated.

  @graceful_swap_stops_ingestor
  Scenario Outline: A retried change that replaces a branched ingestor route with its reader delivers what the route held
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA swap_event ( event_id STRING, tenant STRING, content STRING );
      CREATE WIRE JSON SCHEMA swap_event_wire MODE STRICT (
        event_id string,
        tenant string,
        content string
      );
      CREATE WIRE JSON CODEC swap_event_codec FROM WIRE SCHEMA swap_event_wire TO SCHEMA swap_event;
      CREATE SCHEMA swap_tenant ( tenant STRING );
      CREATE PARAMETRIZER by_swap_tenant SCHEMA swap_tenant TTL 1h;
      CREATE RELAY swap_incoming SCHEMA swap_event BRANCHED BY by_swap_tenant;
      CREATE RELAY swap_delivered SCHEMA swap_event BRANCHED BY by_swap_tenant;
      CREATE VHOST edge http-{{test_id}}-swap-stops.example.com;
      CREATE HTTP ENDPOINT swap_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR swap_in
        FROM swap_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING swap_event_codec
        TO swap_incoming
          INHERIT ALL
          BRANCHED BY by_swap_tenant
          SET tenant = message.tenant
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION swap_reader FROM swap_incoming
        BRANCHED BY by_swap_tenant
        TO swap_delivered INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "swap_reader" is saved as placeholder "reader_owner"
    # The ingestor runs on every node; its route and its reader run on the junction's node.
    When these Plexus commands are executed on the leader node
      """
      RELOCATE RELAY swap_incoming, RELAY swap_delivered ONTO NODE {{reader_owner}} IGNORE PREFERENCES;
      CREATE SUBSCRIPTION swap_observed TO swap_incoming;
      """
    # An ingestor's step cannot fail once its change commits, so the fault fails the swap at the
    # reader. The reader is a junction, which the swap replaces after it stopped the ingestor.
    Given every entity swap in domain "{{domain}}" on node "{{reader_owner}}" fails while replacing junction "swap_reader"
    When these Plexus commands begin executing in the background
      """
      BEGIN;
      ALTER HTTP ENDPOINT INGESTOR swap_in SET FILTER WHERE input.event_id != 'none';
      ALTER JUNCTION swap_reader SET DETACHED;
      COMMIT;
      """
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    # Until the fault clears, the node retries the change and puts the ingestor back each time, so
    # its endpoint is briefly unavailable now and then: a post waits until it is accepted.
    When http payload is accepted within "30s" by node "{{reader_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"i-1","tenant":"acme","content":"c-i-1"}
      """
    And http payload is accepted within "30s" by node "{{reader_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"i-2","tenant":"beta","content":"c-i-2"}
      """
    And http payload is accepted within "30s" by node "{{reader_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"i-3","tenant":"acme","content":"c-i-3"}
      """
    And http payload is accepted within "30s" by node "{{reader_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"i-4","tenant":"beta","content":"c-i-4"}
      """
    # The node retries the committed change on its own, and once its swap can succeed it replaces
    # the ingestor and its reader together. Every retry, the failing ones too, stops the ingestor
    # before it holds the reader's input, and no drain flushes the route before the swap.
    When entity swaps in domain "{{domain}}" on node "{{reader_owner}}" stop failing
    Then within "60s" the relay subscription receives payloads
      """
      "event_id":"i-1"
      "event_id":"i-2"
      "event_id":"i-3"
      "event_id":"i-4"
      """
    And the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_swap_stops_reingestor
  Scenario Outline: A retried change that replaces a reingestor with its reader delivers what the reingestor held
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA swap_event ( event_id STRING, tenant STRING, content STRING );
      CREATE WIRE JSON SCHEMA swap_event_wire MODE STRICT (
        event_id string,
        tenant string,
        content string
      );
      CREATE WIRE JSON CODEC swap_event_codec FROM WIRE SCHEMA swap_event_wire TO SCHEMA swap_event;
      CREATE SCHEMA swap_tenant ( tenant STRING );
      CREATE PARAMETRIZER by_swap_tenant SCHEMA swap_tenant TTL 1h;
      CREATE RELAY swap_incoming SCHEMA swap_event UNBRANCHED;
      CREATE RELAY swap_regrouped SCHEMA swap_event BRANCHED BY by_swap_tenant;
      CREATE RELAY swap_delivered SCHEMA swap_event BRANCHED BY by_swap_tenant;
      CREATE VHOST edge http-{{test_id}}-swap-stops.example.com;
      CREATE HTTP ENDPOINT swap_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR swap_in
        FROM swap_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING swap_event_codec
        TO swap_incoming INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REINGESTOR swap_regroup FROM swap_incoming
        TO swap_regrouped
          INHERIT ALL
          BRANCHED BY by_swap_tenant
          SET tenant = input.tenant
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR swap_dedup FROM swap_regrouped
        DEDUPLICATE ON input.event_id
        MAX TIME 10m
        BRANCHED BY by_swap_tenant
        TO swap_delivered INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "deduplicator" "swap_dedup" is saved as placeholder "dedup_owner"
    When these Plexus commands are executed on the leader node
      """
      RELOCATE REINGESTOR swap_regroup, RELAY swap_incoming, RELAY swap_regrouped, RELAY swap_delivered ONTO NODE {{dedup_owner}} IGNORE PREFERENCES;
      CREATE SUBSCRIPTION swap_observed TO swap_regrouped;
      """
    Given every entity swap in domain "{{domain}}" on node "{{dedup_owner}}" fails while replacing reingestor "swap_regroup"
    When these Plexus commands begin executing in the background
      """
      BEGIN;
      ALTER REINGESTOR swap_regroup SET FILTER WHERE input.event_id != 'none';
      ALTER DEDUPLICATOR swap_dedup SET DEDUPLICATE ON input.content;
      COMMIT;
      """
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    When http payload is posted to node "{{dedup_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"r-1","tenant":"acme","content":"c-r-1"}
      """
    And http payload is posted to node "{{dedup_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"r-2","tenant":"beta","content":"c-r-2"}
      """
    And http payload is posted to node "{{dedup_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"r-3","tenant":"acme","content":"c-r-3"}
      """
    And http payload is posted to node "{{dedup_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"r-4","tenant":"beta","content":"c-r-4"}
      """
    When entity swaps in domain "{{domain}}" on node "{{dedup_owner}}" stop failing
    Then within "60s" the relay subscription receives payloads
      """
      "event_id":"r-1"
      "event_id":"r-2"
      "event_id":"r-3"
      "event_id":"r-4"
      """
    And the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_swap_stops_generator
  Scenario Outline: A retried change that replaces a generator with its reader delivers the rows the generator held
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA swap_event ( event_id STRING, tenant STRING, content STRING );
      CREATE WIRE JSON SCHEMA swap_event_wire MODE STRICT (
        event_id string,
        tenant string,
        content string
      );
      CREATE WIRE JSON CODEC swap_event_codec FROM WIRE SCHEMA swap_event_wire TO SCHEMA swap_event;
      CREATE SCHEMA swap_tenant ( tenant STRING );
      CREATE PARAMETRIZER by_swap_tenant SCHEMA swap_tenant TTL 1h;
      CREATE RELAY swap_state SCHEMA swap_event BRANCHED BY by_swap_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY swap_generated SCHEMA swap_event BRANCHED BY by_swap_tenant;
      CREATE RELAY swap_delivered SCHEMA swap_event BRANCHED BY by_swap_tenant;
      CREATE VHOST edge http-{{test_id}}-swap-stops.example.com;
      CREATE HTTP ENDPOINT swap_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR swap_in
        FROM swap_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING swap_event_codec
        TO swap_state
          INHERIT ALL
          BRANCHED BY by_swap_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE GENERATOR swap_synth
        USING MATERIALIZED STATE swap_state
        EACH 1h
        BRANCHED BY by_swap_tenant
        TO swap_generated
          SET event_id = relay_state.swap_state.event_id,
              tenant = relay_state.swap_state.tenant,
              content = relay_state.swap_state.content
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR swap_dedup FROM swap_generated
        DEDUPLICATE ON input.event_id
        MAX TIME 10m
        BRANCHED BY by_swap_tenant
        TO swap_delivered INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "deduplicator" "swap_dedup" is saved as placeholder "dedup_owner"
    When these Plexus commands are executed on the leader node
      """
      RELOCATE GENERATOR swap_synth, RELAY swap_state, RELAY swap_generated, RELAY swap_delivered ONTO NODE {{dedup_owner}} IGNORE PREFERENCES;
      CREATE SUBSCRIPTION swap_observed TO swap_generated;
      """
    # The generator samples this state on its hourly cadence, and at once whenever its task starts.
    When http payload is posted to node "{{dedup_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"g-1","tenant":"acme","content":"c-g-1"}
      """
    And http payload is posted to node "{{dedup_owner}}" with host "http-{{test_id}}-swap-stops.example.com" path "/events"
      """
      {"event_id":"g-2","tenant":"beta","content":"c-g-2"}
      """
    # Each failed swap puts the previous generator back; its task starts again and samples both
    # branches at once, and the rows rest in its one-hour flush until the next swap stops it. The
    # replacement marks the rows it generates and sends them at once, so they stay apart from the
    # previous generator's rows and show when the change has replaced it.
    Given every entity swap in domain "{{domain}}" on node "{{dedup_owner}}" fails while replacing generator "swap_synth"
    When these Plexus commands begin executing in the background
      """
      BEGIN;
      ALTER GENERATOR swap_synth
        SET EACH 2h,
        REPLACE ROUTE TO swap_generated
          SET event_id = relay_state.swap_state.event_id,
              tenant = relay_state.swap_state.tenant,
              content = 'replaced'
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      ALTER DEDUPLICATOR swap_dedup SET DEDUPLICATE ON input.content;
      COMMIT;
      """
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    When entity swaps in domain "{{domain}}" on node "{{dedup_owner}}" stop failing
    Then within "60s" the relay subscription receives payloads
      """
      "content":"c-g-1"
      "content":"c-g-2"
      "content":"replaced","event_id":"g-1"
      "content":"replaced","event_id":"g-2"
      """
    # Every previous generator ended before the replacement started, after its rows left once: none
    # of them arrives again.
    And the relay subscription does not receive a payload containing fragments within "2s"
      """
      "content":"c-g-
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
