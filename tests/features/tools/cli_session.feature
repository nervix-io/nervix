Feature: CLI public session dispatch
  @connector_completion
  Scenario Outline: CLI completes functions in connector filters and HTTP request expressions
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "<prefix>|" on node "{{leader}}"
    Then the CLI suggestion "<function>" applied to "<prefix>|" yields "<complete>"

    Examples:
      | cluster_size | prefix                                                                                                                                                                             | function    | complete                                                                                                                                                                              |
      | 1            | CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec FILTER WHERE read_                                                     | read_header | CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec FILTER WHERE read_header                                                  |
      | 1            | CREATE KAFKA TOPIC EMITTER e FROM incoming WHERE co                                                                                                                                | coalesce    | CREATE KAFKA TOPIC EMITTER e FROM incoming WHERE coalesce                                                                                                                             |
      | 1            | CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD co                                                                                                                    | coalesce    | CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD coalesce                                                                                                                 |
      | 1            | CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD 'POST' PATH co                                                                                                        | coalesce    | CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD 'POST' PATH coalesce                                                                                                     |
      | 1            | ALTER HTTP ENDPOINT INGESTOR i ADD ROUTE TO outgoing SET value = read_                                                                                                             | read_header | ALTER HTTP ENDPOINT INGESTOR i ADD ROUTE TO outgoing SET value = read_header                                                                                                          |
      | 3            | CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec FILTER WHERE read_                                                     | read_header | CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec FILTER WHERE read_header                                                  |
      | 3            | CREATE KAFKA TOPIC EMITTER e FROM incoming WHERE co                                                                                                                                | coalesce    | CREATE KAFKA TOPIC EMITTER e FROM incoming WHERE coalesce                                                                                                                             |
      | 3            | CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD co                                                                                                                    | coalesce    | CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD coalesce                                                                                                                 |
      | 3            | CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD 'POST' PATH co                                                                                                        | coalesce    | CREATE HTTP REQUEST EMITTER e FROM incoming TO client METHOD 'POST' PATH coalesce                                                                                                     |
      | 3            | ALTER HTTP ENDPOINT INGESTOR i ADD ROUTE TO outgoing SET value = read_                                                                                                             | read_header | ALTER HTTP ENDPOINT INGESTOR i ADD ROUTE TO outgoing SET value = read_header                                                                                                          |
      | 1            | CREATE KAFKA TOPIC INGESTOR i FROM broker TOPIC 'events' OFFSET BY CONSUMER GROUP 'g' START AT EARLIEST MODE NO ACK ON QUIESCE SUSPEND DECODE USING codec FILTER WHERE metadata.to | topic       | CREATE KAFKA TOPIC INGESTOR i FROM broker TOPIC 'events' OFFSET BY CONSUMER GROUP 'g' START AT EARLIEST MODE NO ACK ON QUIESCE SUSPEND DECODE USING codec FILTER WHERE metadata.topic |
      | 1            | CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec FILTER WHERE coalesce(read_                                            | read_header | CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec FILTER WHERE coalesce(read_header                                         |
      | 3            | CREATE KAFKA TOPIC INGESTOR i FROM broker TOPIC 'events' OFFSET BY CONSUMER GROUP 'g' START AT EARLIEST MODE NO ACK ON QUIESCE SUSPEND DECODE USING codec FILTER WHERE metadata.to | topic       | CREATE KAFKA TOPIC INGESTOR i FROM broker TOPIC 'events' OFFSET BY CONSUMER GROUP 'g' START AT EARLIEST MODE NO ACK ON QUIESCE SUSPEND DECODE USING codec FILTER WHERE metadata.topic |
      | 3            | CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec FILTER WHERE coalesce(read_                                            | read_header | CREATE HTTP ENDPOINT INGESTOR i FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING codec FILTER WHERE coalesce(read_header                                         |

  @connector_completion
  Scenario Outline: CLI applies semantic completion edits at the real cursor
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event ( value I64, secret STRING );
      CREATE SCHEMA unrelated ( unrelated_field I64 );
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "ALTER SCHEMA order_event DROP FIELD val|x" on node "{{leader}}"
    Then the CLI suggestion "value" applied to "ALTER SCHEMA order_event DROP FIELD val|x" yields "ALTER SCHEMA order_event DROP FIELD value"
    And the CLI output does not contain "unrelated_field"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: CLI completion uses UTF-8 byte edits after Unicode text
    Given a 1 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event ( value I64 );
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "// 😊\nALTER SCHEMA order_event DROP FIELD val|x" on node "{{leader}}"
    Then the CLI suggestion "value" applied to "// 😊\nALTER SCHEMA order_event DROP FIELD val|x" yields "// 😊\nALTER SCHEMA order_event DROP FIELD value"

  Scenario: CLI completion searches local upload paths
    Given a 1 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "UPLOAD RESOURCE bundle VERSION 'Cargo.t|ml'" on node "{{leader}}"
    Then the CLI suggestion "Cargo.toml" applied to "UPLOAD RESOURCE bundle VERSION 'Cargo.t|ml'" yields "UPLOAD RESOURCE bundle VERSION 'Cargo.toml'"

  Scenario Outline: CLI dispatches a command through a public session
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "LIST DOMAINS;" on node "{{leader}}"
    Then the CLI output contains "{{domain}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: CLI connects by hostname over <mode> through the configured DNS fixture
    Given client grpc transport is configured with mode "<mode>"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "LIST DOMAINS;" on node "{{leader}}" through fixture DNS
    Then the CLI output contains "{{domain}}"
    And the DNS fixture eventually receives a question for "native-session.nervix.test"

    Examples:
      | cluster_size | mode  |
      | 1            | http  |
      | 3            | http  |
      | 1            | https |
      | 3            | https |

  Scenario: CLI rejects a TLS certificate for a different DNS hostname
    Given client grpc transport is configured with mode "https"
    And cluster peers are addressed by "DNS names"
    And a 1 node nervix cluster is started
    When the CLI executes "LIST DOMAINS;" on node "node-1" through fixture DNS name "unlisted-session.test"
    Then the CLI fails with "certificate"
    And the DNS fixture eventually receives a question for "unlisted-session.test"

  Scenario: CLI connects by hostname and follows the leader from a follower session
    Given cluster peers are addressed by "DNS names"
    And a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the CLI executes "LIST DOMAINS;" on node "{{follower}}" through fixture DNS
    Then the CLI output contains "{{domain}}"
    And the DNS fixture eventually receives a question for "native-session.nervix.test"

  Scenario: CLI rejects incorrect credentials through a public session
    Given a 1 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "LIST DOMAINS;" on node "{{leader}}" with password "incorrect_password"
    Then the CLI fails with "Unauthenticated"

  Scenario Outline: CLI subscription keeps receiving after a high volume of rows
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA metric ( value I32 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT raw_metrics_endpoint ON edge PATH '/metrics' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR raw_metrics_source FROM raw_metrics_endpoint MODE ACK MAX 8 ACK TIMEOUT 60s ON QUIESCE REJECT RETRY AFTER 1s DECODE USING metric_codec TO raw_metrics INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI subscribes to relay "raw_metrics" on node "{{leader}}"
    Then the CLI subscription output eventually contains "listening for events from relay 'raw_metrics'"
    When 270 metric http payloads are posted with acknowledgement, 4 at a time, to host "http-{{test_id}}.example.com" path "/metrics"
    Then the CLI subscription output eventually contains '"value":270'

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @slots_2
  Scenario Outline: CLI keeps printing notices and new subscription rows after its node restarts
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA metric ( value I32 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT raw_metrics_endpoint ON edge PATH '/metrics' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR raw_metrics_source FROM raw_metrics_endpoint MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec TO raw_metrics INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "cli_node"
    When the CLI REPL is started on node "{{cli_node}}"
    And node "{{cli_node}}" is stopped
    And node "{{cli_node}}" is started
    Then node "{{cli_node}}" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":0}
      """
    When the CLI REPL runs "LIST DOMAINS;"
    Then the CLI REPL eventually displays "{{domain}} pace=UNPACED"
    When the CLI REPL runs "CREATE SUBSCRIPTION watch TO raw_metrics;"
    Then the CLI REPL eventually displays "created subscription 'watch'"
    When http payload is posted to node "{{cli_node}}" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"extra":true}
      """
    Then the CLI REPL eventually displays "[events] server ERROR: ingestor 'raw_metrics_source' general error in domain '{{domain}}': decode rejection"
    When http payload is posted repeatedly to node "{{cli_node}}" with host "http-{{test_id}}.example.com" path "/metrics" until the CLI REPL displays '[events] subscription [watch] from [raw_metrics]: {"value":7}'
      """
      {"value":7}
      """
    Then the CLI REPL eventually displays '[events] subscription [watch] from [raw_metrics]: {"value":7}'
    When the CLI REPL runs "exit"
    Then the CLI REPL ends successfully

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: CLI attaches its session to the active domain's clock
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 100ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "ATTACH DOMAIN CLOCK;" on node "{{leader}}"
    Then the CLI output contains "attached to the clock of domain '{{domain}}': generation 1, paced"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The CLI follows a domain clock through its generations
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    When the CLI follows the clock of domain "{{domain}}" on node "node-1"
    Then within "10s" the CLI clock output contains "attached to the clock of domain '{{domain}}': generation 1, paced: period 100ms, skew 10ms, logical origin 2030-01-01T00:00:00Z, UTC anchor"
    And within "10s" the CLI clock output contains "time rate 2"
    And within "10s" the CLI clock output has 3 increasing ticks for generation 1 of domain "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then within "10s" the CLI clock output contains "[events] domain clock [{{domain}}]: generation 1, stopped"
    When the domain clock is started at now with time rate "1.0" on the leader node
    Then within "10s" the CLI clock output contains "[events] domain clock [{{domain}}]: generation 2, paced: period 100ms, skew 10ms"
    And within "10s" the CLI clock output has a tick for generation 2 after its state of domain "{{domain}}"
    When the CLI clock process receives Ctrl-C
    Then the CLI clock process exits successfully

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The CLI refuses to follow the clock of a missing domain
    Given a <cluster_size> node nervix cluster is started
    When the CLI attempts to follow the clock of missing domain "absent-clock" on node "<cli_node>"
    Then the CLI fails with "domain 'absent-clock' does not exist"

    Examples:
      | cluster_size | cli_node |
      | 1            | node-1   |
      | 3            | node-2   |

  Scenario Outline: The CLI follows a domain clock across a cluster restart
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 500ms SKEW 50ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    When the CLI follows the clock of domain "{{domain}}" on node "<cli_node>"
    Then within "10s" the CLI clock output has 2 increasing ticks for generation 1 of domain "{{domain}}"
    When the cluster is restarted
    Then within "60s" the CLI clock output contains "[events] domain clock [{{domain}}] notice: the session was interrupted; the clock is attached again on the next session"
    And within "60s" the CLI clock output shows the clock it attached to again after an interruption of domain "{{domain}}"
    And within "30s" the CLI clock output has a tick for generation 1 after its state of domain "{{domain}}"
    When the CLI clock process receives Ctrl-C
    Then the CLI clock process exits successfully

    Examples:
      | cluster_size | cli_node |
      | 1            | node-1   |
      | 3            | node-2   |

  Scenario: The CLI restores a domain clock after transport loss
    Given a 1 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    Given the CLI clock connection to node "node-1" is forwarded
    When the CLI follows the clock of domain "{{domain}}" through its TCP forwarder
    Then within "10s" the CLI clock output contains "attached to the clock of domain '{{domain}}': generation 1, paced"
    When the TCP forwarder at "127.0.0.1" stops
    Then within "20s" the CLI clock output contains "[events] domain clock [{{domain}}] notice: the session was interrupted"
    When the TCP forwarder at "127.0.0.1" restarts
    Then within "20s" the CLI clock output has a fresh state for generation 1 after interruption of domain "{{domain}}"
    And within "20s" the CLI clock output has a tick for generation 1 after its state of domain "{{domain}}"
    When the CLI clock process receives Ctrl-C
    Then the CLI clock process exits successfully

  Scenario Outline: The CLI exits with the status of each outcome
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "exit_bundle" containing
      """
      {
        "readme.txt": "an upload fixture of the CLI exit statuses\n"
      }
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "CREATE RESOURCE exit_resource;" on node "{{leader}}"
    Then the CLI exited with status 0 and printed "created resource 'exit_resource'"
    When the CLI executes "CREATE RESOURCE exit_resource;" on node "{{leader}}"
    Then the CLI exited with status 1 and printed "resource 'exit_resource' already exists"
    When the CLI runs "uncordon-node node-1" on node "{{leader}}"
    Then the CLI exited with status 0 and printed "uncordoned node 'node-1'"
    When the CLI runs "cordon-node node-9" on node "{{leader}}"
    Then the CLI exited with status 1 and printed "error: node 'node-9' is not a raft member"
    When the CLI runs "subscribe watch missing_relay" on node "{{leader}}"
    Then the CLI exited with status 1 and printed "error: "
    When the CLI executes "UPLOAD RESOURCE undeclared_bundle VERSION '{{exit_bundle}}';" on node "{{leader}}"
    Then the CLI exited with status 1 and printed "upload resource 'undeclared_bundle' failed"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Ctrl-C cancels a CLI command the leader has not admitted
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command admission on node "{{leader}}" pauses before proposal
    When the CLI starts executing "CREATE RESOURCE cancelled_resource;" on node "{{leader}}"
    Then the command admission pause on node "{{leader}}" is reached
    When the started CLI receives Ctrl-C
    Then the started CLI exits with status 130 and prints "cancelled: the command was not admitted, so it changed nothing"
    When the command admission pause on node "{{leader}}" is released
    And the CLI executes "CREATE RESOURCE cancelled_resource;" on node "{{leader}}"
    Then the CLI exited with status 0 and printed "created resource 'cancelled_resource'"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A CLI command cancelled after admission is uncertain until its execution reference recovers it
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command execution on node "{{leader}}" pauses after durable admission
    When the CLI starts executing "CREATE RESOURCE admitted_resource;" on node "{{leader}}"
    Then the durable command admission pause on node "{{leader}}" is reached
    When the started CLI receives Ctrl-C
    Then the started CLI exits with status 3 and prints "outcome: not known yet"
    And the execution reference the CLI printed is saved as placeholder "admitted_reference"
    When the durable command admission pause on node "{{leader}}" is released
    And the CLI executes "CREATE RESOURCE admitted_resource;" under execution reference "{{admitted_reference}}" on node "{{leader}}"
    Then the CLI exited with status 0 and printed "created resource 'admitted_resource'"
    When the CLI executes "CREATE RESOURCE admitted_resource;" on node "{{leader}}"
    Then the CLI exited with status 1 and printed "resource 'admitted_resource' already exists"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The CLI REPL survives a cancelled and an uncertain command, and running the uncertain one again recovers it
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI REPL is started on node "{{leader}}"
    Given command admission on node "{{leader}}" pauses before proposal
    When the CLI REPL runs "CREATE RESOURCE repl_cancelled;"
    Then the command admission pause on node "{{leader}}" is reached
    When the CLI REPL receives Ctrl-C
    Then the CLI REPL eventually displays "cancelled: the command was not admitted, so it changed nothing"
    When the command admission pause on node "{{leader}}" is released
    Given command execution on node "{{leader}}" pauses after durable admission
    When the CLI REPL runs "CREATE RESOURCE repl_admitted;"
    Then the durable command admission pause on node "{{leader}}" is reached
    When the CLI REPL receives Ctrl-C
    Then the CLI REPL eventually displays "outcome: not known yet"
    When the durable command admission pause on node "{{leader}}" is released
    And the CLI REPL runs "CREATE RESOURCE repl_admitted;"
    Then the CLI REPL eventually displays "repeating the uncertain statement under execution reference"
    And the CLI REPL eventually displays "created resource 'repl_admitted'"
    When the CLI REPL runs "CREATE RESOURCE repl_cancelled;"
    Then the CLI REPL eventually displays "created resource 'repl_cancelled'"
    When the CLI REPL runs "exit"
    Then the CLI REPL ends successfully

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The CLI REPL refuses an upload inside a transaction and recovers an uncertain upload by running it again
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "repl_bundle" containing
      """
      {
        "readme.txt": "an upload fixture of the CLI REPL\n"
      }
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE repl_bundle;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI REPL is started on node "{{leader}}"
    And the CLI REPL runs "BEGIN;"
    Then the CLI REPL eventually displays "transaction started"
    When the CLI REPL runs "UPLOAD RESOURCE repl_bundle VERSION '{{repl_bundle}}';"
    Then the CLI REPL eventually displays "error: client-local commands are not allowed while a transaction is active"
    When the CLI REPL runs "REVERT;"
    Then the CLI REPL eventually displays "transaction reverted"
    Given resource installation on node "{{leader}}" pauses before promotion
    When the CLI REPL runs "UPLOAD RESOURCE repl_bundle VERSION '{{repl_bundle}}';"
    Then the resource installation pause on node "{{leader}}" is reached
    When the CLI REPL receives Ctrl-C
    Then the CLI REPL eventually displays "upload outcome: not known yet"
    When the resource installation pause on node "{{leader}}" is released
    And the CLI REPL runs "UPLOAD RESOURCE repl_bundle VERSION '{{repl_bundle}}';"
    Then the CLI REPL eventually displays "repeating the uncertain upload under upload identity"
    And the CLI REPL eventually displays "uploaded resource version 1"
    When the CLI REPL runs "exit"
    Then the CLI REPL ends successfully

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An uncertain CLI upload is recovered by repeating it under its upload identity
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "uncertain_bundle" containing
      """
      {
        "readme.txt": "an upload fixture of an uncertain CLI upload\n"
      }
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE uncertain_bundle;
      """
    Then the current leader node is saved as placeholder "leader"
    Given resource installation on node "{{leader}}" pauses before promotion
    When the CLI starts executing "UPLOAD RESOURCE uncertain_bundle VERSION '{{uncertain_bundle}}';" on node "{{leader}}"
    Then the resource installation pause on node "{{leader}}" is reached
    When the started CLI receives Ctrl-C
    Then the started CLI exits with status 3 and prints "upload outcome: not known yet"
    And the upload identity the CLI printed is saved as placeholder "uncertain_identity"
    When the resource installation pause on node "{{leader}}" is released
    And the CLI executes "UPLOAD RESOURCE uncertain_bundle VERSION '{{uncertain_bundle}}';" under upload identity "{{uncertain_identity}}" on node "{{leader}}"
    Then the CLI exited with status 0 and printed "uploaded resource version 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
