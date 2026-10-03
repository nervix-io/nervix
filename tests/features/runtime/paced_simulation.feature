Feature: Paced simulation drivers
  The runnable drivers under examples/paced-simulation attach to the domain clock, submit sensor
  readings stamped with the tick centers the clock has reached through a client ingestor, and
  apply and acknowledge the readings the graph constructs through attached client emitters. The
  rust driver is built on the Rust client library and the python driver on the shared C binding.
  Every scenario runs the published programs against the published example graph.

  @paced_simulation
  Scenario Outline: The <driver> driver paces readings by the attached clock at time rate <time_rate>
    Given a 1 node nervix cluster is started
    And the active domain is "paced_simulation"
    And the leader node is configured with the paced simulation example graph
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE <time_rate>;
      """
    And the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 12 --sensors 3"
    Then within "120s" the paced simulation driver exits with status 0
    And the paced simulation driver reports
      | readings          | 36 |
      | completed         | 36 |
      | not_admitted      | 0  |
      | processing_failed | 0  |
      | outcome_unknown   | 0  |
      | effects           | 36 |
      | rejection_notices | 0  |
      | duplicates        | 0  |
      | generations       | 1  |
    And the paced simulation effects hold every reading of its ledger exactly once
    And the paced simulation readings of every sensor occur one clock period apart
    And every paced simulation reading was admitted no earlier than it occurred
    And the paced simulation driver ran for at least 10 clock periods at time rate "<time_rate>"

    Examples:
      | driver | time_rate |
      | rust   | 0.5       |
      | rust   | 4.0       |
      | python | 0.5       |
      | python | 4.0       |

  @paced_simulation
  Scenario Outline: The <driver> driver keeps pacing through a node that executes none of its endpoints while the clock authority moves away
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the active domain is "paced_simulation"
    And the leader node is configured with the paced simulation example graph
    # The committed clock authority of a domain is chosen deterministically from its name, and
    # among three nodes the authority of paced_simulation is node-3.
    And domain clock progress for domain "{{domain}}" on node "node-3" is paused before delivery
    When leadership is transferred to node "node-1"
    And these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 1.0;
      """
    Then domain clock progress for domain "{{domain}}" on node "node-3" reaches the delivery pause within the authority observation budget
    When domain clock progress for domain "{{domain}}" on node "node-3" resumes
    And these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR simulated_readings ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER observed_readings ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER rejection_notices ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE RELAY readings ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY rejected_readings ONTO NODE node-1 IGNORE PREFERENCES;
      """
    Then within "30s" the leader node describes ingestor "simulated_readings" with
      """
      owner: node-2
      """
    When the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 60 --sensors 2"
    Then within "60s" the paced simulation driver prints a line starting with "OUTCOME tick="
    When node "node-3" is stopped
    Then within "180s" the paced simulation driver exits with status 0
    And the paced simulation driver reports
      | readings          | 120 |
      | completed         | 120 |
      | effects           | 120 |
      | rejection_notices | 0   |
      | generations       | 1   |
    And the paced simulation effects hold every reading of its ledger exactly once
    And within "30s" the leader node describes ingestor "simulated_readings" with
      """
      owner: node-2
      forwarded producers: 0
      outstanding batches: 0
      """

    Examples:
      | driver |
      | rust   |
      | python |

  @paced_simulation
  Scenario Outline: The <driver> driver's readings stamped before the admission window are rejected under TIMESTAMP AT and admitted under TIMESTAMP NOW on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "paced_simulation"
    And the leader node is configured with the paced simulation example graph
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 2.0;
      """
    And the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 9 --sensors 2 --invalid-every 3"
    Then within "120s" the paced simulation driver exits with status 0
    And the paced simulation driver reports
      | readings          | 18 |
      | completed         | 18 |
      | effects           | 15 |
      | rejection_notices | 3  |
      | duplicates        | 0  |
    And the paced simulation rejection notices name every reading its ledger stamped before the admission window
    When the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 9 --sensors 2 --invalid-every 3 --timestamps now"
    Then within "120s" the paced simulation driver exits with status 0
    And the paced simulation driver reports
      | readings          | 18 |
      | completed         | 18 |
      | effects           | 18 |
      | rejection_notices | 0  |
      | duplicates        | 0  |
    And the paced simulation effects hold every reading of its ledger exactly once
    And the paced simulation effects show TIMESTAMP NOW admitting the readings stamped before the admission window

    Examples:
      | driver | cluster_size |
      | rust   | 1            |
      | rust   | 3            |
      | python | 1            |
      | python | 3            |

  @paced_simulation
  Scenario Outline: The <driver> driver stops with its domain and continues in the next START generation without mixing generations on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "paced_simulation"
    And the leader node is configured with the paced simulation example graph
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 1.0;
      """
    And the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 300 --sensors 2 --follow-generations"
    Then within "60s" the paced simulation driver prints a line starting with "OUTCOME tick="
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Then within "60s" the paced simulation driver prints a line starting with "CLOCK generation=1 state=stopped"
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 4.0;
      """
    Then within "60s" the paced simulation driver prints a line starting with "REOPENED generation=2"
    And within "180s" the paced simulation driver finishes with the status its outcomes imply
    And the paced simulation driver reports
      | generations | 2 |
      | duplicates  | 0 |
    And every completed paced simulation reading has exactly one effect from its own generation
    And the paced simulation ledger submits no reading of generation 1 after generation 2 begins

    Examples:
      | driver | cluster_size |
      | rust   | 1            |
      | rust   | 3            |
      | python | 1            |
      | python | 3            |

  @paced_simulation
  Scenario Outline: The <driver> driver reports unknown outcomes after its session is cut, and the <replayer> driver replays them without repeating an effect
    Given a 1 node nervix cluster is started
    And the active domain is "paced_simulation"
    And the leader node is configured with the paced simulation example graph
    And the gRPC endpoint of node "node-1" is forwarded from fixture address "127.0.0.1"
    # At half real time the admission window retains 51 seconds of wall time, which the replay
    # below stays well inside.
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 0.5;
      """
    And the <driver> paced simulation driver runs through the forwarded gRPC endpoint of node "node-1" with arguments "--ticks 12 --sensors 2 --processing-time 1s"
    Then within "60s" the paced simulation driver prints a line starting with "PROCESSING"
    When the TCP forwarder at "127.0.0.1" stops
    Then within "60s" the paced simulation driver prints a line starting with "INTERRUPTED clock"
    And within "60s" the paced simulation driver prints a line containing "outcome_unknown session_lost"
    When the TCP forwarder at "127.0.0.1" restarts
    Then within "180s" the paced simulation driver exits with status 3
    And the paced simulation driver reports
      | outcome_unknown | >= 1 |
      | generations     | 1    |
    When the <replayer> paced simulation driver runs through the forwarded gRPC endpoint of node "node-1" with arguments "--ticks 0 --replay"
    Then within "120s" the paced simulation driver exits with status 0
    And the paced simulation driver reports
      | readings        | >= 1 |
      | completed       | >= 1 |
      | outcome_unknown | 0    |
    And the paced simulation effects hold every reading of its ledger exactly once

    Examples:
      | driver | replayer |
      | rust   | python   |
      | python | rust     |

  @paced_simulation
  Scenario Outline: The <driver> driver saturates its producer on one session while its consumers acknowledge, commands complete and the clock keeps ticking on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "paced_simulation"
    And the leader node is configured with the paced simulation example graph
    # One batch of 300 readings fills the byte credit, so each tick's batch waits for the one
    # before it to complete while the consumers acknowledge its output.
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 0.5;
      """
    And the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 12 --sensors 2 --burst 150 --credit-batches 4 --credit-bytes 24KiB --inspect-every 200ms"
    Then within "300s" the paced simulation driver exits with status 0
    And the paced simulation driver reports
      | readings               | 3600     |
      | completed              | 3600     |
      | effects                | 3600     |
      | duplicates             | 0        |
      | credit_waits           | >= 1     |
      | peak_outstanding_bytes | <= 24576 |
      | inspections            | >= 2     |
      | ticks_observed         | >= 2     |
    And the paced simulation effects hold every reading of its ledger exactly once
    And every paced simulation reading was admitted no earlier than it occurred

    Examples:
      | driver | cluster_size |
      | rust   | 1            |
      | rust   | 3            |
      | python | 1            |
      | python | 3            |

  @paced_simulation
  Scenario Outline: The <driver> driver's output waits for its consumers with a bounded backlog and drains as competing consumers join and leave
    Given a 1 node nervix cluster is started
    And the active domain is "paced_simulation"
    And the leader node is configured with the paced simulation example graph
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 2.0;
      """
    And the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 20 --sensors 2 --credit-batches 2 --consumers 2 --consumer-delay 5s --consumer-leave-after 3"
    Then within "60s" the paced simulation driver prints a line starting with "WAITING credit"
    And within "30s" the leader node describes emitter "observed_readings" retaining output for no consumer
    And within "120s" the paced simulation driver exits with status 0
    And the paced simulation driver reports
      | readings         | 40   |
      | completed        | 40   |
      | effects          | 40   |
      | credit_waits     | >= 1 |
      | consumers_joined | 2    |
      | consumers_left   | 1    |
    And the paced simulation effects hold every reading of its ledger exactly once

    Examples:
      | driver |
      | rust   |
      | python |

  @paced_simulation
  Scenario Outline: The <driver> driver fails clearly on a graph or domain it cannot run
    Given a 1 node nervix cluster is started
    And the active domain is "paced_simulation"
    And the leader node is configured with the paced simulation example graph
    When the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 5"
    Then within "60s" the paced simulation driver exits with status 2
    And the paced simulation driver's errors contain "the clock of domain 'paced_simulation' is stopped at generation 0; START the domain before running the simulation"
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 2.0;
      """
    And the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 5 --emitter missing_output"
    Then within "60s" the paced simulation driver exits with status 2
    And the paced simulation driver's errors contain "emitter 'missing_output' of domain 'paced_simulation' refused the consumer: endpoint not found"
    When the <driver> paced simulation driver runs through node "node-1" with arguments "--ticks 5 --domain missing_domain"
    Then within "60s" the paced simulation driver exits with status 2
    And the paced simulation driver's errors contain "cannot attach to the clock of domain 'missing_domain'"

    Examples:
      | driver |
      | rust   |
      | python |
