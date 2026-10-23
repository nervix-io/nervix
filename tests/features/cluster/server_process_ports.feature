Feature: Server process listener ports

  The harness draws the ports a server process binds before the process starts, and until the
  process binds a drawn port any other socket on the runner can take it. A launch whose process
  reports that it could not bind a drawn listener is launched again on fresh ports for that
  listener, within the deadline its startup began with.

  Scenario Outline: A server process starts on a fresh <listener> port when another socket holds the one it drew
    Given another socket holds the "<listener>" port the next nervix-server process draws
    When a nervix-server process is started
    Then the server process was relaunched once because another socket held its "<listener>" port

    Examples:
      | listener    |
      | web console |
      | gRPC        |

  Scenario: A real-process cluster starts when another socket holds the interconnect port its bootstrap member drew
    Given another socket holds the "interconnect" port the bootstrap member of the next nervix-server process cluster draws
    When a 3 node nervix-server process cluster is started
    Then server process node "node-1" was relaunched once because another socket held its "interconnect" port

  Scenario: A restarted real-process cluster member starts on a fresh gRPC port when another socket holds the one it had
    Given a 3 node nervix-server process cluster is started
    When server process node "node-2" receives SIGKILL
    Then server process node "node-2" is terminated by SIGKILL
    When another socket holds the "gRPC" port of server process node "node-2"
    And server process node "node-2" restarts from its existing database
    Then server process node "node-2" was relaunched once because another socket held its "gRPC" port

  Scenario: All persisted voters recover together when two peers must move their gRPC listeners
    Given a 3 node nervix-server process cluster is started
    When all server processes receive SIGKILL
    And another socket holds the "gRPC" port of server process node "node-2"
    And another socket holds the "gRPC" port of server process node "node-3"
    And all server processes restart from their existing databases
    Then server process node "node-2" was relaunched once because another socket held its "gRPC" port
    And server process node "node-3" was relaunched once because another socket held its "gRPC" port

  Scenario: A server process whose startup ends for another reason is not launched again
    When a nervix-server process is started with command-line option "--web-console-tls-cert" set to "/nonexistent/web-console.pem"
    Then the server process startup fails at once with exit status 1 without another launch
