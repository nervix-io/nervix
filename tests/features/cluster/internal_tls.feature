Feature: Internal TLS
  Scenario Outline: Resources replicate over the authenticated interconnect
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "secure"
    And node "node-1" has resource directory "resource_dir" containing
      """
      {
        "bundle/model.bin": "hello"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN secure;
      CREATE RESOURCE proto;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE proto VERSION '{{resource_dir}}';
      """
    Then within "10s" node "node-1" eventually reports describe resource as "cluster_ready: true"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    And within "10s" node "node-1" eventually reports describe resource as "resource: proto@1"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Interconnect peers connect with certificate identities
    Given a 3 node nervix cluster is started
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"

  Scenario Outline: Invalid interconnect peer credentials are rejected
    Given a 1 node nervix cluster is started
    When an interconnect peer with "<fault>" credentials attempts to connect to node "node-1"
    Then the interconnect peer is rejected

    Examples:
      | fault                  |
      | untrusted client       |
      | wrong cluster identity |
      | wrong node identity    |
      | mismatched endpoint    |
      | expired certificate    |

  Scenario: Interconnect certificate authority rotates without restarting the cluster
    Given a 3 node nervix cluster is started
    When interconnect certificates are rotated to a new certificate authority
    And node "node-4" is added to the cluster
    Then node "node-1" eventually reports interconnect to "node-4" as "connected"
    And node "node-4" eventually reports interconnect to "node-2" as "connected"

  Scenario Outline: Clients execute NSPL over HTTPS gRPC
    Given client grpc transport is configured with mode "https"
    And a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN secure;
      """
    Then the last command output contains
      """
      created domain 'secure'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
