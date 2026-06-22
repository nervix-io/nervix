Feature: Internal TLS
  Scenario Outline: Resources replicate over HTTPS cluster api
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And cluster internal transports are configured with cluster api mode "https" and interconnect mode "http"
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

  Scenario: Interconnect peers connect over TLS
    Given cluster internal transports are configured with cluster api mode "http" and interconnect mode "https"
    And a 3 node nervix cluster is started
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"

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
