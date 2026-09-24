Feature: IP address, network and URL functions
  Scenario Outline: Addresses and URLs parse into typed addresses and URL Standard components
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA raw_endpoint (
        id STRING,
        address STRING,
        url STRING
      );
      CREATE SCHEMA address_view (
        id STRING,
        address BYTES,
        canonical STRING,
        family I64,
        subnet STRING,
        unmapped STRING,
        in_ten BOOL,
        in_ten_as_written BOOL,
        in_documentation BOOL
      );
      CREATE SCHEMA url_view (
        id STRING,
        scheme STRING,
        host STRING,
        port I64,
        path STRING,
        decoded_path STRING,
        query STRING,
        fragment STRING,
        search STRING,
        tags <tags_type>
      );
      CREATE CODEC raw_endpoint_batch_codec
        FROM JSON
        TO SCHEMA raw_endpoint
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY raw_endpoints SCHEMA raw_endpoint UNBRANCHED;
      CREATE RELAY address_views SCHEMA address_view UNBRANCHED;
      CREATE RELAY url_views SCHEMA url_view UNBRANCHED;
      CREATE VHOST edge network-values-{{test_id}}.example.com;
      CREATE ENDPOINT endpoint_ingress ON edge PATH '/endpoints' TYPE HTTP;
      CREATE INGESTOR endpoint_source
        FROM ENDPOINT endpoint_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING raw_endpoint_batch_codec
        TO raw_endpoints
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION inspect_endpoints
        FROM raw_endpoints
        UNBRANCHED
        TO address_views
          SET id = input.id,
              address = ip_from_string(input.address),
              canonical = ip_to_string(output.address),
              family = ip_family(output.address),
              subnet = ip_to_string(ip_trunc(output.address, IF output.family = 4 THEN 24 ELSE 48 END)),
              unmapped = ip_to_string(ip_unmap(output.address)),
              in_ten = ip_in_network(ip_unmap(output.address), '10.0.0.0/8'),
              in_ten_as_written = ip_in_network(output.address, '10.0.0.0/8'),
              in_documentation = ip_in_network(output.address, '2001:db8::/32')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO url_views
          SET id = input.id,
              scheme = url_scheme(input.url),
              host = coalesce(url_host(input.url), 'absent'),
              port = coalesce(url_port(input.url), -1),
              path = url_path(input.url),
              decoded_path = url_decode(output.path),
              query = coalesce(url_query(input.url), 'absent'),
              fragment = coalesce(url_fragment(input.url), 'absent'),
              search = coalesce(url_query_value(input.url, 'q'), 'absent'),
              tags = url_query_values(input.url, 'tag')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION address_views_subscription TO address_views;
      CREATE SUBSCRIPTION url_views_subscription TO url_views;
      START;
      """
    And http payload is posted to node "node-1" with host "network-values-{{test_id}}.example.com" path "/endpoints"
      """
      [{"id":"private-v4","address":"10.1.2.3","url":"HTTPS://Example.COM:8443/a/./b/../caf%C3%A9?q=hello+world%21&tag=x&tag=y%26z#Frag"},{"id":"broadcast","address":"255.255.255.255","url":"http://bücher.example/"},{"id":"unspecified-v4","address":"0.0.0.0","url":"http://[2001:DB8::1]:80/path%20with%20space?q="},{"id":"mapped","address":"::FFFF:10.9.8.7","url":"mailto:ops@example.com"},{"id":"documentation","address":"2001:DB8:0:0:0:0:0:1","url":"https://example.com/?tag=&tag=a+b&q=first&q=second"},{"id":"all-ones-v6","address":"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff","url":"ftp://files.example/pub/"},{"id":"unspecified-v6","address":"::","url":"urn:isbn:0451450523"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"private-v4" | "address":"CgECAw==" | "canonical":"10.1.2.3" | "family":4 | "subnet":"10.1.2.0" | "unmapped":"10.1.2.3" | "in_ten":true | "in_ten_as_written":true | "in_documentation":false
      "id":"broadcast" | "canonical":"255.255.255.255" | "family":4 | "subnet":"255.255.255.0" | "in_ten":false | "in_documentation":false
      "id":"unspecified-v4" | "address":"AAAAAA==" | "canonical":"0.0.0.0" | "subnet":"0.0.0.0" | "in_ten":false
      "id":"mapped" | "canonical":"::ffff:10.9.8.7" | "family":6 | "subnet":"::" | "unmapped":"10.9.8.7" | "in_ten":true | "in_ten_as_written":false | "in_documentation":false
      "id":"documentation" | "canonical":"2001:db8::1" | "family":6 | "subnet":"2001:db8::" | "unmapped":"2001:db8::1" | "in_ten":false | "in_documentation":true
      "id":"all-ones-v6" | "canonical":"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff" | "subnet":"ffff:ffff:ffff::" | "in_documentation":false
      "id":"unspecified-v6" | "address":"AAAAAAAAAAAAAAAAAAAAAA==" | "canonical":"::" | "family":6 | "subnet":"::" | "in_documentation":false
      "id":"private-v4" | "scheme":"https" | "host":"example.com" | "port":8443 | "path":"/a/caf%C3%A9" | "decoded_path":"/a/café" | "query":"q=hello+world%21&tag=x&tag=y%26z" | "fragment":"Frag" | "search":"hello world!" | "tags":["x","y&z"]
      "id":"broadcast" | "scheme":"http" | "host":"xn--bcher-kva.example" | "port":80 | "path":"/" | "query":"absent" | "fragment":"absent" | "search":"absent" | "tags":[]
      "id":"unspecified-v4" | "host":"2001:db8::1" | "port":80 | "path":"/path%20with%20space" | "decoded_path":"/path with space" | "query":"q=" | "search":"" | "tags":[]
      "id":"mapped" | "scheme":"mailto" | "host":"absent" | "port":-1 | "path":"ops@example.com" | "query":"absent" | "search":"absent"
      "id":"documentation" | "host":"example.com" | "port":443 | "path":"/" | "query":"tag=&tag=a+b&q=first&q=second" | "search":"first" | "tags":["","a b"]
      "id":"all-ones-v6" | "scheme":"ftp" | "host":"files.example" | "port":21 | "path":"/pub/"
      "id":"unspecified-v6" | "scheme":"urn" | "host":"absent" | "port":-1 | "path":"isbn:0451450523"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count | tags_type   |
      | 1            | 0             | VEC<STRING> |
      | 3            | 0             | VEC<STRING> |

  Scenario Outline: A route filter selects messages by the network their typed address belongs to
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA raw_client (
        id STRING,
        client STRING
      );
      CREATE SCHEMA internal_client (
        id STRING,
        client BYTES
      );
      CREATE CODEC raw_client_batch_codec
        FROM JSON
        TO SCHEMA raw_client
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY raw_clients SCHEMA raw_client UNBRANCHED;
      CREATE RELAY internal_clients SCHEMA internal_client UNBRANCHED;
      CREATE VHOST edge network-filter-{{test_id}}.example.com;
      CREATE ENDPOINT client_ingress ON edge PATH '/clients' TYPE HTTP;
      CREATE INGESTOR client_source
        FROM ENDPOINT client_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING raw_client_batch_codec
        TO raw_clients
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION select_internal_clients
        FROM raw_clients
        UNBRANCHED
        TO internal_clients
          SET id = input.id,
              client = ip_from_string(input.client)
          WHERE ip_in_network(ip_unmap(output.client), '10.0.0.0/8')
            OR ip_in_network(output.client, 'fd00::/8')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION internal_clients_subscription TO internal_clients;
      START;
      """
    And http payload is posted to node "node-1" with host "network-filter-{{test_id}}.example.com" path "/clients"
      """
      [{"id":"public-v4","client":"203.0.113.9"},{"id":"ten-network","client":"10.20.30.40"},{"id":"below-ten","client":"9.255.255.255"},{"id":"mapped-ten","client":"::ffff:10.0.0.1"},{"id":"above-ten","client":"11.0.0.0"},{"id":"unique-local","client":"fd12:3456::1"},{"id":"public-v6","client":"2001:db8::5"},{"id":"public-last","client":"192.0.2.1"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"ten-network" | "client":"ChQeKA=="
      "id":"mapped-ten"
      "id":"unique-local"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Malformed addresses, prefixes, networks, URLs and escapes fail only their message, and guards route them explicitly
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA raw_probe (
        id STRING,
        address STRING OPTIONAL,
        network STRING,
        prefix I64,
        url STRING OPTIONAL,
        escaped STRING
      );
      CREATE SCHEMA strict_probe (
        id STRING,
        subnet BYTES OPTIONAL,
        member BOOL OPTIONAL,
        host STRING OPTIONAL,
        decoded STRING
      );
      CREATE SCHEMA guarded_probe (
        id STRING,
        address_state STRING,
        url_state STRING,
        address BYTES OPTIONAL,
        host STRING OPTIONAL
      );
      CREATE SCHEMA probe_error (
        input_id STRING,
        error_code STRING,
        error_message STRING
      );
      CREATE CODEC raw_probe_batch_codec
        FROM JSON
        TO SCHEMA raw_probe
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY raw_probes SCHEMA raw_probe UNBRANCHED;
      CREATE RELAY strict_probes SCHEMA strict_probe UNBRANCHED;
      CREATE RELAY guarded_probes SCHEMA guarded_probe UNBRANCHED;
      CREATE RELAY probe_errors SCHEMA probe_error UNBRANCHED;
      CREATE VHOST edge network-errors-{{test_id}}.example.com;
      CREATE ENDPOINT probe_ingress ON edge PATH '/probes' TYPE HTTP;
      CREATE INGESTOR probe_source
        FROM ENDPOINT probe_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING raw_probe_batch_codec
        TO raw_probes
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION probe_networks
        FROM raw_probes
        UNBRANCHED
        TO strict_probes
          SET id = input.id,
              subnet = ip_trunc(ip_from_string(input.address), input.prefix),
              member = ip_in_network(ip_from_string(input.address), input.network),
              host = url_host(input.url),
              decoded = url_decode(input.escaped)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO probe_errors
          SET input_id = input.id,
              error_code = error.code,
              error_message = error.message
        TO guarded_probes
          SET id = input.id,
              address_state = CASE
                WHEN is_null(input.address) THEN 'missing'
                WHEN is_ip_address(input.address) THEN 'address'
                ELSE 'malformed'
              END,
              url_state = CASE
                WHEN is_null(input.url) THEN 'missing'
                WHEN is_url(input.url) THEN 'url'
                ELSE 'malformed'
              END,
              address = CASE WHEN is_ip_address(input.address) THEN ip_from_string(input.address) END,
              host = CASE WHEN is_url(input.url) THEN url_host(input.url) END
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION strict_probes_subscription TO strict_probes;
      CREATE SUBSCRIPTION guarded_probes_subscription TO guarded_probes;
      CREATE SUBSCRIPTION probe_errors_subscription TO probe_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "network-errors-{{test_id}}.example.com" path "/probes"
      """
      [{"id":"valid","address":"192.168.1.20","network":"192.168.0.0/16","prefix":24,"url":"https://example.com/x","escaped":"a%20b"},{"id":"leading-zero","address":"010.0.0.1","network":"10.0.0.0/8","prefix":8,"url":"https://example.com/","escaped":"x"},{"id":"zone-index","address":"fe80::1%eth0","network":"fe80::/10","prefix":64,"url":"https://example.com/","escaped":"x"},{"id":"wide-prefix","address":"10.0.0.1","network":"10.0.0.0/8","prefix":33,"url":"https://example.com/","escaped":"x"},{"id":"host-bits","address":"10.0.0.1","network":"10.0.0.1/8","prefix":8,"url":"https://example.com/","escaped":"x"},{"id":"family-prefix","address":"10.0.0.1","network":"10.0.0.0/33","prefix":8,"url":"https://example.com/","escaped":"x"},{"id":"relative-url","address":"10.0.0.1","network":"10.0.0.0/8","prefix":8,"url":"/search?q=x","escaped":"x"},{"id":"bad-escape","address":"10.0.0.1","network":"10.0.0.0/8","prefix":8,"url":"https://example.com/","escaped":"100%"},{"id":"missing","address":null,"network":"10.0.0.0/8","prefix":8,"url":null,"escaped":"x"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"valid" | "subnet":"wKgBAA==" | "member":true | "host":"example.com" | "decoded":"a b"
      "id":"missing" | "decoded":"x"
      "input_id":"leading-zero" | "error_code":"evaluation" | cast_failed: ip_from_string input is not an IPv4 or IPv6 address
      "input_id":"zone-index" | cast_failed: ip_from_string input is not an IPv4 or IPv6 address
      "input_id":"wide-prefix" | invalid_argument: ip_trunc prefix length must be 0 to 32 for an IPv4 address
      "input_id":"host-bits" | invalid_argument: ip_in_network network has host bits set past its prefix length
      "input_id":"family-prefix" | invalid_argument: ip_in_network network prefix length must be 0 to 32 for an IPv4 network
      "input_id":"relative-url" | cast_failed: url_host input is not an absolute URL: relative URL without a base
      "input_id":"bad-escape" | cast_failed: url_decode input is not valid percent-encoded UTF-8
      "id":"valid" | "address_state":"address" | "url_state":"url" | "address":"wKgBFA==" | "host":"example.com"
      "id":"leading-zero" | "address_state":"malformed" | "url_state":"url"
      "id":"zone-index" | "address_state":"malformed"
      "id":"relative-url" | "address_state":"address" | "url_state":"malformed"
      "id":"missing" | "address_state":"missing" | "url_state":"missing"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Network functions keep the sensitivity of the values they read
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA secret_visit (
        id STRING,
        client STRING SENSITIVE,
        referrer STRING SENSITIVE
      );
      CREATE SCHEMA visit_facts (
        id STRING,
        subnet STRING SENSITIVE,
        internal BOOL SENSITIVE,
        referrer_host STRING OPTIONAL SENSITIVE,
        campaign STRING OPTIONAL SENSITIVE
      );
      CREATE CODEC secret_visit_codec
        FROM JSON
        TO SCHEMA secret_visit
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY secret_visits SCHEMA secret_visit UNBRANCHED;
      CREATE RELAY visit_facts SCHEMA visit_facts UNBRANCHED;
      CREATE VHOST edge network-sensitive-{{test_id}}.example.com;
      CREATE ENDPOINT visit_ingress ON edge PATH '/visits' TYPE HTTP;
      CREATE INGESTOR visit_source
        FROM ENDPOINT visit_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING secret_visit_codec
        TO secret_visits
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION summarize_visits
        FROM secret_visits
        UNBRANCHED
        TO visit_facts
          SET id = input.id,
              subnet = ip_to_string(ip_trunc(ip_from_string(input.client), 24)),
              internal = ip_in_network(ip_from_string(input.client), '10.0.0.0/8'),
              referrer_host = url_host(input.referrer),
              campaign = url_query_value(input.referrer, 'utm_campaign')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION visit_facts_subscription TO visit_facts;
      START;
      """
    And http payload is posted to node "node-1" with host "network-sensitive-{{test_id}}.example.com" path "/visits"
      """
      {"id":"sensitive","client":"10.1.2.3","referrer":"https://partner.example/landing?utm_campaign=spring"}
      """
    Then the relay subscription receives a payload
      """
      "id":"sensitive"
      """
    And the last relay subscription payload masks field "subnet"
    And the last relay subscription payload masks field "internal"
    And the last relay subscription payload masks field "referrer_host"
    And the last relay subscription payload masks field "campaign"
    And the last relay subscription payload does not contain "10.1.2"
    And the last relay subscription payload does not contain "partner.example"
    And the last relay subscription payload does not contain "spring"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A statement is rejected for an invalid constant network, a text address or a leaked result
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "<error>"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA raw_client (
        id STRING,
        client STRING SENSITIVE
      );
      CREATE SCHEMA client_class (
        id STRING,
        internal BOOL
      );
      CREATE RELAY raw_clients SCHEMA raw_client UNBRANCHED;
      CREATE RELAY client_classes SCHEMA client_class UNBRANCHED;
      CREATE JUNCTION classify_clients
        FROM raw_clients
        UNBRANCHED
        TO client_classes
          SET id = input.id,
              internal = <membership>
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | membership                                                                | error                                                                                  |
      | 1            | ip_in_network(leak_sensitive(ip_from_string(input.client)), '10.0.0.1/8') | function 'ip_in_network' network '10.0.0.1/8' has host bits set past its prefix length |
      | 3            | ip_in_network(leak_sensitive(ip_from_string(input.client)), '10.0.0.1/8') | function 'ip_in_network' network '10.0.0.1/8' has host bits set past its prefix length |
      | 1            | ip_in_network(leak_sensitive(input.client), '10.0.0.0/8')                 | function 'ip_in_network' requires BYTES input, found Utf8                              |
      | 3            | ip_in_network(leak_sensitive(input.client), '10.0.0.0/8')                 | function 'ip_in_network' requires BYTES input, found Utf8                              |
      | 1            | ip_in_network(ip_from_string(input.client), '10.0.0.0/8')                 | would store sensitive data in a non-sensitive output field                             |
      | 3            | ip_in_network(ip_from_string(input.client), '10.0.0.0/8')                 | would store sensitive data in a non-sensitive output field                             |
