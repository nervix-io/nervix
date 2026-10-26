Feature: Declared connector client configuration
  Nervix checks client configuration before committing it. Plexus completion and public
  inspection use the same key declarations, including the ownership of driver properties.

  Scenario Outline: A Redis client refuses a subscription buffer outside its production range
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "invalid client config key 'subscription_buffer_messages'"
      """
      CREATE REDIS CLIENT checked POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:6379/0',
        'subscription_buffer_messages' = '18446744073709551615'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WebSockets client refuses a rendered limit outside its portable range
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "handshake_buffer_capacity"
      """
      CREATE WEBSOCKETS CLIENT checked CONFIG {
        'endpoint' = 'ws://127.0.0.1:8080',
        'handshake_buffer_capacity' = '{{ 65536 }}'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Prometheus client refuses a rendered API root with a query
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "addr"
      """
      CREATE PROMETHEUS CLIENT checked CONFIG {
        'addr' = '{{ "http://127.0.0.1:9090/?query=private-api-value" }}',
        'timeout_ms' = '100'
      };
      """
    Then the last command error does not contain
      """
      private-api-value
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WebSockets client accepts its rendered portable limits before connecting
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE WEBSOCKETS CLIENT checked CONFIG {
        'endpoint' = 'ws://127.0.0.1:8080',
        'handshake_buffer_capacity' = '{{ 16 }}',
        'idle_timeout_ms' = '1000',
        'max_frame_size' = '4096',
        'max_message_size' = '8192',
        'ping_interval_ms' = '100'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      CREATE WEBSOCKETS CLIENT checked
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Redis client accepts its optional subscription settings before connecting
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE REDIS CLIENT checked POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:6379/0',
        'idle_timeout_ms' = '1000',
        'subscription_buffer_messages' = '1024'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      'subscription_buffer_messages' = '1024'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Public client inspection redacts a delegated cloud endpoint query
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE AZURE_BLOB CLIENT checked CONFIG {
        'endpoint' = 'https://account.blob.example.com/?sig=private-cloud-signature'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      'endpoint' = '********'
      """
    And the last command output does not contain
      """
      private-cloud-signature
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Kafka client refuses a host-owned delivery report property
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "client config key 'delivery.report.only.error' is reserved for Nervix"
      """
      CREATE KAFKA CLIENT checked CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092',
        'delivery.report.only.error' = 'true'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client refuses a duplicate key before commit
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "duplicate client config key 'addr'"
      """
      CREATE MQTT CLIENT checked CONFIG {
        'addr' = 'mqtt://127.0.0.1:1883',
        'addr' = 'mqtt://127.0.0.1:1884'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client refuses a missing required key before commit
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "missing required client config key 'addr'"
      """
      CREATE MQTT CLIENT checked CONFIG { 'client_id' = 'checked' };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client refuses a value outside a declared key type
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "invalid client config key 'bind'"
      """
      CREATE ZEROMQ CLIENT checked CONFIG {
        'addr' = 'tcp://127.0.0.1:5555',
        'bind' = 'sometimes'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A TLS client refuses a missing required trust file before commit
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "client config key 'addr' requires 'tls_ca_file'"
      """
      CREATE MQTT CLIENT checked CONFIG { 'addr' = 'mqtts://127.0.0.1:8883' };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client accepts a readable file through its pinned resource mount
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE trust_files;
      """
    And node "node-1" has resource directory "trust_fixture" containing
      """
      {"ca.pem":"trust material is inspected by the connector when it initializes\n"}
      """
    When these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE trust_files VERSION "{{trust_fixture}}";
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE MQTT CLIENT checked
      MOUNT trust_files VERSION 1
      CONFIG {
        'addr' = 'mqtts://127.0.0.1:8883',
        'client_id' = 'config-{{ instance }}',
        'tls_ca_file' = '{{ trust_files }}/ca.pem'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      MOUNT trust_files VERSION 1
      """
    And the last command output contains
      """
      'tls_ca_file' = '{{ trust_files }}/ca.pem'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client refuses a configured trust path that is not a readable file
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "client config file key 'tls_ca_file' is not a readable file"
      """
      CREATE MQTT CLIENT checked CONFIG {
        'addr' = 'mqtts://127.0.0.1:8883',
        'tls_ca_file' = '<trust_path>'
      };
      """

    Examples:
      | cluster_size | trust_path                                            |
      | 1            | /nervix-client-config-86bchxeu5/missing-authority.pem |
      | 3            | /nervix-client-config-86bchxeu5/missing-authority.pem |
      | 1            | /                                                     |
      | 3            | /                                                     |

  Scenario Outline: A client refuses an unpaired TLS identity before commit
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "client config key 'tls_cert_file' requires 'tls_key_file'"
      """
      CREATE MQTT CLIENT checked CONFIG {
        'addr' = 'mqtts://127.0.0.1:8883',
        'tls_cert_file' = 'client.pem'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Nervix-owned client refuses an undeclared key before commit
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "unknown client config key 'fetch.wait.max.ms'"
      """
      CREATE MQTT CLIENT checked CONFIG {
        'addr' = 'mqtt://127.0.0.1:1883',
        'fetch.wait.max.ms' = '100'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A driver-delegated client accepts its offline-validated property
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE KAFKA CLIENT checked CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092',
        'fetch.wait.max.ms' = '100'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      'fetch.wait.max.ms' = '100'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A NATS client accepts its declared core write timeout
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE NATS CLIENT checked CONFIG {
        'addr' = 'nats://127.0.0.1:4222',
        'timeout_ms' = '100'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      'timeout_ms' = '100'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A driver-delegated client refuses an invalid property value before commit
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "invalid client config key 'fetch.wait.max.ms'"
      """
      CREATE KAFKA CLIENT checked CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092',
        'fetch.wait.max.ms' = 'not-a-number'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A database client refuses an address the driver cannot parse
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "invalid client config key 'addr'"
      """
      CREATE MYSQL CLIENT checked POOL SIZE MIN 0 MAX 4 CONFIG {
        'addr' = 'http://127.0.0.1/database'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Public client inspection redacts a declared secret
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SQS CLIENT checked CONFIG {
        'endpoint' = 'http://127.0.0.1:4566',
        'region' = 'us-east-1',
        'access_key_id' = 'test-key',
        'secret_access_key' = 'task-config-secret'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      'secret_access_key' = '********'
      """
    And the last command output does not contain
      """
      task-config-secret
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output does not contain
      """
      task-config-secret
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A transaction refuses a driver URL without exposing its credential and remains usable
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these Plexus commands
      """
      BEGIN;
      """
    When client "owner" fails to execute these Plexus commands
      """
      CREATE MYSQL CLIENT checked POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'http://user:private-driver-credential@127.0.0.1:3306/database'
      };
      """
    Then the last command error contains
      """
      invalid client config key 'addr'
      """
    And the last command error does not contain
      """
      private-driver-credential
      """
    When client "owner" executes these Plexus commands
      """
      CREATE REDIS CLIENT valid POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:6379/0'
      };
      COMMIT;
      SHOW CREATE CLIENT valid;
      """
    Then the last command output contains
      """
      CREATE REDIS CLIENT valid
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
