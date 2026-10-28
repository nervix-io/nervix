@resource_source_discovery @deloxide_stress
Feature: Resource source discovery
  A published version can reach a replica before a usable source appears in that replica's
  discovery view. The absent local copy remains pending until it can be installed.

  # This is a three-node case because the published archive must be held by another node.
  Scenario: A resource upload waits for a replica to discover its source
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "lookup_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    Then the current leader node is saved as placeholder "resource_source"
    And a node other than placeholder "resource_source" is saved as placeholder "replica"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE lookup_bundle;
      """
    Given resource source discovery on node "{{replica}}" is deferred
    And client "uploader" is connected to the leader node
    When client "uploader" selects domain "{{domain}}"
    And client "uploader" begins uploading resource "lookup_bundle" from "{{lookup_dir}}" with identity "deferred-source" in the background
    Then the deferred resource source discovery on node "{{replica}}" is observed
    And within "30s" node "{{replica}}" eventually reports describe resource as "- {{replica}} topology=alive state=pending"
      """
      DESCRIBE RESOURCE lookup_bundle VERSION 1;
      """
    When resource source discovery on node "{{replica}}" resumes
    Then the background Plexus execution succeeds
    And within "30s" node "{{replica}}" eventually reports describe resource as "- {{replica}} topology=alive state=ready"
      """
      DESCRIBE RESOURCE lookup_bundle VERSION 1;
      """
    When these Plexus commands are executed through the client on node "{{replica}}"
      """
      CREATE SCHEMA zip_entry (zip STRING, city STRING);
      CREATE WIRE JSON SCHEMA zip_wire MODE STRICT (zip string, city string);
      CREATE CODEC zip_codec FROM WIRE JSON SCHEMA zip_wire TO SCHEMA zip_entry;
      CREATE HASH MAP zip_by_code
        KEY zip
        FROM RESOURCE lookup_bundle VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING zip_codec;
      LOOKUP zip_by_code KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
