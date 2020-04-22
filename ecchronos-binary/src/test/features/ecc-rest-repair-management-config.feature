Feature: API to get scheduled repair config

  Scenario: Get repair configuration for all scheduled repairs
    Given I have a json schema in repair_config_list.json
    And I use the url http://localhost:8080/repair-management/v1/config
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_config_list

  Scenario: Get repair configuration for all scheduled repairs in the keyspace test
    Given I have a json schema in repair_config_list.json
    And I use the url http://localhost:8080/repair-management/v1/config/keyspaces/test
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_config_list

  Scenario: Get repair configuration for table test.table1
    Given I have a json schema in repair_config.json
    And I use the url http://localhost:8080/repair-management/v1/config/keyspaces/test/tables/table1
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_config
