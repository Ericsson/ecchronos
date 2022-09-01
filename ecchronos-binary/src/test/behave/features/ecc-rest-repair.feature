Feature: API for repairs

  Scenario: Run local repair for table test.table1
    Given I have a json schema in repair_list_v2.json
    And I use the url http://localhost:8080/repair-management/v2/repairs?keyspace=test&table=table1&isLocal=true
    When I send a POST request
    Then the response is successful
    And the response matches the json repair_list_v2
    And the id from response is extracted for test.table1

  Scenario: Get repair status for all repairs
    Given I have a json schema in repair_list_v2.json
    And I use the url http://localhost:8080/repair-management/v2/repairs
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_list_v2

  Scenario: Get repair status for all repairs in the keyspace test
    Given I have a json schema in repair_list_v2.json
    And I use the url http://localhost:8080/repair-management/v2/repairs?keyspace=test
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_list_v2
    And the job list contains only keyspace test

  Scenario: Get repair status for table test.table1 and then get by id
    Given I have a json schema in repair_list_v2.json
    And I use the url http://localhost:8080/repair-management/v2/repairs?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_list_v2
    And the id from response is extracted for test.table1
    Given I have a json schema in repair_list_v2.json
    And I fetch repairs with id
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_list_v2