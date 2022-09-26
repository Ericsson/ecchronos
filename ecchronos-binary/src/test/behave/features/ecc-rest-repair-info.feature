Feature: API for repair info

  Scenario: Get repair info for table test1.table1 in the last 5 minutes
    Given I have a json schema in repair_info.json
    And I use the url localhost:8080/repair-management/v2/repairInfo?keyspace=test&table=table1&duration=5m
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_info

  Scenario: Get local repair info for table test1.table1 in the last 5 minutes
    Given I have a json schema in repair_info.json
    And I use the url localhost:8080/repair-management/v2/repairInfo?keyspace=test&table=table1&duration=5m&isLocal=true
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_info

  Scenario: Get repair info for keyspace test1 in the last 5 minutes
    Given I have a json schema in repair_info.json
    And I use the url localhost:8080/repair-management/v2/repairInfo?keyspace=test&duration=5m
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_info

  Scenario: Get repair info for all tables in the last 5 minutes
    Given I have a json schema in repair_info.json
    And I use the url localhost:8080/repair-management/v2/repairInfo?duration=5m
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_info

  Scenario: Get repair info for all tables since epoch
    Given I have a json schema in repair_info.json
    And I use the url localhost:8080/repair-management/v2/repairInfo?since=0
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_info

  Scenario: Get repair info for all tables between epoch and epoch+5 minutes
    Given I have a json schema in repair_info.json
    And I use the url localhost:8080/repair-management/v2/repairInfo?since=0&duration=5m
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_info

  Scenario: Get local repair info for all tables between epoch and epoch+5 minutes
    Given I have a json schema in repair_info.json
    And I use the url localhost:8080/repair-management/v2/repairInfo?since=0&duration=5m&isLocal=true
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_info