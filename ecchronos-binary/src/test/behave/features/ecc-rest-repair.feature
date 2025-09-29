Feature: API for repairs

  Scenario: Run repair for table test.table1
    Given I have a json schema repairs
    And I use the url localhost:8080/repair-management/repairs?keyspace=test&table=table1&all=true
    When I send a POST request
    Then the response is successful
    And the response matches the json schema repairs

  Scenario: Run invalid repair for table test.table1
    Given I have a json schema repairs
    And I use the url localhost:8080/repair-management/repairs?keyspace=test&table=table1
    When I send a POST request
    Then the response is bad request


  Scenario: Get repair status for all repairs
    Given I have a json schema repairs
    And I use the url localhost:8080/repair-management/repairs
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repairs

  Scenario: Get repair status for all repairs in the keyspace test
    Given I have a json schema repairs
    And I use the url localhost:8080/repair-management/repairs?keyspace=test
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repairs
    And the job list contains only keyspace test

  Scenario: Get repair status for table test.table1 and then get by id
    Given I have a json schema repairs
    And I use the url localhost:8080/repair-management/repairs?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repairs
    And the nodeid from response is extracted for test.table1
    Given I have a json schema repairs
    And I fetch repairs with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repairs

  Scenario: Run invalid repair for table twcs table
    Given I have a json schema repairs
    And I use the url localhost:8080/repair-management/repairs?keyspace=ecchronos&table=repair_history&all=true
    When I send a POST request
    Then the repair request failed

  Scenario: Run override repair for table twcs table
    Given I have a json schema repairs
    And I use the url localhost:8080/repair-management/repairs?keyspace=ecchronos&table=repair_history&forceRepairTWCS=true&all=true
    When I send a POST request
    Then the response is successful
