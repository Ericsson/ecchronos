Feature: API for repair info

  Scenario: Get repair info for table test1.table1 in the last 5 minutes
    Given I use the url localhost:8080/state/nodes
    When I send a GET request
    Then the response is successful
    And the first nodeid from response is extracted from the node list
    Given I have a json schema repair_info
    And I fetch repair info with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repair_info

  Scenario: Get local repair info for table test1.table1 in the last 5 minutes
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the nodeid from response is extracted for test.table1
    Given I have a json schema repair_info
    And I fetch local repair info with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repair_info

  Scenario: Get repair info for keyspace test1 in the last 5 minutes
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the nodeid from response is extracted for test.table1
    Given I have a json schema repair_info
    And I fetch repair for keyspace info with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repair_info

  Scenario: Get repair info for all tables in the last 5 minutes
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the nodeid from response is extracted for test.table1
    Given I have a json schema repair_info
    And I fetch repair for all tables info with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repair_info

  Scenario: Get repair info for all tables since epoch
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the nodeid from response is extracted for test.table1
    Given I have a json schema repair_info
    And I fetch repair info for all tables since epoch with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repair_info

  Scenario: Get repair info for all tables between epoch and epoch+5 minutes
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the nodeid from response is extracted for test.table1
    Given I have a json schema repair_info
    And I fetch repair info for all tables between epoch and epoch+5 minutes with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repair_info

  Scenario: Get local repair info for all tables between epoch and epoch+5 minutes
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the nodeid from response is extracted for test.table1
    Given I have a json schema repair_info
    And I fetch repair local info for all tables between epoch and epoch+5 minutes with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema repair_info