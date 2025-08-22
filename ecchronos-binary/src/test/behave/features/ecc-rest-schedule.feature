Feature: API to get schedule status

  Scenario: Get schedule status for all repairs
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules

  Scenario: Get schedule status for all repairs in the keyspace test
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the job list contains only keyspace test

  Scenario: Get schedule status for table test.table1 and then get by id
    Given I use the url localhost:8080/state/nodes
    When I send a GET request
    Then the response is successful
    And the first nodeid from response is extracted from the node list
    Given I have a json schema schedules
    And I fetch schedules with nodeid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules

  Scenario: Get full schedule status for table test.table1 and then get by id
    Given I use the url localhost:8080/state/nodes
    When I send a GET request
    Then the response is successful
    And the first nodeid from response is extracted from the node list
    Given I have a json schema full_schedules
    And I fetch schedules with nodeid and full
    When I send a GET request
    Then the response is successful
    And the response matches the json schema full_schedules

  Scenario: Get schedule status for table test.table1 and then get by nodeid and jobid
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the nodeid from response is extracted for test.table1
    And the jobid from response is extracted for test.table1
    Given I have a json schema schedule
    And I fetch schedules with nodeid and jobid
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedule

  Scenario: Get full schedule status for table test.table1 and then get by nodeid and jobid
    Given I have a json schema schedules
    And I use the url localhost:8080/repair-management/schedules?keyspace=test&table=table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules
    And the nodeid from response is extracted for test.table1
    And the jobid from response is extracted for test.table1
    Given I have a json schema full_schedule
    And I fetch schedules with nodeid and jobid and full
    When I send a GET request
    Then the response is successful
    And the response matches the json schema full_schedule

  Scenario: Get schedule status for table test.table1 and then get by id keyspace and table
    Given I use the url localhost:8080/state/nodes
    When I send a GET request
    Then the response is successful
    And the first nodeid from response is extracted from the node list
    Given I have a json schema schedules
    And I fetch schedules with nodeid keyspace test and table table1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules

  Scenario: Get schedule status for table test.table1 and then get by id keyspace
    Given I use the url localhost:8080/state/nodes
    When I send a GET request
    Then the response is successful
    And the first nodeid from response is extracted from the node list
    Given I have a json schema schedules
    And I fetch schedules with nodeid and keyspace test
    When I send a GET request
    Then the response is successful
    And the response matches the json schema schedules