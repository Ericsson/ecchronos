Feature: API to get scheduled repair status

  Scenario: Get repair status for table test.table1
    Given I have a json schema in repair_job_list.json
    And I use the url http://localhost:8080/repair-management/v1/status/keyspaces/test/tables/table1
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_job_list

  Scenario: Get repair status for all scheduled repairs
    Given I have a json schema in repair_job_list.json
    And I use the url http://localhost:8080/repair-management/v1/status
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_job_list

  Scenario: Get repair status for all scheduled repairs in the keyspace test
    Given I have a json schema in repair_job_list.json
    And I use the url http://localhost:8080/repair-management/v1/status/keyspaces/test
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_job_list
    And the job list contains only keyspace test