Feature: API to schedule repairs

  Scenario: Schedule repair for table test.table1
    Given I have a json schema in demand_repair_job.json
    And I use the url http://localhost:8080/repair-management/v1/schedule/keyspaces/test/tables/table1
    When I send a GET request
    Then the response is successful
    And the response matches the json demand_repair_job