Feature: API to schedule repairs

  Scenario: Schedule repair for table test2.table2
    Given I have a json schema in demand_repair_job.json
    And I use the url http://localhost:8080/repair-management/v1/schedule/keyspaces/test2/tables/table2
    When I send a POST request
    Then the response is successful
    And the response matches the json demand_repair_job
    And the job for test2.table2 change status to completed