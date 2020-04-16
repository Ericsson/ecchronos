Feature: API to get repair config of multiple tables

  Background: Schema setup
    Given I have a json schema in repair_config.json

  Scenario: List configuration
    Given I use the url http://localhost:8080/repair/scheduled/v1/config
    When I send a GET request
    Then the response is successful
    And the response matches the json repair_config
