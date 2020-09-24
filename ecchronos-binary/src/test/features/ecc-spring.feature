Feature: ecc-spring

  Scenario: RestServer health check returns UP
    Given I use the url http://localhost:8080/actuator/health
    When I send a GET request
    Then the response is successful
    And the status is UP
