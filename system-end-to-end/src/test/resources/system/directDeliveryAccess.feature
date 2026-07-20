Feature: Direct access to the delivery-service enforces authorization
  As a security reviewer,
  I want the delivery-service to authorize on its own port,
  so that security is defense-in-depth, not merely perimeter (gateway).

  Scenario: A direct admin call without a propagated identity is unauthorized
    Given the system is running
    When I call the delivery admin fleet view directly without any identity
    Then the direct delivery call is rejected with status 401

  Scenario: A direct admin call with a Sender identity is forbidden
    Given the system is running
    And I have a valid Sender session
    When I call the delivery admin fleet view directly with the Sender identity
    Then the direct delivery call is rejected with status 403
