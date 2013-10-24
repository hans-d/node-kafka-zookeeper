Feature: Connects with Zookeeper

  # In order to work with kafka in a distributed way
  # As a user of this module
  # I want to injteract with zookeeper

  Scenario: Get registered brokers from zookeeper
    Given the background
     When retrieving all registered broker ids
     Then the resulting data should be [1, 5]


