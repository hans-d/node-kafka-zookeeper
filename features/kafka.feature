Feature: Works with Zookeeper

# In order to process messages
# As a user of this module
# I want to interact with kafka

  Scenario: Consume topic via stream
    Given the background
      And a connected kafkazoo client
      And a subscribeable topic that has unconsumed messages
     When I create a consumer for that topic
     Then I should directly receive messages

