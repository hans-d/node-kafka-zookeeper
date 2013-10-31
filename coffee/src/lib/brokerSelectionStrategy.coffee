_ = require 'underscore'

###
  A broker selection strategy determines which brokers are available for producing to,
###

###
  Selects one registered (active) broker where the topic is already available.
  If none is present, a random broker is picked.
###
exports.pickOne = () ->

  select: (brokers) ->
    brokerIds = _.keys brokers
    selectedId = brokerIds[Math.floor(Math.random() * brokerIds.length)]

    @emit 'brokers', _.pick(brokers, selectedId), selectedId


  zooKafka = connections.zooKafka
  broker = null

  zooKafka.getRegisteredTopicBrokers @topic, (err, brokers) =>
    select brokers if brokers.length > 0

    zooKafka.getAllRegisteredBrokers (err, brokers) =>
      return @emit 'error', 'no brokers available' if brokers.length == 0
      select brokers
