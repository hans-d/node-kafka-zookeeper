{EventEmitter} = require 'events'

class StandaloneStrategy extends EventEmitter

  constructor: (connections, consumerGroup, topic, consumerId, options) ->
    @zooKafka = connections.zooKafka
    @consumerGroup = consumerGroup
    @topic = topic
    @consumerId = consumerId

  connect: ->
    @zooKafka.getRegisteredTopicPartitions this.topic, (err, partitions) =>
      return @emit 'error', err if err
      @emit 'partitions', partitions

module.exports = StandaloneStrategy;