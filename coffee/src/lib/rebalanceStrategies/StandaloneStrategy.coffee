{EventEmitter} = require 'events'

###
  Consumer strategy to connect to all available partitions as registered in zookeeper
  assuming there are no other consumers (thus no distributed read).

  A rebalance strategy determines what topic partitions should be consumed for a given
  topic by a consumer in consumer group. The {@link TopicConsumer} is responsible for
  the actual rebalancing (connect and disconnect) of the provided topic partitions.

  @event partitions The partitions that should be consumed
  @event {String[]} partitions.partitions
  @event error On errors
  @event error.error Error details
###

module.exports = class StandaloneStrategy extends EventEmitter

  ###
    Constructs the strategy.
  ###
  constructor: (connections, consumerGroup, topic, consumerId, options) ->
    @zooKafka = connections.zooKafka
    @consumerGroup = consumerGroup
    @topic = topic
    @consumerId = consumerId

  ###
    Initiate providing partitions via the 'partitions' event.

    Currently only gets the registered topic partition once.

    TODO: watch zookeeper for changes, and emit new set of partitions

    @event partitions If new partitions
    @event error If error
  ###
  connect: ->
    @zooKafka.getRegisteredTopicPartitions this.topic, (err, partitions) =>
      return @emit 'error', err if err
      @emit 'partitions', partitions
