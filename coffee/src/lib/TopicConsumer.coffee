{Readable} = require 'stream'
util = require 'util'

async = require 'async'
uuid = require 'node-uuid'
_ = require 'underscore'

Compression = require './Compression'
PartitionConsumer = require './PartitionConsumer'
StandaloneStrategy = require './rebalanceStrategies/StandaloneStrategy'


###
  Coordinates the consumption of the various partitions and brokers
  where topic messages are stored.

  Creation is done by {@link Kafakazoo), which provides a reference to the
  created topic consumer.

  Information about this is retrieved from zookeeper, which partitions and
  brokers to consume is further determined by the rebalance strategy.
###
module.exports = class TopicConsumer extends Readable

  ###
    Constructs a topic consumer.

    @param {Object} connections Kafkazoo connections
    @param {String} consumerGroup Kafka consumer group
    @param {String} consumerGroup Kafka topic
    @param {Object} options (optional)
    @param {Object} options.rebalanceStrategy (optional) Rebalance strategy class
      (defaults to StandaloneStrategy)
  ###
  constructor: (connections, consumerGroup, topic, options) ->
    super objectMode: true

    options = options || {}

    @connections = connections
    @topic = topic
    @consumerGroup = consumerGroup
    @consumerId = options.consumerId || uuid.v1()

    rebalanceStrategy = options.rebalanceStrategy || StandaloneStrategy
    @rebalancer = new rebalanceStrategy @connections, @consumerGroup, @topic, @consumerId
    @rebalancer.on 'partitions', @rebalance

    @partitionConsumers = {}

    @partitionConsumerConfig = {}
    @preprocess = new Compression.Decompressor()

    @preprocess.on 'error', (msg, detail) =>
      @emit 'error', msg, detail

    @preprocess.on 'readable', =>
      data = @preprocess.read()
      @push(data)


  ###
    Connects to zookeeper and kafka.

    Delegates to the rebalance strategy to deliver the partitions to read from.
    This is done via the ```partitions``` event, that fires #rebalance
  ###
  connect: ->
    @rebalancer.connect()


  ###
    Dummy implementation of stream.Readable#_read

    No automatic reading
  ###
  _read : ->


  ###
    Connecting and disconnection the low level partition consumers, as provided by the
    rebalance strategy,

    Currently only does initial connection

    TODO: implement rebalancing
  ###
  rebalance: (partitions) ->

    return @emit 'error', 'rebalance not implemented yet' if _.keys(@partitionConsumers).length != 0

    async.each partitions, (partition, asyncReady) =>
        @connectPartitionConsumer partition
        asyncReady()

  ###
    Construction of a partition consumer.

    Wires the various events
  ###
  connectPartitionConsumer: (partition) ->
    id = partition.brokerPartitionId

    @emit 'connecting', partition

    partitionConsumer = new PartitionConsumer @, partition, @partitionConsumerConfig
    partitionConsumer.pipe @preprocess

    partitionConsumer.on 'readable', =>
      @emit 'partitionReadable', id

    for event in ['connected', 'offsetUpdate', 'consuming', 'consumed', 'offsetOutOfRange']
      # without do() event will keep the latest value of the loop
      do (event) =>
        partitionConsumer.on event, (arg1, arg2) =>
          @emit event, id, arg1, arg2


    partitionConsumer.on 'error', (msg, detail) =>
      @emit 'partitionError', id, msg, detail


    partitionConsumer.connect()

    @partitionConsumers[id] = partitionConsumer
