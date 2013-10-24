{Readable} = require 'stream'
util = require 'util'

async = require 'async'
uuid = require 'node-uuid'
_ = require 'underscore'

Compression = require './Compression'
PartitionConsumer = require './PartitionConsumer'
StandaloneStrategy = require './rebalanceStrategy/Standalone'

module.exports = class TopicConsumer extends Readable

  constructor: (connections, consumerGroup, topic, options) ->
    super objectMode: true

    options = options || {};

    @connections = connections;
    @topic = topic;
    @consumerGroup = consumerGroup;
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


  connect: ->
    @rebalancer.connect()


  _read : ->


  rebalance: (partitions) ->

    return @emit 'error', 'rebalance not implemented yet' if _.keys(@partitionConsumers).length != 0

    async.each partitions, (partition, asyncReady) =>
        @connectPartitionConsumer partition
        asyncReady()


  connectPartitionConsumer: (partition) ->
    id = partition.brokerPartitionId

    @emit 'connecting', partition

    partitionConsumer = new PartitionConsumer @, partition, @partitionConsumerConfig
    partitionConsumer.pipe @preprocess

    partitionConsumer.on 'readable', =>
      @emit 'partitionReadable', id

    for event in ['connected', 'offsetUpdate', 'consuming', 'consumed', 'offsetOutOfRange']
      # without do() event will stay the latest value of the loop
      do (event) =>
        partitionConsumer.on event, (arg1, arg2) =>
          @emit event, id, arg1, arg2


    partitionConsumer.on 'error', (msg, detail) =>
      @emit 'partitionError', id, msg, detail


    partitionConsumer.connect()

    @partitionConsumers[id] = partitionConsumer
#    @emit 'connected', partition
