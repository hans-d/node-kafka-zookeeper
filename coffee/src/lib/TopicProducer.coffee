{EventEmitter} = require 'events'
{Writable} = require 'stream'

async = require 'async'
{Producer} = require 'prozess'
_ = require 'underscore'

brokerSelectionStrategy = require './brokerSelectionStrategy'
Message = require './Message'

###
  Central producer for a topic, coordinates the actual producers for producing
  to brokers and partitions
###
module.exports = class TopicProducer extends Writable

  ###
    Constructs the topic producer.
  ###
  constructor: (connections, topic, options) ->
    options = options || {}
    options.objectMode = true
    options.decodeStrings = false

    super options

    @connections = connections
    @topic = topic

    @brokerSelectionStrategy = options.brokerSelectionStrategy || brokerSelectionStrategy.pickOne
    @on 'brokers', @connectBrokers

    @brokers = {}


  connect: ->
    @brokerSelectionStrategy.apply @


  connectBrokers: (brokers, defaultId) ->
    return @emit 'error', 'reconnect not implemented yet' if _.keys(@brokers).length != 0

    async.each _.keys(brokers), (brokerId, asyncReady) =>
      @brokers[brokerId] = @connections.getOrCreateBrokerProducer brokers[brokerId]
      asyncReady()
    , (err) =>
      return @emit 'error', err if err
      @defaultBrokerId = defaultId
      @emit 'connected'


  _write: (data, encoding, done) ->
    messages = Message.asListOfMessages data
    perBroker = _.groupBy messages, (message) -> return message.brokerId

    async.each _.keys(perBroker), (brokerId, asyncBrokerReady) =>
      messagesPerBroker = perBroker[brokerId]
      perPartition = _.groupBy messagesPerBroker, (message) -> return message.partitionId

      async.each _.keys(perPartition), (partitionId, asyncPartitionReady) =>
        messagesPerPartition = perPartition[partitionId]
        @brokers[@defaultBrokerId].send messagesPerPartition, topic: @topic, partition: 0, asyncPartitionReady
      , (err) -> asyncBrokerReady err

    , (err) -> done err
