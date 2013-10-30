{Readable} = require 'stream'

bignum = require 'bignum'
{Consumer} = require 'prozess'
_ = require 'underscore'

###
  Utility function to create properties for a class
###
Function::property = (prop, desc) ->
  Object.defineProperty @prototype, prop, desc


###
  Consumer for a specific topic partition. Wraps the underlying Prozess client.
###
module.exports = class PartitionConsumer extends Readable


  ###
    Current offset, as reported by Prozess client
  ###
  @property 'offset',
    get: ->
      return if @consumer then bignum(@consumer.offset).toString() else null


  ###
    Constructs the partition consumer
  ###
  constructor: (topicConsumer, topicPartition, options) ->
    super objectMode: true

    options = _.defaults options || {},
      consumeOnConnected: true

    @topicPartition = topicPartition
    @partitionDetails = {}

    @zooKafka = topicConsumer.zooKafka
    @consumerGroup = topicConsumer.consumerGroup

    @consumer = null
    @consumerConfig =
        maxMessageSize: options.maxMessageSize
        polling: options.polling

    @latestOffset = null

    @_defineHandlers options

    @on 'connected', @consumeNext if options.consumeOnConnected


  ###
    Sets up default event handlers (part of construction).
  ###
  _defineHandlers: (options) ->

    @onNoMessages = options.onNoMessages || () ->
      setTimeout =>
        @consumeNext()
      , options.noMessagesTimeout || 2000

    @onOffsetOutOfRange = options.onOffsetOutOfRange || () ->
      @consumer.getLatestOffset (error, offset) =>
        return @fatal 'retrieving latest offset', error if error
        @._registerConsumerOffset offset, =>
          @consumeNext()

    @onConsumptionError = options.onConsumptionError || (error) ->
      @fatal 'on consumption', error


  ###
    Connects to Kafka
  ###
  connect: ->
    @_getPartitionConnectionAndOffsetDetails (error, details) =>
      return @fatal 'retrieving connection details', error if error

      @partitionDetails = details

      config =
        host: details.broker.host
        port: details.broker.port
        topic: details.topic
        partition: details.partitionId
        offset: details.offset
        maxMessageSize: @consumerConfig.maxMessageSize
        polling: @consumerConfig.polling
      @consumer = new Consumer config
      @emit 'connecting', config

      @consumer.connect (error) =>
        return @fatal 'connecting', error if error
        @emit 'connected', config


  ###
    Disconnects from Kafka
  ###
  disconnect: ->
    @consumer = null


  ###
    Shut down consumer
  ###
  shutdown: ->
    @disconnect()
    @push null


  ###
    Utility function
  ###
  _getPartitionConnectionAndOffsetDetails: (onData) ->
    @zooKafka.getPartitionConnectionAndOffsetDetails @consumerGroup, @topicPartition, onData


  ###
    Utility function
  ###
  _registerConsumerOffset: (offset, onReady) ->
    @zooKafka.registerConsumerOffset @consumerGroup, @topicPartition, offset, onReady
    @emit 'offsetUpdate', offset, @.partitionDetails.offset


  ###
    Dummy implementation of stream.Readable#_read
  ###
  _read: ->


  ###
    Consume from Kafka.

    Kafka 0.7 is currently polling only.
  ###
  consumeNext: ->
    @emit 'consuming'
    @latestOffset = @offset

    @consumer.consume (error, messages) =>

      meta = _.extend {}, @topicPartition,
          offset:
            previous: @latestOffset
            current: @offset

      @emit 'consumed', error, messages, meta

      if error
        return @onConsumptionError.apply @, [error] if error.message != 'OffsetOutOfRange'

        @emit 'offsetOutOfRange', @partitionDetails.offset
        return @onOffsetOutOfRange.apply @

      return @onNoMessages.apply @ if messages.length == 0

      @push
        messages: messages
        meta: meta

  ###
    Utility method
  ###
  fatal: (msg, detail) ->
    @emit 'error', msg, detail