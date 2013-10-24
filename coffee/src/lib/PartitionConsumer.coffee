{Readable} = require 'stream'

bignum = require 'bignum'
{Consumer} = require 'prozess'
_ = require 'underscore'

Function::property = (prop, desc) ->
  Object.defineProperty @prototype, prop, desc

module.exports = class PartitionConsumer extends Readable

  @property 'offset',
    get: ->
      return if @consumer then bignum(@consumer.offset).toString() else null

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


  _defineHandlers: (options) ->

    @onNoMessages = options.onNoMessages || (this_) ->
      setTimeout ->
        this_.consumeNext()
      , options.noMessagesTimeout || 2000

    @onOffsetOutOfRange = options.onOffsetOutOfRange || (this_) ->
      this_.consumer.getLatestOffset (error, offset) ->
        return this_.fatal 'retrieving latest offset', error if error
        this_._registerConsumerOffset offset, ->
          this_.consumeNext()

    @onConsumptionError = options.onConsumptionError || (this_, error) ->
      this_.fatal 'on consumption', error

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


  disconnect: ->
    @consumer = null


  shutdown: ->
    @disconnect()
    @push null


  _getPartitionConnectionAndOffsetDetails: (onData) ->
    @zooKafka.getPartitionConnectionAndOffsetDetails @consumerGroup, @topicPartition, onData


  _registerConsumerOffset: (offset, onReady) ->
    @zooKafka.registerConsumerOffset @consumerGroup, @topicPartition, offset, onReady
    @emit 'offsetUpdate', offset, @.partitionDetails.offset


  _read: ->

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
        return @onConsumptionError @, error if error.message != 'OffsetOutOfRange'

        @emit 'offsetOutOfRange', @partitionDetails.offset
        return @onOffsetOutOfRange @

      return @onNoMessages @ if messages.length == 0

      @push
        messages: messages
        meta: meta


  fatal: (msg, detail) ->
    @emit 'error', msg, detail