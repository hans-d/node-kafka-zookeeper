{EventEmitter} = require 'events'
_ = require 'underscore'
{Message} = require 'prozess'

ERR_Unknown = new Error "Unknown"
ERR_OffsetOutOfRange = new Error "OffsetOutOfRange"
ERR_InvalidMessage = new Error "InvalidMessage"
ERR_WrongPartition = new Error "WrongPartition"
ERR_InvalidFetchSize = new Error "InvalidFetchSize"
ERR_Other = new Error "Unknown error code: 99"

exports.Brokers = class Brokers

  brokers = {}

  class Broker

    constructor: ->
      @queues = {}

    write: (topic, partition, messages) ->
      key = "#{topic}-#{partition}"
      @queues[key] = [] if not key of queues
      @queues[key].push message for message in messages

    read: (topic, partition, offset, maxMessageSize, onData) ->
      key = "#{topic}-#{partition}"
      return onData ERR_OffsetOutOfRange if offset => @queues[key].length

      length = 0
      batch []
      while offset < @queues[key].length and length < maxMessageSize
        item = @queues[key][offset]
        batch.push item
        offset++
        length = length + item.byteLength
      onData null, batch

    getLatestOffset: (topic, partition) ->
      key = "#{topic}-#{partition}"
      return @queues[key].length

  @connect: (host, port) ->
    broker = "#{host}:#{port}"
    brokers[broker] = new Broker() unless broker of brokers
    return brokers[broker]

  @getBrokers: () ->
    return brokers

  @reset: () ->
    brokers = {}


exports.Producer = class Producer extends EventEmitter

  constructor: (topic, options) ->
    options = options || {}
    @topic = topic
    @partition = options.partition || 0
    @host = options.host || 'localhost'
    @port = options.port || 9092
    @broker = null


    @connection = null

  connect: () ->
    @broker = Brokers.connect @host, @port
    return @emit 'connect'

  send: (message, options, cb) ->
    if arguments.length == 2
      cb = options
      options = {}

    options.partition = options.partition || this.partition
    options.topic = options.topic || this.topic

    messages = toListOfMessages toArray messages
    @broker.write options.topic, options.partition, messages


exports.Consumer = class Consumer

  constructor: (options) ->
    options = options || {};
    @topic = options.topic || 'test'
    @partition = options.partition || 0
    @host = options.host || 'localhost'
    @port = options.port || 9092
    @offset = options.offset || 0
    @maxMessageSize = options.maxMessageSize || 1024 * 1024
    @polling = options.polling  || 2

  connect: () ->
    @broker = Brokers.connect @host, @port

  consume: (cb) ->
    @broker.read @topic, @partition, @offset, @maxMessageSize, (err, messages) ->
      return cb err if err
      @offset = @offset + messages.length
      return cb null, messages

  getLatestOffset: (cb) ->
    cb null, @brokers.getLatestOffset @topic, @partition


toArray = (arg) ->
  return arg if _.isArray arg
  return [arg]

toListOfMessages = (args) ->
  return _.map args, (arg) ->
    return arg if arg instanceof Message
    return new Messagearg


