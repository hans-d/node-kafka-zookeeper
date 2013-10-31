{EventEmitter} = require 'events'

{Message} = require 'prozess'
_ = require 'underscore'

exports.Message = Message

ERR_Unknown = new Error "Unknown"
ERR_OffsetOutOfRange = new Error "OffsetOutOfRange"
ERR_InvalidMessage = new Error "InvalidMessage"
ERR_WrongPartition = new Error "WrongPartition"
ERR_InvalidFetchSize = new Error "InvalidFetchSize"
ERR_Other = new Error "Unknown error code: 99"

###
  Fakes a set of kafka brokers

  Queues are memory based and implemented using Arrays.
  Writes pushes the provided messages into the queue.
  Offsets are related to the queue offset in the array (any calculation using the
  offset and message length will fail)

###
exports.Brokers = class Brokers

  brokers = {}


  ###
    Fake broker, holds the queues for a broker.

    Topics are keys of the queue
  ###
  class Broker

    ###
      Constructs a broker
    ###
    constructor: ->
      @queues = {}


    ###
      Adds messages for topic-partition to the queue
    ###
    write: (topic, partition, messages) ->
      key = "#{topic}-#{partition}"
      @queues[key] = [] if not key of queues
      @queues[key].push message for message in messages


    ###
      Retrieves messages from the queue.

      Provides error on offset out of range
      Uses maxMessageSize to batch messages
    ###
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

    ###
      Gets the latests offset
    ###
    getLatestOffset: (topic, partition) ->
      key = "#{topic}-#{partition}"
      return @queues[key].length

  ###
    Connects to a fake broker.

    Will create the broker if not exists, otherwise will use the existing broker
  ###
  @connect: (host, port) ->
    broker = "#{host}:#{port}"
    brokers[broker] = new Broker() unless broker of brokers
    return brokers[broker]


  ###
    Returns all connected brokers
  ###
  @getBrokers: () ->
    return brokers

  ###
    Removes all brokers and their queues.

    For testing purposes
  ###
  @reset: () ->
    brokers = {}


###
  Fake producer, interacts with the fake broker(s)

  API compatible with Prozess module
###
exports.Producer = class Producer extends EventEmitter

  ###
    Construct producer
  ###
  constructor: (topic, options) ->
    options = options || {}
    @topic = topic
    @partition = options.partition || 0
    @host = options.host || 'localhost'
    @port = options.port || 9092
    @broker = null

    @connection = null


  ###
    Connects to a broker
  ###
  connect: () ->
    @broker = Brokers.connect @host, @port
    return @emit 'connect'


  ###
    Send (produce) a message to the broker
  ###
  send: (message, options, cb) ->
    if arguments.length == 2
      cb = options
      options = {}

    options.partition = options.partition || this.partition
    options.topic = options.topic || this.topic

    messages = toListOfMessages toArray messages
    @broker.write options.topic, options.partition, messages



###
  Fake consumer, interacts with the fake broker(s)

  API compatible with zookeeper module
###
exports.Consumer = class Consumer

  ###
    Constructs the consumer
  ###
  constructor: (options) ->
    options = options || {};
    @topic = options.topic || 'test'
    @partition = options.partition || 0
    @host = options.host || 'localhost'
    @port = options.port || 9092
    @offset = options.offset || 0
    @maxMessageSize = options.maxMessageSize || 1024 * 1024
    @polling = options.polling  || 2


  ###
    Connects to the fake broker
  ###
  connect: () ->
    @broker = Brokers.connect @host, @port

  ###
    Consume (read) messages from the broker
  ###
  consume: (cb) ->
    @broker.read @topic, @partition, @offset, @maxMessageSize, (err, messages) ->
      return cb err if err
      @offset = @offset + messages.length
      return cb null, messages


  ###
    Returns latests offset
  ###
  getLatestOffset: (cb) ->
    cb null, @brokers.getLatestOffset @topic, @partition


###
  Utility function to turn messages into an array
###
toArray = (arg) ->
  return arg if _.isArray arg
  return [arg]

###
  Utility function to create real Kafka Message objects
###
toListOfMessages = (args) ->
  return _.map args, (arg) ->
    return arg if arg instanceof Message
    return new Messagearg


