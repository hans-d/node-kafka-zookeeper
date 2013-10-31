{EventEmitter} = require 'events'

_ = require 'underscore'
zookeeper = require 'zookeeper-hd'

Connections = require './Connections'

###
  Higher level client for kafka, that interacts with zookeeper.
###
module.exports = class Kafkazoo extends EventEmitter


  ###
    Constructs client.

    @event connected If successfull connected (see #connect)
    @event error If error
    @event error.message User friendly (but generic) message
    @event error.detail Developer friendly (original) details

    @param {Object} [options="{zookeeper: {connect: 'localhost:2181', root:'/'}}"]
    @param {Object} options.zookeeper
    @param {String} options.zookeeper.connect Connect string for zookeeper hosts. Comma-delimited
    @param {String} options.zookeeper.root Zookeeper root path
    @param {Object} options.zookeeper.clientConfig (optional) Full PlusClient options
  ###
  constructor: (options) ->
    options = _.defaults options || {},
      zookeeper: {}

    # allow for partial zookeeper configuration
    options.zookeeper = _.defaults options.zookeeper,
      connect: 'localhost:2181'
      root: '/'
      clientConfig: {}

    # if no clientConfig is provided...
    options.zookeeper.clientConfig = _.defaults options.zookeeper.clientConfig,
      connect: options.zookeeper.connect
      root: options.zookeeper.root

    @_zookeeper = new zookeeper.PlusClient options.zookeeper.clientConfig
    @config = _.omit options, zookeeper


    @connections = new Connections @_zookeeper


  ###
    Connect to zookeeper.

    Kafka connections are only made with #createProducer and #createConsumer.

    @event If connected
    @event If error
  ###
  connect: ->
    @_zookeeper.connect (err) =>
      @fatal 'Error connecting zookeeper', err if err
      @emit 'connected'


  ###
    Utility method to signal failing of client

    @event error
  ###
  fatal: (message, detail) ->
    @emit 'error', message, detail


  ###
    Creates a consumer for given topic

    @return {Object} {@link TopicConsumer}
  ###
  createConsumer: (topic, consumerGroup, options) ->
    return @connections.newTopicConsumer consumerGroup, topic, options

  ###
    Creates a producer for given topic

    @return {Object{ {@link TopicProducer}
  ###
  createProducer: (topic, options) ->
    return false


  ###
    Returns all registered (active) brokers, see ZooKafka#getAllRegisteredBrokers

    @param {Function} onData Callback,
  ###
  getAllRegisteredBrokers: (onData) ->
    @connections.zooKafka.getAllRegisteredBrokers onData
