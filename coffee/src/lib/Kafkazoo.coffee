{EventEmitter} = require 'events'

_ = require 'underscore'
zookeeper = require 'zookeeper-hd'

TopicConsumer = require './TopicConsumer'
ZooKafka = require './ZooKafka'

module.exports = class Kafkazoo extends EventEmitter

  constructor: (options) ->
    options = _.defaults options || {},
      zookeeper: {}

    options.zookeeper = _.defaults options.zookeeper,
      connect: 'localhost:2181'
      root: '/'
      clientConfig: {}

    options.zookeeper.clientConfig = _.defaults options.zookeeper.clientConfig,
      connect: options.zookeeper.connect
      root: options.zookeeper.root

    @_zookeeper = new zookeeper.PlusClient options.zookeeper.clientConfig
    @config = _.omit options, zookeeper

    @connections = () -> # make it an object, so its passed by reference
    @connections.zooKafka = new ZooKafka this._zookeeper
    @connections.topicConsumer = {}

  connect: ->
    @_zookeeper.connect (err) =>
      @fatal 'Error connecting zookeeper', err if err
      @emit 'connected'

  fatal: (message, detail) ->
    @emit 'error', message, detail

  createConsumer: (topic, consumerGroup, options) ->
    return @connections.topicConsumer[ "#{consumerGroup}-#{topic}" ] =
        new TopicConsumer @connections, consumerGroup, topic, options

  getAllRegisteredBrokers: (onData) ->
    @connections.zooKafka.getAllRegisteredBrokers onData
