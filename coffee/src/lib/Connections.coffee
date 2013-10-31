{EventEmitter} = require 'events'

TopicConsumer = require './TopicConsumer'
TopicProducer = require './TopicProducer'

ZooKafka = require './ZooKafka'

module.exports = class Connections extends EventEmitter

  constructor: (zkClient) ->
    @zooKafka = new ZooKafka zkClient
    @topicConsumer = {}
    @topicProducer = {}
    @brokerProducer = {}


  newTopicConsumer: (consumerGroup, topic, options) ->
    return @topicConsumer[ "#{consumerGroup}-#{topic}" ] =
      new TopicConsumer @, consumerGroup, topic, options

  newTopicProducer: (topic, options) ->
    return @topicProducer[topic] = new TopicProducer @, topic, options


  newConnectedBrokerProducer: (brokerDetails) ->
    @brokerProducer[brokerDetails.brokerId] = producer = new Producer 'undefined',
      host: brokerDetails.host, port: brokerDetails.port

    producer.on 'error', (err) =>
      @emit 'brokerProducerError', brokerDetails, err
      @emit 'error', "broker error: #{err}", brokerDetails

    producer.on 'brokerReconnectError', (err) =>
      @emit 'brokerProducerReconnectError', brokerDetails, err

    producer.connect()
    return producer


  getOrCreateBrokerProducer: (brokerDetails) ->
    return @brokerProducer[brokerDetails.brokerId] if brokerId of @brokerProducer
    return @newConnectedBrokerProducer brokerDetails