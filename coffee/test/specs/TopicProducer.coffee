mockery = require 'mockery'
should = require 'should'
sinon = require 'sinon'

FakeProzess = require '../../src/lib/FakeProzess'

describe 'TopicProducer class', ->
  BrokerSelectionStrategy = TopicProducer = null
  topicProducer = null

  before ->
    BrokerSelectionStrategy =
      pickOne: -> return 1

    mockery.enable useCleanCache: true
    mockery.registerMock 'prozess', FakeProzess
    mockery.registerMock './brokerSelectionStrategy', BrokerSelectionStrategy

    mockery.registerAllowables [
      '../../src/lib/TopicProducer',
      './brokerSelectionStrategy', './Message'
      'events', 'stream',
      'async', 'underscore',
    ]

    TopicProducer = require '../../src/lib/TopicProducer'

  beforeEach ->
    topicProducer = new TopicProducer()

  describe 'construction', ->

    it 'can be constructed', ->
      topicProducer = new TopicProducer()

    it 'has a default broker selection strategy', ->
      topicProducer = new TopicProducer()
      topicProducer.brokerSelectionStrategy.should.be.equal BrokerSelectionStrategy.pickOne


  describe '#connect', ->

    it 'calls broker selection strateggy', ->
      sinon.spy topicProducer, 'brokerSelectionStrategy'

      topicProducer.connect()

      topicProducer.brokerSelectionStrategy.calledOnce.should.be.true


  describe '#connectBrokers', ->


  describe '#_write', ->

    beforeEach ->
      topicProducer.defaultBrokerId = 'test'
      topicProducer.brokers['test'] = send: sinon.stub().yields null

    it 'should send messages', ->
      topicProducer._write 'test', null, () ->
      topicProducer.brokers.test.send.calledOnce.should.be.true

    it 'should provide a correct message', ->
      topicProducer._write 'test', null, () ->

      messages = topicProducer.brokers.test.send.getCall(0).args[0]
      firstMessage = messages[0]

      firstMessage.should.be.instanceof FakeProzess.Message
      firstMessage.payload.should.equal 'test'
