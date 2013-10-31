mockery = require 'mockery'
should = require 'should'
sinon = require 'sinon'
_ = require 'underscore'

TopicConsumer = require '../../src/lib/TopicConsumer'
TopicProducer = require '../../src/lib/TopicProducer'
ZooKafka = require '../../src/lib/ZooKafka'


describe 'Kafkazoo class', ->
  ZookeeperClientModuleStub = ZookeeperClientStub = kafkaConnectionsStub = null
  stubbed = {}
  Kafkazoo = kafka = null

  before ->
    class ZookeeperClientStub
      connect: sinon.stub().yields null

    ZookeeperClientModuleStub =
      PlusClient: sinon.spy ZookeeperClientStub

    stubbedClass = (stubId, extra) ->
        ->
          stub = stubbed[stubId]
          stub._initArgs = arguments
          extra stub if extra
          stub


    mockery.enable useCleanCache: true
    mockery.registerMock 'zookeeper-hd', ZookeeperClientModuleStub
    mockery.registerMock './ZooKafka', stubbedClass 'zooKafka'
    mockery.registerMock './TopicConsumer',
      stubbedClass 'topicConsumer', (stub) -> stub.connections = stub._initArgs[0]
    mockery.registerMock './TopicProducer',
      stubbedClass 'topicProducer', (stub) -> stub.connections = stub._initArgs[0]

    mockery.registerAllowables [
        '../../src/lib/Kafkazoo', './Connections',
        'events', 'util',
        'underscore',
    ]

    Kafkazoo = require '../../src/lib/Kafkazoo'

  after ->
    mockery.deregisterAll()
    mockery.disable()

  beforeEach ->
    stubbed = {};
    stubbed['zooKafka'] = sinon.stub new ZooKafka();
    class kafkaConnectionsStub
    kafkaConnectionsStub.zookafka = stubbed['zooKafka'];

    stubbed['topicConsumer'] = sinon.stub new TopicConsumer kafkaConnectionsStub
    stubbed['topicProducer'] = sinon.stub new TopicProducer kafkaConnectionsStub


    kafka = new Kafkazoo()


  describe 'construction', ->

    it 'should by default use localhost:2181 for the zookeeper connection', ->
      target = ZookeeperClientModuleStub.PlusClient
      target.reset()

      kafka = new Kafkazoo()

      target.callCount.should.equal 1
      target.calledWithNew().should.be.true
      target.getCall(0).args[0].should.include connect: 'localhost:2181'

    it 'should use given zookeeper connection', ->
      target = ZookeeperClientModuleStub.PlusClient;
      target.reset();

      kafka = new Kafkazoo zookeeper: connect: "foobar:1234"

      target.callCount.should.equal 1
      target.calledWithNew().should.be.true
      target.getCall(0).args[0].should.include connect: 'foobar:1234'

    it 'should by default use / as zookeeper root', ->
      target = ZookeeperClientModuleStub.PlusClient
      target.reset()

      kafka = new Kafkazoo()

      target.callCount.should.equal 1
      target.calledWithNew().should.be.true
      target.getCall(0).args[0].should.include root: '/'

    it 'should use given zookeeper root', ->
      target = ZookeeperClientModuleStub.PlusClient;
      target.reset();

      kafka = new Kafkazoo zookeeper: root: '/some/foor/bar'

      target.callCount.should.equal 1
      target.calledWithNew().should.be.true
      target.getCall(0).args[0].should.include root: '/some/foor/bar'

    it 'should have a connections object', ->
      kafka = new Kafkazoo();
      kafka.connections.should.exist

    describe 'connections object', ->

      beforeEach ->
        kafka = new Kafkazoo()

      it 'should have a zooKafka object', ->
        kafka.connections.zooKafka.should.exist
        kafka.connections.zooKafka.should.equal stubbed['zooKafka']

      it 'should have an empty topicConsumer', ->
        kafka.connections.topicConsumer.should.exist
        kafka.connections.topicConsumer.should.eql {}

  describe '#connect', ->
    it 'should connect zookeeper', ->
      target = ZookeeperClientStub.prototype.connect;
      target.reset()

      kafka.connect()

      target.calledOnce.should.be.true

    it 'should emit connected', ->
        triggered = false;
        kafka.on 'connected', -> triggered = true
        kafka.connect()

        triggered.should.be.true

  describe 'createConsumer', ->

    it 'should create a topic consumer with given group and topic', ->
      kafka.createConsumer 'topic', 'group'

      stubbed['topicConsumer']._initArgs.should.include
        1: 'group'
        2: 'topic'

    it 'should add created topic consumer to kafka connections', ->
      kafka.createConsumer 'topic', 'group'

      kafka.connections.topicConsumer.should.have.keys 'group-topic'


  describe 'createProducer', ->

    it 'should create a producer wfor a given topic', ->
      kafka.createProducer 'topic'


