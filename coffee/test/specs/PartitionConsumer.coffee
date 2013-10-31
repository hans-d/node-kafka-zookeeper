stream = require 'stream'
util = require 'util'

concat = require 'concat-stream'
mockery = require 'mockery'
should = require 'should'
sinon = require 'sinon'
_ = require 'underscore'
FakeProzess = require '../../src/lib/FakeProzess'


describe 'PartitionConsumer class', ->
  ProzessStub = ProzessConsumerStub = ZookeeperClientStub = TopicConsumerStub = null
  PartitionConsumer = partitionConsumer = null
  fakeClock = null

  given = {}
  DEFAULT_NOMSG_TIMEOUT = 2000

  createTestPartitionConsumer = (options) ->
    options = _.defaults options || {},
      consumeOnConnected: false
    p = new PartitionConsumer given['topic consumer instance'], given['partition details'], options
    p.on 'error', (msg, detail) ->
      throw Error "should not be happening: #{msg} / #{util.inspect(detail)}"
    return p

  before ->
    class ProzessConsumerStub
      constructor: ->
        @initArgs = arguments
        @offset = given['current offset']

      connect: (cb) ->
        cb null

      consume: (cb) ->
        @offset = given['new offset'];
        cb null, ['foo?']

      getLatestOffset: (cb) -> cb null

    ProzessStub =
      Consumer: sinon.spy ProzessConsumerStub

    class ZookeeperClientStub
      getPartitionConnectionAndOffsetDetails: (group, topicPartition, cb) ->
        cb null, given['partition connection details']

      registerConsumerOffset: (consumerGroup, topicPartition, offset, cb) ->
        cb null

    class TopicConsumerStub
      constructor: (consumerGroup) ->
        @zooKafka = new ZookeeperClientStub()
        @consumerGroup = consumerGroup

    mockery.enable useCleanCache: true
    mockery.registerMock 'prozess', ProzessStub
    mockery.registerAllowables [
      '../../src/lib/PartitionConsumer',
      'stream', 'util', 'bignum', 'underscore',
      './build/Debug/bignum', './build/Release/bignum'
    ]

    PartitionConsumer = require '../../src/lib/PartitionConsumer'

  after ->
    mockery.deregisterAll()
    mockery.disable();

  beforeEach ->
    given['consumer group'] = 'foobar'
    given['current offset'] = '123'
    given['new offset'] = '150'
    given['registered offset'] = '100'

    given['partition details'] =
      brokerPartitionId: '1-0'
      brokerId: '1'
      partitionId: 0
      topic: 'topic1'

    given['partition connection details'] = _.extend
      consumerGroup: given['consumer group']
      offset: '100'
      broker:
        host: '127.0.0.2'
        port: '1234'
    , given['partition details']

    given['topic consumer instance'] = new TopicConsumerStub given['consumer group']

    fakeClock = sinon.useFakeTimers()
    partitionConsumer = createTestPartitionConsumer()


  afterEach ->
    fakeClock.restore()

    for target in [ProzessConsumerStub]
      for method in _.methods target.prototype
        target.prototype[method].restore() if 'restore' in _.methods target.prototype[method]

  describe 'construction', ->

    describe 'creates a consumer', ->

      describe 're-use data from topic consumer', ->

        beforeEach ->
          partitionConsumer = new PartitionConsumer given['topic consumer instance'], given['partition details']

        it 'should reuse the zookeeper client', ->
          partitionConsumer.zooKafka.should.equal given['topic consumer instance'].zooKafka

        it 'should reuse the consumergroup', ->
          partitionConsumer.consumerGroup.should.equal(given['topic consumer instance'].consumerGroup);


      it 'should have a default onNoMessages handler', ->
        partitionConsumer = new PartitionConsumer given['topic consumer instance'], given['partition details']

        partitionConsumer.onNoMessages.should.be.instanceof Function

      it 'should have a given onNoMessages handler', ->
        userdefined = -> 'foo no messages'
        partitionConsumer = new PartitionConsumer(
            given['topic consumer instance'], given['partition details'], onNoMessages: userdefined
        )

        partitionConsumer.onNoMessages.should.equal userdefined

      it 'should have a default onOffsetOutOfRange handler', ->
        partitionConsumer = new PartitionConsumer given['topic consumer instance'], given['partition details']

        partitionConsumer.onOffsetOutOfRange.should.be.instanceof Function

      it 'should have a default onOffsetOutOfRange handler', ->
        userdefined = -> return 'foo offset out of range'
        partitionConsumer = new PartitionConsumer(
            given['topic consumer instance'], given['partition details'], onOffsetOutOfRange: userdefined
        )

        partitionConsumer.onOffsetOutOfRange.should.equal userdefined

      it 'should not have a kafka consumer yet', ->
        partitionConsumer = new PartitionConsumer given['topic consumer instance'], given['partition details']

        should.not.exist partitionConsumer.consumer

      it 'should consume when connected', ->
        partitionConsumer = new PartitionConsumer given['topic consumer instance'], given['partition details']
        partitionConsumer.listeners('connected').length.should.equal 1
        partitionConsumer.listeners('connected')[0].should.equal partitionConsumer.consumeNext

      it 'should not consume when connected if specified', ->
        partitionConsumer = new PartitionConsumer(
            given['topic consumer instance'], given['partition details'], consumeOnConnected: false
        )
        partitionConsumer.listeners('connected').length.should.equal 0

      it 'should have an offset property that is empty when not connected', ->
        should.not.exist partitionConsumer.offset

      it 'should have an offset property that offers the real offset of the kafka client', ->
        partitionConsumer.connect()
        partitionConsumer.offset.should.equal partitionConsumer.consumer.offset


    describe 'default handlers', ->

      describe 'onNoMessages', ->

        it 'continues consuming after the timeout', (done) ->
          triggered = false
          sinon.stub partitionConsumer, 'consumeNext', ->
              triggered = true
              done()

          partitionConsumer.onNoMessages partitionConsumer

          fakeClock.tick DEFAULT_NOMSG_TIMEOUT - 200
          triggered.should.be.false

          fakeClock.tick DEFAULT_NOMSG_TIMEOUT
          triggered.should.be.true

        it 'has a configurable timeout', (done) ->
          triggered = false
          partitionConsumer = createTestPartitionConsumer noMessagesTimeout: 100
          sinon.stub partitionConsumer, 'consumeNext', ->
              triggered = true
              done()

          partitionConsumer.onNoMessages partitionConsumer
          fakeClock.tick 100

          triggered.should.be.true

      describe 'onOffsetOutOfRange', ->

        beforeEach ->
          offsetUpdated = false;
          partitionConsumer.connect()
          partitionConsumer.consumer.getLatestOffset = sinon.stub()
            .yields null, given['new offset']

        it 'should continue consuming after correction', (done) ->
          triggered = false;
          sinon.stub partitionConsumer, 'consumeNext', ->
            triggered = true
            done()

          partitionConsumer.onOffsetOutOfRange partitionConsumer

          triggered.should.be.true

        it 'should reset offset to latest offset', ->
          partitionConsumer.consumeNext = sinon.stub()
          sinon.spy partitionConsumer, '_registerConsumerOffset'

          partitionConsumer.onOffsetOutOfRange partitionConsumer

          partitionConsumer.consumer.getLatestOffset.calledOnce.should.be.true
          partitionConsumer._registerConsumerOffset.calledOnce.should.be.true
          partitionConsumer._registerConsumerOffset.calledWith(given['new offset']).should.be.true

        it 'should fail when retrieving offset fails', (done) ->
          partitionConsumer.consumer.getLatestOffset = sinon.stub().yields 'foo'

          sinon.stub partitionConsumer, 'fatal', (msg, detail) ->
              msg.should.equal 'retrieving latest offset'
              done()

          partitionConsumer.onOffsetOutOfRange partitionConsumer

      describe 'onConsumptionError', ->

        it 'should be fatal', (done) ->
          sinon.stub partitionConsumer, 'fatal', (msg, detail) ->
            msg.should.equal 'on consumption'
            detail.should.equal 'foo error'
            done()

          partitionConsumer.onConsumptionError 'foo error'

  describe 'readable stream', ->

    it 'should implement #_read', ->
      partitionConsumer.connect()
      partitionConsumer._read()

    it 'should be possible to pipe to another stream', (done) ->
      target = concat (data) ->
        data.should.eql [ {msg:1}, {msg:2} ]
        done()

      partitionConsumer.pipe target

      partitionConsumer.push [ {msg:1}, {msg:2} ]
      partitionConsumer.push null

    it 'should be readable', (done) ->
      partitionConsumer.on 'readable', ->
        data = partitionConsumer.read()
        data.should.eql [ {msg:1}, {msg:2} ]
        done()

      partitionConsumer.push [ {msg:1}, {msg:2} ]

  describe '#connect', ->
    expectedConfig = null

    before ->
      expectedConfig =
        host: given['partition connection details'].broker.host
        port: given['partition connection details'].broker.port
        topic: given['partition details'].topic
        partition: given['partition details'].partitionId
        offset: given['partition connection details'].offset
        maxMessageSize: undefined
        polling: undefined

    it 'should retrieve the connection details from zookeeper', ->
      sinon.spy given['topic consumer instance'].zooKafka, 'getPartitionConnectionAndOffsetDetails'
      method = given['topic consumer instance'].zooKafka.getPartitionConnectionAndOffsetDetails

      partitionConsumer.connect()

      method.calledOnce.should.be.true
      method.calledWith(given['consumer group'], given['partition details']).should.be.true

    it 'should connect to kafka using info from zookeeper', ->
      ProzessStub.Consumer.reset()

      should.not.exist partitionConsumer.consumer

      partitionConsumer.connect()

      partitionConsumer.consumer.should.be.instanceof ProzessConsumerStub
      ProzessStub.Consumer.calledWithNew()

      initArgs = partitionConsumer.consumer.initArgs
      initArgs.should.have.keys '0'
      initArgs[0].should.eql
        host: given['partition connection details'].broker.host
        port: given['partition connection details'].broker.port
        topic: given['partition details'].topic
        partition: given['partition details'].partitionId
        offset: given['partition connection details'].offset
        maxMessageSize: undefined
        polling: undefined


    it 'should emit connecting and connected events when connected', ->
      triggeredConnecting = triggeredConnected = false

      partitionConsumer.on 'connecting', -> triggeredConnecting = true
      partitionConsumer.on 'connected', -> triggeredConnected = true

      partitionConsumer.connect()

      triggeredConnecting.should.be.true
      triggeredConnected.should.be.true

    it 'should emit connecting event event and error when connecting fails', ->
      triggeredConnecting = triggeredError = true;

      sinon.stub(ProzessConsumerStub.prototype, 'connect').yields 'foo!'

      partitionConsumer.removeAllListeners 'error'

      partitionConsumer.on 'connecting', -> triggeredConnecting = true
      partitionConsumer.on 'error', -> triggeredError = true

      partitionConsumer.connect()

      triggeredConnecting.should.be.true
      triggeredError.should.be.true

    it 'should emit config on connecting and connected events when connected', ->
      configConnecting = configConnected = null

      partitionConsumer.on 'connecting', (config) -> configConnecting = config
      partitionConsumer.on 'connected', (config) -> configConnected = config

      partitionConsumer.connect()

      configConnecting.should.eql expectedConfig
      configConnected.should.eql expectedConfig

    it 'should emit connecting event with config details when connection fails', ->
      configConnecting = null

      sinon.stub(ProzessConsumerStub.prototype, 'connect').yields 'foo!'
      partitionConsumer.removeAllListeners 'error'

      partitionConsumer.on 'connecting', (config) -> configConnecting = config
      partitionConsumer.on 'error', ->

      partitionConsumer.connect()

      configConnecting.should.eql expectedConfig

  describe '#_getPartitionConnectionAndOffsetDetails', ->

    it 'should call the zookeeper client with the correct details', (done) ->
      sinon.spy partitionConsumer.zooKafka, 'getPartitionConnectionAndOffsetDetails'
      method = partitionConsumer.zooKafka.getPartitionConnectionAndOffsetDetails

      partitionConsumer._getPartitionConnectionAndOffsetDetails (error, value) ->
        method.calledOnce.should.be.true
        method.calledWith(given['consumer group'], given['partition details']).should.be.true
        done()

  describe '#_registerConsumerOffset', ->

    beforeEach ->
      partitionConsumer.connect()

    it 'should call the zookeeper client with the correct details', (done) ->
      sinon.spy partitionConsumer.zooKafka, 'registerConsumerOffset'
      method = partitionConsumer.zooKafka.registerConsumerOffset

      partitionConsumer._registerConsumerOffset '999', (error, value) ->
        method.calledOnce.should.be.true
        method.calledWith('foobar', given['partition details'], '999').should.be.true
        done()

    it 'should emit offsetUpdate with the old and new offset value', (done) ->
      partitionConsumer.on 'offsetUpdate', (updated, previous) ->
        updated.should.equal '999'
        previous.should.equal '100'
        done()

      partitionConsumer._registerConsumerOffset '999', ->

  describe '#consumeNext', ->

    beforeEach ->
      partitionConsumer.connect()

    describe 'consume from kafka client', ->

      it 'should call the kafka consumer', ->
        consumed = false;
        partitionConsumer.consumer.consume = sinon.stub().yields null, []

        partitionConsumer.consumeNext()

        partitionConsumer.consumer.consume.calledOnce.should.be.true

      it 'should emit the results in a consumed event', ->
        consumed = false;
        partitionConsumer.consumer.consume = sinon.stub().yields null, ['foo']

        partitionConsumer.on 'consumed', (err, data) ->
          should.not.exist err
          data.should.eql ['foo']
          consumed = true

        partitionConsumer.consumeNext()

        consumed.should.be.true

      it 'should emit the error in a consumed event', ->
        consumed = false
        partitionConsumer.consumer.consume = sinon.stub().yields 'foo!', null
        partitionConsumer.removeAllListeners 'error'
        partitionConsumer.on 'error', ->

        partitionConsumer.on 'consumed', (err, data) ->
            err.should.equal 'foo!'
            should.not.exist data
            consumed = true

        partitionConsumer.consumeNext()

        consumed.should.be.true

    describe 'handle error from kafka client', ->

      describe 'OffsetOutOfRange error', ->

        beforeEach ->
          calledBefore = false;
          partitionConsumer.consumer.consume = (cb) ->
            return cb null, [] if calledBefore
            calledBefore = true
            cb message: 'OffsetOutOfRange', null

        it 'should not emit error with the OffsetOutOfRange', ->
          error = false;
          partitionConsumer.consumeNext()

        it 'should emit OffsetOutOfRange with the current offset', ->
          triggered = false;

          partitionConsumer.on 'offsetOutOfRange', (givenOffset) ->
            givenOffset.should.equal given['registered offset']
            triggered = true

          partitionConsumer.consumeNext()

          triggered.should.be.true

        it 'should delegate to #onOffsetOutOfRange', ->
          delegated = partitionConsumer.onOffsetOutOfRange = sinon.stub();

          partitionConsumer.consumeNext();

          delegated.calledOnce.should.be.true;
          delegated.calledWith().should.be.true;

        it 'should try another consumeNext by default', ->
          sinon.spy(partitionConsumer.consumer, 'consume');
          sinon.spy(partitionConsumer, 'consumeNext');

          partitionConsumer.consumeNext();

          partitionConsumer.consumeNext.calledTwice.should.be.true;
          partitionConsumer.consumer.consume.calledTwice.should.be.true;

        it 'should not try another consumeNext when the delegate is changed', ->
          partitionConsumer.onOffsetOutOfRange = sinon.stub();
          sinon.spy(partitionConsumer.consumer, 'consume');
          sinon.spy(partitionConsumer, 'consumeNext');

          partitionConsumer.consumeNext();

          partitionConsumer.consumeNext.calledOnce.should.be.true;
          partitionConsumer.consumer.consume.calledOnce.should.be.true;

      describe 'other errors', ->

        beforeEach ->
          partitionConsumer.consumer.consume = sinon.stub().yields 'foo!', null

        it 'should delegate to #onConsumptionError', ->
          delegated = partitionConsumer.onConsumptionError = sinon.stub()

          partitionConsumer.consumeNext()

          delegated.calledOnce.should.be.true
          delegated.calledWith('foo!').should.be.true

        it 'should emit error event by default', ->
          error = false;
          partitionConsumer.removeAllListeners 'error'
          partitionConsumer.on 'error', (msg, detail) ->
            msg.should.equal 'on consumption'
            detail.should.equal 'foo!'
            error = true

          partitionConsumer.consumeNext();

          error.should.be.true;

        it 'should not emit error event when delegate is changed', ->
          partitionConsumer.onConsumptionError = sinon.stub()
          partitionConsumer.removeAllListeners 'error'

          partitionConsumer.consumeNext()

        it 'should not retry consume by default', ->
          error = false;
          partitionConsumer.removeAllListeners 'error'
          partitionConsumer.on 'error', ->

          sinon.spy(partitionConsumer, 'consumeNext')

          partitionConsumer.consumeNext()

          partitionConsumer.consumeNext.calledOnce.should.be.true
          partitionConsumer.consumer.consume.calledOnce.should.be.true

    describe 'messages received without error', ->

      describe 'empty messages', ->

        beforeEach ->
          calledBefore = false;
          sinon.stub ProzessConsumerStub.prototype, 'consume', (cb) ->
            return cb null, ['foo2']  if calledBefore
            calledBefore = true;
            cb null, []

          partitionConsumer = createTestPartitionConsumer consumeOnConnected: false
          partitionConsumer.connect()

        it 'should delegate to onNoMessages when no messages are received', ->
          delegated = partitionConsumer.onNoMessages = sinon.stub()

          partitionConsumer.consumeNext()

          delegated.calledOnce.should.be.true

        it 'should retry consume by default (after a timeout)', ->
          sinon.spy partitionConsumer, 'consumeNext'
          sinon.spy partitionConsumer, 'onNoMessages'

          partitionConsumer.consumeNext()
          fakeClock.tick DEFAULT_NOMSG_TIMEOUT

          partitionConsumer.onNoMessages.calledOnce.should.be.true
          partitionConsumer.consumeNext.calledTwice.should.be.true
          partitionConsumer.consumer.consume.calledTwice.should.be.true

        it 'should not retry consume when delegate is changed', ->
          delegated = partitionConsumer.onNoMessages = sinon.stub()

          sinon.spy partitionConsumer, 'consumeNext'

          partitionConsumer.consumeNext()
          fakeClock.tick DEFAULT_NOMSG_TIMEOUT

          partitionConsumer.onNoMessages.calledOnce.should.be.true
          partitionConsumer.consumeNext.calledOnce.should.be.true
          partitionConsumer.consumer.consume.calledOnce.should.be.true

        it 'should not publish empty messages on the stream', ->
          count = 0;
          partitionConsumer.on 'data', (data) -> count++

          partitionConsumer.consumeNext()

          count.should.equal(0)

        it 'should publish the first non-empty messages on the stream', ->
          count = 0;
          partitionConsumer.on 'data', (data) ->
            should.exist(data)
            count++

          partitionConsumer.consumeNext()

          fakeClock.tick DEFAULT_NOMSG_TIMEOUT

          count.should.equal 1

      it 'should not delegate to onNoMessages when messages are received', ->
        delegated = partitionConsumer.onNoMessages = sinon.stub()
        partitionConsumer.consumer.consume = sinon.stub().yields null, [ 'foo']

        partitionConsumer.consumeNext()

        delegated.called.should.be.false

      it 'should publish the received message via the stream', ->
        partitionConsumer.connect()
        sinon.spy partitionConsumer, 'push'
        expected = _.extend
          offset:
            current: given['new offset']
            previous: given['current offset']
        , given['partition details']

        partitionConsumer.consumeNext()

        partitionConsumer.push.calledOnce.should.be.true
        partitionConsumer.push.getCall(0).args[0].meta.should.eql expected

        partitionConsumer.push.calledWith(
          messages: ['foo?']
          meta: expected
        ).should.be.true

      it 'should wait when messages are received', ->
        sinon.spy partitionConsumer, 'consumeNext'

        partitionConsumer.consumeNext()

        partitionConsumer.consumeNext.calledOnce.should.be.true
