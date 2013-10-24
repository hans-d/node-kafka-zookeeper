stream = require 'stream'
{Readable} = require 'stream'
util = require 'util'

mockery = require 'mockery'
uuid = require 'node-uuid'
should = require 'should'
sinon = require 'sinon'
_ = require 'underscore'

describe 'Topic consumer', ->
  zkClientStub = PartitionConsumer = TopicConsumer =
    topicConsumer = givenPartitions = givenPartitionIds = partitionConsumer = null
  uuidV1Stub = null
  StrategyStub = PartitionConsumerStub = NodeUuidStub = null

  before ->
    class StrategyStub
      on: ->
      connect: ->

    class PartitionConsumerStub extends Readable
      constructor: ->
        super objectMode: true
        @initArgs = arguments

      connect: ->
        @emit 'connected'

      _read: ->

    uuidV1Stub = uuid.v1()

    NodeUuidStub = {}
    NodeUuidStub.v1 = -> uuidV1Stub

    mockery.enable useCleanCache: true
    mockery.registerMock 'node-uuid', NodeUuidStub
    mockery.registerMock './PartitionConsumer', PartitionConsumerStub
    mockery.registerMock './rebalanceStrategy/Standalone', StrategyStub
    mockery.registerAllowables [
      'stream', 'util',
      'async', 'underscore',
      'zlib', 'crypto',
      './Compression', '../../src/lib/TopicConsumer'
    ]

    TopicConsumer = require '../../src/lib/TopicConsumer'

  after ->
    mockery.deregisterAll()
    mockery.disable()

  createNewTopicConsumer = (options) ->
      tc = new TopicConsumer zkClientStub, 'groupA', 'foo', options
      tc.on 'error', (msg, detail) ->
        throw new Error "should not occur: #{msg} / #{detail}"
      tc

  beforeEach ->
    zkClientStub = sinon.stub()
    givenPartitions = [ {brokerPartitionId: '1-1'}, {brokerPartitionId: '2-3'} ]
    givenPartitionIds = [ '1-1', '2-3']

    topicConsumer = createNewTopicConsumer()


  it 'publishes the data from the partition consumers', (done) ->
    topicConsumer.rebalance givenPartitions
    partitionConsumer = topicConsumer.partitionConsumers['1-1']

    topicConsumer.on 'data', (data) ->
      data.should.eql messages: ['foo']
      done()

    partitionConsumer.push
      messages: [ { compression:0, payload: 'foo' } ]

  describe 'construction', ->

    it 'can be constructed with defaults', ->
      topicConsumer = new TopicConsumer zkClientStub, 'groupA', 'foo'

      topicConsumer.consumerGroup.should.equal 'groupA'
      topicConsumer.topic.should.equal 'foo'
      topicConsumer.consumerId.should.equal uuidV1Stub
      topicConsumer.partitionConsumers.should.eql {}

      topicConsumer.rebalancer.should.be.instanceOf StrategyStub

    it 'can be given a strategy class', ->
      class StrategyStub2
        on : ->

      topicConsumer = new TopicConsumer zkClientStub, 'groupA', 'foo', rebalanceStrategy: StrategyStub2

      topicConsumer.rebalancer.should.not.be.instanceOf StrategyStub
      topicConsumer.rebalancer.should.be.instanceOf StrategyStub2

    it 'should stream data via preprocess', (done) ->
      topicConsumer.on 'data', (data) ->
        data.should.eql messages: [ 'foo' ]
        done()

      topicConsumer.preprocess.write
        messages: [{ compression:0, payload: 'foo'}]

  describe '#connect', ->

    it 'should connect the used rebalance strategy', ->
      sinon.spy topicConsumer.rebalancer, 'connect'

      topicConsumer.connect()

      topicConsumer.rebalancer.connect.calledOnce.should.be.true

  describe '#rebalance', ->
      partitions = null

      it 'should initially rebalance', ->
        topicConsumer.rebalance(givenPartitions)

      it 'should currently fail on multiple rebalances', (done) ->
        topicConsumer.removeAllListeners('error');
        topicConsumer.on 'error', (msg, detail) ->
            msg.should.equal('rebalance not implemented yet');
            done();

        topicConsumer.rebalance(givenPartitions);
        topicConsumer.rebalance(givenPartitions);

      it 'should rebalance multiple times'

      it 'should delegate connecting to connectPartitionConsumer', ->
          sinon.spy(topicConsumer, 'connectPartitionConsumer');

          topicConsumer.rebalance(givenPartitions);

          target = topicConsumer.connectPartitionConsumer;
          target.calledTwice.should.be.true;
          target.calledWith(givenPartitions[0]).should.be.true;
          target.calledWith(givenPartitions[1]).should.be.true;

  describe '#connectPartitionConsumer', ->

      beforeEach ->
          topicConsumer = new TopicConsumer(zkClientStub, 'groupA', 'foo')

      it 'should construct a partition consumer', ->
          topicConsumer.connectPartitionConsumer(givenPartitions[0]);

          topicConsumer.partitionConsumers.should.have.keys(['1-1']);
          topicConsumer.partitionConsumers['1-1'].should.be.instanceOf(PartitionConsumerStub);
          topicConsumer.partitionConsumers['1-1'].initArgs.should.include
              0: topicConsumer,
              1: givenPartitions[0]

      it 'should register created partition consumers in partitionConsumers with partionId as key', ->
          topicConsumer.connectPartitionConsumer(givenPartitions[0]);
          topicConsumer.connectPartitionConsumer(givenPartitions[1]);

          topicConsumer.partitionConsumers.should.have.keys(givenPartitionIds);

      describe 'bubble events', ->


        beforeEach ->
          topicConsumer = new TopicConsumer zkClientStub, 'groupA', 'foo'
          topicConsumer.connectPartitionConsumer givenPartitions[0]
          partitionConsumer = topicConsumer.partitionConsumers['1-1']


        it 'should re-emit readable as partitionReadable with the partition id added', (done) ->
          topicConsumer.on 'partitionReadable', (id) ->
            id.should.equal '1-1'
            done()

          partitionConsumer.emit 'readable'


        it 'should re-emit connected with the partition id added', (done) ->
          topicConsumer.connectPartitionConsumer givenPartitions[0]
          topicConsumer.on 'connected', (id, details) ->
            id.should.equal '1-1'
            details.should.eql connection: 'details'
            done()

          partitionConsumer.emit 'connected', connection: 'details'


        it 'should re-emit offsetUpdate with the partition id added', (done) ->
          topicConsumer.on 'offsetUpdate', (id, newOffset) ->
              id.should.equal '1-1'
              newOffset.should.equal '100'
              done()

          partitionConsumer.emit 'offsetUpdate', '100'


        it 'should re-emit consuming with the partition id added', (done) ->
          topicConsumer.on 'consuming', (id) ->
            id.should.equal '1-1'
            done()

          partitionConsumer.emit 'consuming'


        it 'should re-emit consumed with the partition id added', (done) ->
          topicConsumer.on 'consumed', (id, err, messages) ->
            id.should.equal '1-1'
            err.should.equal 'err'
            messages.should.eql ['msg1', 'msg2']
            done()

          partitionConsumer.emit 'consumed', 'err', ['msg1', 'msg2']


        it 'should re-emit offsetOutOfRange with the partition id added', (done) ->
          topicConsumer.on 'offsetOutOfRange', (id, givenOffset) ->
            id.should.equal '1-1'
            givenOffset.should.equal '99'
            done()

          partitionConsumer.emit 'offsetOutOfRange', '99'


        it 'should re-emit error as partitionError with the partition id added', (done) ->
          topicConsumer.on 'partitionError', (id, msg, detail) ->
            id.should.equal '1-1'
            msg.should.equal 'oh oh'
            detail.should.equal 'dont know'
            done()
          partitionConsumer.emit 'error', 'oh oh', 'dont know'


      it 'emits events before and after connecting', ->
        emitted = []
        topicConsumer.on 'connecting', -> emitted.push 'connecting'
        topicConsumer.on 'connected', -> emitted.push 'connected'

        topicConsumer.connectPartitionConsumer givenPartitions[0]

        emitted.should.eql ['connecting', 'connected']
