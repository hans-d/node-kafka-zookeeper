// This file has been generated from coffee source files

var Readable, mockery, should, sinon, stream, util, uuid, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

stream = require('stream');

Readable = require('stream').Readable;

util = require('util');

mockery = require('mockery');

uuid = require('node-uuid');

should = require('should');

sinon = require('sinon');

_ = require('underscore');

describe('Topic consumer', function() {
  var NodeUuidStub, PartitionConsumer, PartitionConsumerStub, StrategyStub, TopicConsumer, createNewTopicConsumer, givenPartitionIds, givenPartitions, partitionConsumer, topicConsumer, uuidV1Stub, zkClientStub;
  zkClientStub = PartitionConsumer = TopicConsumer = topicConsumer = givenPartitions = givenPartitionIds = partitionConsumer = null;
  uuidV1Stub = null;
  StrategyStub = PartitionConsumerStub = NodeUuidStub = null;
  before(function() {
    StrategyStub = (function() {
      function StrategyStub() {}

      StrategyStub.prototype.on = function() {};

      StrategyStub.prototype.connect = function() {};

      return StrategyStub;

    })();
    PartitionConsumerStub = (function(_super) {
      __extends(PartitionConsumerStub, _super);

      function PartitionConsumerStub() {
        PartitionConsumerStub.__super__.constructor.call(this, {
          objectMode: true
        });
        this.initArgs = arguments;
      }

      PartitionConsumerStub.prototype.connect = function() {
        return this.emit('connected');
      };

      PartitionConsumerStub.prototype._read = function() {};

      return PartitionConsumerStub;

    })(Readable);
    uuidV1Stub = uuid.v1();
    NodeUuidStub = {};
    NodeUuidStub.v1 = function() {
      return uuidV1Stub;
    };
    mockery.enable({
      useCleanCache: true
    });
    mockery.registerMock('node-uuid', NodeUuidStub);
    mockery.registerMock('./PartitionConsumer', PartitionConsumerStub);
    mockery.registerMock('./rebalanceStrategy/Standalone', StrategyStub);
    mockery.registerAllowables(['stream', 'util', 'async', 'underscore', 'zlib', 'crypto', './Compression', '../../src/lib/TopicConsumer']);
    return TopicConsumer = require('../../src/lib/TopicConsumer');
  });
  after(function() {
    mockery.deregisterAll();
    return mockery.disable();
  });
  createNewTopicConsumer = function(options) {
    var tc;
    tc = new TopicConsumer(zkClientStub, 'groupA', 'foo', options);
    tc.on('error', function(msg, detail) {
      throw new Error("should not occur: " + msg + " / " + detail);
    });
    return tc;
  };
  beforeEach(function() {
    zkClientStub = sinon.stub();
    givenPartitions = [
      {
        brokerPartitionId: '1-1'
      }, {
        brokerPartitionId: '2-3'
      }
    ];
    givenPartitionIds = ['1-1', '2-3'];
    return topicConsumer = createNewTopicConsumer();
  });
  it('publishes the data from the partition consumers', function(done) {
    topicConsumer.rebalance(givenPartitions);
    partitionConsumer = topicConsumer.partitionConsumers['1-1'];
    topicConsumer.on('data', function(data) {
      data.should.eql({
        messages: ['foo']
      });
      return done();
    });
    return partitionConsumer.push({
      messages: [
        {
          compression: 0,
          payload: 'foo'
        }
      ]
    });
  });
  describe('construction', function() {
    it('can be constructed with defaults', function() {
      topicConsumer = new TopicConsumer(zkClientStub, 'groupA', 'foo');
      topicConsumer.consumerGroup.should.equal('groupA');
      topicConsumer.topic.should.equal('foo');
      topicConsumer.consumerId.should.equal(uuidV1Stub);
      topicConsumer.partitionConsumers.should.eql({});
      return topicConsumer.rebalancer.should.be.instanceOf(StrategyStub);
    });
    it('can be given a strategy class', function() {
      var StrategyStub2;
      StrategyStub2 = (function() {
        function StrategyStub2() {}

        StrategyStub2.prototype.on = function() {};

        return StrategyStub2;

      })();
      topicConsumer = new TopicConsumer(zkClientStub, 'groupA', 'foo', {
        rebalanceStrategy: StrategyStub2
      });
      topicConsumer.rebalancer.should.not.be.instanceOf(StrategyStub);
      return topicConsumer.rebalancer.should.be.instanceOf(StrategyStub2);
    });
    return it('should stream data via preprocess', function(done) {
      topicConsumer.on('data', function(data) {
        data.should.eql({
          messages: ['foo']
        });
        return done();
      });
      return topicConsumer.preprocess.write({
        messages: [
          {
            compression: 0,
            payload: 'foo'
          }
        ]
      });
    });
  });
  describe('#connect', function() {
    return it('should connect the used rebalance strategy', function() {
      sinon.spy(topicConsumer.rebalancer, 'connect');
      topicConsumer.connect();
      return topicConsumer.rebalancer.connect.calledOnce.should.be["true"];
    });
  });
  describe('#rebalance', function() {
    var partitions;
    partitions = null;
    it('should initially rebalance', function() {
      return topicConsumer.rebalance(givenPartitions);
    });
    it('should currently fail on multiple rebalances', function(done) {
      topicConsumer.removeAllListeners('error');
      topicConsumer.on('error', function(msg, detail) {
        msg.should.equal('rebalance not implemented yet');
        return done();
      });
      topicConsumer.rebalance(givenPartitions);
      return topicConsumer.rebalance(givenPartitions);
    });
    it('should rebalance multiple times');
    return it('should delegate connecting to connectPartitionConsumer', function() {
      var target;
      sinon.spy(topicConsumer, 'connectPartitionConsumer');
      topicConsumer.rebalance(givenPartitions);
      target = topicConsumer.connectPartitionConsumer;
      target.calledTwice.should.be["true"];
      target.calledWith(givenPartitions[0]).should.be["true"];
      return target.calledWith(givenPartitions[1]).should.be["true"];
    });
  });
  return describe('#connectPartitionConsumer', function() {
    beforeEach(function() {
      return topicConsumer = new TopicConsumer(zkClientStub, 'groupA', 'foo');
    });
    it('should construct a partition consumer', function() {
      topicConsumer.connectPartitionConsumer(givenPartitions[0]);
      topicConsumer.partitionConsumers.should.have.keys(['1-1']);
      topicConsumer.partitionConsumers['1-1'].should.be.instanceOf(PartitionConsumerStub);
      return topicConsumer.partitionConsumers['1-1'].initArgs.should.include({
        0: topicConsumer,
        1: givenPartitions[0]
      });
    });
    it('should register created partition consumers in partitionConsumers with partionId as key', function() {
      topicConsumer.connectPartitionConsumer(givenPartitions[0]);
      topicConsumer.connectPartitionConsumer(givenPartitions[1]);
      return topicConsumer.partitionConsumers.should.have.keys(givenPartitionIds);
    });
    describe('bubble events', function() {
      beforeEach(function() {
        topicConsumer = new TopicConsumer(zkClientStub, 'groupA', 'foo');
        topicConsumer.connectPartitionConsumer(givenPartitions[0]);
        return partitionConsumer = topicConsumer.partitionConsumers['1-1'];
      });
      it('should re-emit readable as partitionReadable with the partition id added', function(done) {
        topicConsumer.on('partitionReadable', function(id) {
          id.should.equal('1-1');
          return done();
        });
        return partitionConsumer.emit('readable');
      });
      it('should re-emit connected with the partition id added', function(done) {
        topicConsumer.connectPartitionConsumer(givenPartitions[0]);
        topicConsumer.on('connected', function(id, details) {
          id.should.equal('1-1');
          details.should.eql({
            connection: 'details'
          });
          return done();
        });
        return partitionConsumer.emit('connected', {
          connection: 'details'
        });
      });
      it('should re-emit offsetUpdate with the partition id added', function(done) {
        topicConsumer.on('offsetUpdate', function(id, newOffset) {
          id.should.equal('1-1');
          newOffset.should.equal('100');
          return done();
        });
        return partitionConsumer.emit('offsetUpdate', '100');
      });
      it('should re-emit consuming with the partition id added', function(done) {
        topicConsumer.on('consuming', function(id) {
          id.should.equal('1-1');
          return done();
        });
        return partitionConsumer.emit('consuming');
      });
      it('should re-emit consumed with the partition id added', function(done) {
        topicConsumer.on('consumed', function(id, err, messages) {
          id.should.equal('1-1');
          err.should.equal('err');
          messages.should.eql(['msg1', 'msg2']);
          return done();
        });
        return partitionConsumer.emit('consumed', 'err', ['msg1', 'msg2']);
      });
      it('should re-emit offsetOutOfRange with the partition id added', function(done) {
        topicConsumer.on('offsetOutOfRange', function(id, givenOffset) {
          id.should.equal('1-1');
          givenOffset.should.equal('99');
          return done();
        });
        return partitionConsumer.emit('offsetOutOfRange', '99');
      });
      return it('should re-emit error as partitionError with the partition id added', function(done) {
        topicConsumer.on('partitionError', function(id, msg, detail) {
          id.should.equal('1-1');
          msg.should.equal('oh oh');
          detail.should.equal('dont know');
          return done();
        });
        return partitionConsumer.emit('error', 'oh oh', 'dont know');
      });
    });
    return it('emits events before and after connecting', function() {
      var emitted;
      emitted = [];
      topicConsumer.on('connecting', function() {
        return emitted.push('connecting');
      });
      topicConsumer.on('connected', function() {
        return emitted.push('connected');
      });
      topicConsumer.connectPartitionConsumer(givenPartitions[0]);
      return emitted.should.eql(['connecting', 'connected']);
    });
  });
});

/*
//@ sourceMappingURL=TopicConsumer.js.map
*/