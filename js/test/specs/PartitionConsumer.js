// This file has been generated from coffee source files

var concat, mockery, should, sinon, stream, util, _,
  __indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

stream = require('stream');

util = require('util');

concat = require('concat-stream');

mockery = require('mockery');

should = require('should');

sinon = require('sinon');

_ = require('underscore');

describe('PartitionConsumer class', function() {
  var DEFAULT_NOMSG_TIMEOUT, PartitionConsumer, ProzessConsumerStub, ProzessStub, TopicConsumerStub, ZookeeperClientStub, createTestPartitionConsumer, fakeClock, given, partitionConsumer;
  ProzessStub = ProzessConsumerStub = ZookeeperClientStub = TopicConsumerStub = null;
  PartitionConsumer = partitionConsumer = null;
  fakeClock = null;
  given = {};
  DEFAULT_NOMSG_TIMEOUT = 2000;
  createTestPartitionConsumer = function(options) {
    var p;
    options = _.defaults(options || {}, {
      consumeOnConnected: false
    });
    p = new PartitionConsumer(given['topic consumer instance'], given['partition details'], options);
    p.on('error', function(msg, detail) {
      throw Error("should not be happening: " + msg + " / " + (util.inspect(detail)));
    });
    return p;
  };
  before(function() {
    ProzessConsumerStub = (function() {
      function ProzessConsumerStub() {
        this.initArgs = arguments;
        this.offset = given['current offset'];
      }

      ProzessConsumerStub.prototype.connect = function(cb) {
        return cb(null);
      };

      ProzessConsumerStub.prototype.consume = function(cb) {
        this.offset = given['new offset'];
        return cb(null, ['foo?']);
      };

      ProzessConsumerStub.prototype.getLatestOffset = function(cb) {
        return cb(null);
      };

      return ProzessConsumerStub;

    })();
    ProzessStub = {
      Consumer: sinon.spy(ProzessConsumerStub)
    };
    ZookeeperClientStub = (function() {
      function ZookeeperClientStub() {}

      ZookeeperClientStub.prototype.getPartitionConnectionAndOffsetDetails = function(group, topicPartition, cb) {
        return cb(null, given['partition connection details']);
      };

      ZookeeperClientStub.prototype.registerConsumerOffset = function(consumerGroup, topicPartition, offset, cb) {
        return cb(null);
      };

      return ZookeeperClientStub;

    })();
    TopicConsumerStub = (function() {
      function TopicConsumerStub(consumerGroup) {
        this.zooKafka = new ZookeeperClientStub();
        this.consumerGroup = consumerGroup;
      }

      return TopicConsumerStub;

    })();
    mockery.enable({
      useCleanCache: true
    });
    mockery.registerMock('prozess', ProzessStub);
    mockery.registerAllowables(['../../src/lib/PartitionConsumer', 'stream', 'util', 'bignum', 'underscore', './build/Debug/bignum', './build/Release/bignum']);
    return PartitionConsumer = require('../../src/lib/PartitionConsumer');
  });
  after(function() {
    mockery.deregisterAll();
    return mockery.disable();
  });
  beforeEach(function() {
    given['consumer group'] = 'foobar';
    given['current offset'] = '123';
    given['new offset'] = '150';
    given['registered offset'] = '100';
    given['partition details'] = {
      brokerPartitionId: '1-0',
      brokerId: '1',
      partitionId: 0,
      topic: 'topic1'
    };
    given['partition connection details'] = _.extend({
      consumerGroup: given['consumer group'],
      offset: '100',
      broker: {
        host: '127.0.0.2',
        port: '1234'
      }
    }, given['partition details']);
    given['topic consumer instance'] = new TopicConsumerStub(given['consumer group']);
    fakeClock = sinon.useFakeTimers();
    return partitionConsumer = createTestPartitionConsumer();
  });
  afterEach(function() {
    var method, target, _i, _len, _ref, _results;
    fakeClock.restore();
    _ref = [ProzessConsumerStub];
    _results = [];
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      target = _ref[_i];
      _results.push((function() {
        var _j, _len1, _ref1, _results1;
        _ref1 = _.methods(target.prototype);
        _results1 = [];
        for (_j = 0, _len1 = _ref1.length; _j < _len1; _j++) {
          method = _ref1[_j];
          if (__indexOf.call(_.methods(target.prototype[method]), 'restore') >= 0) {
            _results1.push(target.prototype[method].restore());
          } else {
            _results1.push(void 0);
          }
        }
        return _results1;
      })());
    }
    return _results;
  });
  describe('construction', function() {
    describe('creates a consumer', function() {
      describe('re-use data from topic consumer', function() {
        beforeEach(function() {
          return partitionConsumer = new PartitionConsumer(given['topic consumer instance'], given['partition details']);
        });
        it('should reuse the zookeeper client', function() {
          return partitionConsumer.zooKafka.should.equal(given['topic consumer instance'].zooKafka);
        });
        return it('should reuse the consumergroup', function() {
          return partitionConsumer.consumerGroup.should.equal(given['topic consumer instance'].consumerGroup);
        });
      });
      it('should have a default onNoMessages handler', function() {
        partitionConsumer = new PartitionConsumer(given['topic consumer instance'], given['partition details']);
        return partitionConsumer.onNoMessages.should.be["instanceof"](Function);
      });
      it('should have a given onNoMessages handler', function() {
        var userdefined;
        userdefined = function() {
          return 'foo no messages';
        };
        partitionConsumer = new PartitionConsumer(given['topic consumer instance'], given['partition details'], {
          onNoMessages: userdefined
        });
        return partitionConsumer.onNoMessages.should.equal(userdefined);
      });
      it('should have a default onOffsetOutOfRange handler', function() {
        partitionConsumer = new PartitionConsumer(given['topic consumer instance'], given['partition details']);
        return partitionConsumer.onOffsetOutOfRange.should.be["instanceof"](Function);
      });
      it('should have a default onOffsetOutOfRange handler', function() {
        var userdefined;
        userdefined = function() {
          return 'foo offset out of range';
        };
        partitionConsumer = new PartitionConsumer(given['topic consumer instance'], given['partition details'], {
          onOffsetOutOfRange: userdefined
        });
        return partitionConsumer.onOffsetOutOfRange.should.equal(userdefined);
      });
      it('should not have a kafka consumer yet', function() {
        partitionConsumer = new PartitionConsumer(given['topic consumer instance'], given['partition details']);
        return should.not.exist(partitionConsumer.consumer);
      });
      it('should consume when connected', function() {
        partitionConsumer = new PartitionConsumer(given['topic consumer instance'], given['partition details']);
        partitionConsumer.listeners('connected').length.should.equal(1);
        return partitionConsumer.listeners('connected')[0].should.equal(partitionConsumer.consumeNext);
      });
      it('should not consume when connected if specified', function() {
        partitionConsumer = new PartitionConsumer(given['topic consumer instance'], given['partition details'], {
          consumeOnConnected: false
        });
        return partitionConsumer.listeners('connected').length.should.equal(0);
      });
      it('should have an offset property that is empty when not connected', function() {
        return should.not.exist(partitionConsumer.offset);
      });
      return it('should have an offset property that offers the real offset of the kafka client', function() {
        partitionConsumer.connect();
        return partitionConsumer.offset.should.equal(partitionConsumer.consumer.offset);
      });
    });
    return describe('default handlers', function() {
      describe('onNoMessages', function() {
        it('continues consuming after the timeout', function(done) {
          var triggered;
          triggered = false;
          sinon.stub(partitionConsumer, 'consumeNext', function() {
            triggered = true;
            return done();
          });
          partitionConsumer.onNoMessages(partitionConsumer);
          fakeClock.tick(DEFAULT_NOMSG_TIMEOUT - 200);
          triggered.should.be["false"];
          fakeClock.tick(DEFAULT_NOMSG_TIMEOUT);
          return triggered.should.be["true"];
        });
        return it('has a configurable timeout', function(done) {
          var triggered;
          triggered = false;
          partitionConsumer = createTestPartitionConsumer({
            noMessagesTimeout: 100
          });
          sinon.stub(partitionConsumer, 'consumeNext', function() {
            triggered = true;
            return done();
          });
          partitionConsumer.onNoMessages(partitionConsumer);
          fakeClock.tick(100);
          return triggered.should.be["true"];
        });
      });
      describe('onOffsetOutOfRange', function() {
        beforeEach(function() {
          var offsetUpdated;
          offsetUpdated = false;
          partitionConsumer.connect();
          return partitionConsumer.consumer.getLatestOffset = sinon.stub().yields(null, given['new offset']);
        });
        it('should continue consuming after correction', function(done) {
          var triggered;
          triggered = false;
          sinon.stub(partitionConsumer, 'consumeNext', function() {
            triggered = true;
            return done();
          });
          partitionConsumer.onOffsetOutOfRange(partitionConsumer);
          return triggered.should.be["true"];
        });
        it('should reset offset to latest offset', function() {
          partitionConsumer.consumeNext = sinon.stub();
          sinon.spy(partitionConsumer, '_registerConsumerOffset');
          partitionConsumer.onOffsetOutOfRange(partitionConsumer);
          partitionConsumer.consumer.getLatestOffset.calledOnce.should.be["true"];
          partitionConsumer._registerConsumerOffset.calledOnce.should.be["true"];
          return partitionConsumer._registerConsumerOffset.calledWith(given['new offset']).should.be["true"];
        });
        return it('should fail when retrieving offset fails', function(done) {
          partitionConsumer.consumer.getLatestOffset = sinon.stub().yields('foo');
          sinon.stub(partitionConsumer, 'fatal', function(msg, detail) {
            msg.should.equal('retrieving latest offset');
            return done();
          });
          return partitionConsumer.onOffsetOutOfRange(partitionConsumer);
        });
      });
      return describe('onConsumptionError', function() {
        return it('should be fatal', function(done) {
          sinon.stub(partitionConsumer, 'fatal', function(msg, detail) {
            msg.should.equal('on consumption');
            detail.should.equal('foo error');
            return done();
          });
          return partitionConsumer.onConsumptionError(partitionConsumer, 'foo error');
        });
      });
    });
  });
  describe('readable stream', function() {
    it('should implement #_read', function() {
      partitionConsumer.connect();
      return partitionConsumer._read();
    });
    it('should be possible to pipe to another stream', function(done) {
      var target;
      target = concat(function(data) {
        data.should.eql([
          {
            msg: 1
          }, {
            msg: 2
          }
        ]);
        return done();
      });
      partitionConsumer.pipe(target);
      partitionConsumer.push([
        {
          msg: 1
        }, {
          msg: 2
        }
      ]);
      return partitionConsumer.push(null);
    });
    return it('should be readable', function(done) {
      partitionConsumer.on('readable', function() {
        var data;
        data = partitionConsumer.read();
        data.should.eql([
          {
            msg: 1
          }, {
            msg: 2
          }
        ]);
        return done();
      });
      return partitionConsumer.push([
        {
          msg: 1
        }, {
          msg: 2
        }
      ]);
    });
  });
  describe('#connect', function() {
    var expectedConfig;
    expectedConfig = null;
    before(function() {
      return expectedConfig = {
        host: given['partition connection details'].broker.host,
        port: given['partition connection details'].broker.port,
        topic: given['partition details'].topic,
        partition: given['partition details'].partitionId,
        offset: given['partition connection details'].offset,
        maxMessageSize: void 0,
        polling: void 0
      };
    });
    it('should retrieve the connection details from zookeeper', function() {
      var method;
      sinon.spy(given['topic consumer instance'].zooKafka, 'getPartitionConnectionAndOffsetDetails');
      method = given['topic consumer instance'].zooKafka.getPartitionConnectionAndOffsetDetails;
      partitionConsumer.connect();
      method.calledOnce.should.be["true"];
      return method.calledWith(given['consumer group'], given['partition details']).should.be["true"];
    });
    it('should connect to kafka using info from zookeeper', function() {
      var initArgs;
      ProzessStub.Consumer.reset();
      should.not.exist(partitionConsumer.consumer);
      partitionConsumer.connect();
      partitionConsumer.consumer.should.be["instanceof"](ProzessConsumerStub);
      ProzessStub.Consumer.calledWithNew();
      initArgs = partitionConsumer.consumer.initArgs;
      initArgs.should.have.keys('0');
      return initArgs[0].should.eql({
        host: given['partition connection details'].broker.host,
        port: given['partition connection details'].broker.port,
        topic: given['partition details'].topic,
        partition: given['partition details'].partitionId,
        offset: given['partition connection details'].offset,
        maxMessageSize: void 0,
        polling: void 0
      });
    });
    it('should emit connecting and connected events when connected', function() {
      var triggeredConnected, triggeredConnecting;
      triggeredConnecting = triggeredConnected = false;
      partitionConsumer.on('connecting', function() {
        return triggeredConnecting = true;
      });
      partitionConsumer.on('connected', function() {
        return triggeredConnected = true;
      });
      partitionConsumer.connect();
      triggeredConnecting.should.be["true"];
      return triggeredConnected.should.be["true"];
    });
    it('should emit connecting event event and error when connecting fails', function() {
      var triggeredConnecting, triggeredError;
      triggeredConnecting = triggeredError = true;
      sinon.stub(ProzessConsumerStub.prototype, 'connect').yields('foo!');
      partitionConsumer.removeAllListeners('error');
      partitionConsumer.on('connecting', function() {
        return triggeredConnecting = true;
      });
      partitionConsumer.on('error', function() {
        return triggeredError = true;
      });
      partitionConsumer.connect();
      triggeredConnecting.should.be["true"];
      return triggeredError.should.be["true"];
    });
    it('should emit config on connecting and connected events when connected', function() {
      var configConnected, configConnecting;
      configConnecting = configConnected = null;
      partitionConsumer.on('connecting', function(config) {
        return configConnecting = config;
      });
      partitionConsumer.on('connected', function(config) {
        return configConnected = config;
      });
      partitionConsumer.connect();
      configConnecting.should.eql(expectedConfig);
      return configConnected.should.eql(expectedConfig);
    });
    return it('should emit connecting event with config details when connection fails', function() {
      var configConnecting;
      configConnecting = null;
      sinon.stub(ProzessConsumerStub.prototype, 'connect').yields('foo!');
      partitionConsumer.removeAllListeners('error');
      partitionConsumer.on('connecting', function(config) {
        return configConnecting = config;
      });
      partitionConsumer.on('error', function() {});
      partitionConsumer.connect();
      return configConnecting.should.eql(expectedConfig);
    });
  });
  describe('#_getPartitionConnectionAndOffsetDetails', function() {
    return it('should call the zookeeper client with the correct details', function(done) {
      var method;
      sinon.spy(partitionConsumer.zooKafka, 'getPartitionConnectionAndOffsetDetails');
      method = partitionConsumer.zooKafka.getPartitionConnectionAndOffsetDetails;
      return partitionConsumer._getPartitionConnectionAndOffsetDetails(function(error, value) {
        method.calledOnce.should.be["true"];
        method.calledWith(given['consumer group'], given['partition details']).should.be["true"];
        return done();
      });
    });
  });
  describe('#_registerConsumerOffset', function() {
    beforeEach(function() {
      return partitionConsumer.connect();
    });
    it('should call the zookeeper client with the correct details', function(done) {
      var method;
      sinon.spy(partitionConsumer.zooKafka, 'registerConsumerOffset');
      method = partitionConsumer.zooKafka.registerConsumerOffset;
      return partitionConsumer._registerConsumerOffset('999', function(error, value) {
        method.calledOnce.should.be["true"];
        method.calledWith('foobar', given['partition details'], '999').should.be["true"];
        return done();
      });
    });
    return it('should emit offsetUpdate with the old and new offset value', function(done) {
      partitionConsumer.on('offsetUpdate', function(updated, previous) {
        updated.should.equal('999');
        previous.should.equal('100');
        return done();
      });
      return partitionConsumer._registerConsumerOffset('999', function() {});
    });
  });
  return describe('#consumeNext', function() {
    beforeEach(function() {
      return partitionConsumer.connect();
    });
    describe('consume from kafka client', function() {
      it('should call the kafka consumer', function() {
        var consumed;
        consumed = false;
        partitionConsumer.consumer.consume = sinon.stub().yields(null, []);
        partitionConsumer.consumeNext();
        return partitionConsumer.consumer.consume.calledOnce.should.be["true"];
      });
      it('should emit the results in a consumed event', function() {
        var consumed;
        consumed = false;
        partitionConsumer.consumer.consume = sinon.stub().yields(null, ['foo']);
        partitionConsumer.on('consumed', function(err, data) {
          should.not.exist(err);
          data.should.eql(['foo']);
          return consumed = true;
        });
        partitionConsumer.consumeNext();
        return consumed.should.be["true"];
      });
      return it('should emit the error in a consumed event', function() {
        var consumed;
        consumed = false;
        partitionConsumer.consumer.consume = sinon.stub().yields('foo!', null);
        partitionConsumer.removeAllListeners('error');
        partitionConsumer.on('error', function() {});
        partitionConsumer.on('consumed', function(err, data) {
          err.should.equal('foo!');
          should.not.exist(data);
          return consumed = true;
        });
        partitionConsumer.consumeNext();
        return consumed.should.be["true"];
      });
    });
    describe('handle error from kafka client', function() {
      describe('OffsetOutOfRange error', function() {
        beforeEach(function() {
          var calledBefore;
          calledBefore = false;
          return partitionConsumer.consumer.consume = function(cb) {
            if (calledBefore) {
              return cb(null, []);
            }
            calledBefore = true;
            return cb({
              message: 'OffsetOutOfRange'
            }, null);
          };
        });
        it('should not emit error with the OffsetOutOfRange', function() {
          var error;
          error = false;
          return partitionConsumer.consumeNext();
        });
        it('should emit OffsetOutOfRange with the current offset', function() {
          var triggered;
          triggered = false;
          partitionConsumer.on('offsetOutOfRange', function(givenOffset) {
            givenOffset.should.equal(given['registered offset']);
            return triggered = true;
          });
          partitionConsumer.consumeNext();
          return triggered.should.be["true"];
        });
        it('should delegate to #onOffsetOutOfRange', function() {
          var delegated;
          delegated = partitionConsumer.onOffsetOutOfRange = sinon.stub();
          partitionConsumer.consumeNext();
          delegated.calledOnce.should.be["true"];
          return delegated.calledWith(partitionConsumer).should.be["true"];
        });
        it('should try another consumeNext by default', function() {
          sinon.spy(partitionConsumer.consumer, 'consume');
          sinon.spy(partitionConsumer, 'consumeNext');
          partitionConsumer.consumeNext();
          partitionConsumer.consumeNext.calledTwice.should.be["true"];
          return partitionConsumer.consumer.consume.calledTwice.should.be["true"];
        });
        return it('should not try another consumeNext when the delegate is changed', function() {
          partitionConsumer.onOffsetOutOfRange = sinon.stub();
          sinon.spy(partitionConsumer.consumer, 'consume');
          sinon.spy(partitionConsumer, 'consumeNext');
          partitionConsumer.consumeNext();
          partitionConsumer.consumeNext.calledOnce.should.be["true"];
          return partitionConsumer.consumer.consume.calledOnce.should.be["true"];
        });
      });
      return describe('other errors', function() {
        beforeEach(function() {
          return partitionConsumer.consumer.consume = sinon.stub().yields('foo!', null);
        });
        it('should delegate to #onConsumptionError', function() {
          var delegated;
          delegated = partitionConsumer.onConsumptionError = sinon.stub();
          partitionConsumer.consumeNext();
          delegated.calledOnce.should.be["true"];
          return delegated.calledWith(partitionConsumer, 'foo!').should.be["true"];
        });
        it('should emit error event by default', function() {
          var error;
          error = false;
          partitionConsumer.removeAllListeners('error');
          partitionConsumer.on('error', function(msg, detail) {
            msg.should.equal('on consumption');
            detail.should.equal('foo!');
            return error = true;
          });
          partitionConsumer.consumeNext();
          return error.should.be["true"];
        });
        it('should not emit error event when delegate is changed', function() {
          partitionConsumer.onConsumptionError = sinon.stub();
          partitionConsumer.removeAllListeners('error');
          return partitionConsumer.consumeNext();
        });
        return it('should not retry consume by default', function() {
          var error;
          error = false;
          partitionConsumer.removeAllListeners('error');
          partitionConsumer.on('error', function() {});
          sinon.spy(partitionConsumer, 'consumeNext');
          partitionConsumer.consumeNext();
          partitionConsumer.consumeNext.calledOnce.should.be["true"];
          return partitionConsumer.consumer.consume.calledOnce.should.be["true"];
        });
      });
    });
    return describe('messages received without error', function() {
      describe('empty messages', function() {
        beforeEach(function() {
          var calledBefore;
          calledBefore = false;
          sinon.stub(ProzessConsumerStub.prototype, 'consume', function(cb) {
            if (calledBefore) {
              return cb(null, ['foo2']);
            }
            calledBefore = true;
            return cb(null, []);
          });
          partitionConsumer = createTestPartitionConsumer({
            consumeOnConnected: false
          });
          return partitionConsumer.connect();
        });
        it('should delegate to onNoMessages when no messages are received', function() {
          var delegated;
          delegated = partitionConsumer.onNoMessages = sinon.stub();
          partitionConsumer.consumeNext();
          delegated.calledOnce.should.be["true"];
          return delegated.calledWith(partitionConsumer).should.be["true"];
        });
        it('should retry consume by default (after a timeout)', function() {
          sinon.spy(partitionConsumer, 'consumeNext');
          sinon.spy(partitionConsumer, 'onNoMessages');
          partitionConsumer.consumeNext();
          fakeClock.tick(DEFAULT_NOMSG_TIMEOUT);
          partitionConsumer.onNoMessages.calledOnce.should.be["true"];
          partitionConsumer.consumeNext.calledTwice.should.be["true"];
          return partitionConsumer.consumer.consume.calledTwice.should.be["true"];
        });
        it('should not retry consume when delegate is changed', function() {
          var delegated;
          delegated = partitionConsumer.onNoMessages = sinon.stub();
          sinon.spy(partitionConsumer, 'consumeNext');
          partitionConsumer.consumeNext();
          fakeClock.tick(DEFAULT_NOMSG_TIMEOUT);
          partitionConsumer.onNoMessages.calledOnce.should.be["true"];
          partitionConsumer.consumeNext.calledOnce.should.be["true"];
          return partitionConsumer.consumer.consume.calledOnce.should.be["true"];
        });
        it('should not publish empty messages on the stream', function() {
          var count;
          count = 0;
          partitionConsumer.on('data', function(data) {
            return count++;
          });
          partitionConsumer.consumeNext();
          return count.should.equal(0);
        });
        return it('should publish the first non-empty messages on the stream', function() {
          var count;
          count = 0;
          partitionConsumer.on('data', function(data) {
            should.exist(data);
            return count++;
          });
          partitionConsumer.consumeNext();
          fakeClock.tick(DEFAULT_NOMSG_TIMEOUT);
          return count.should.equal(1);
        });
      });
      it('should not delegate to onNoMessages when messages are received', function() {
        var delegated;
        delegated = partitionConsumer.onNoMessages = sinon.stub();
        partitionConsumer.consumer.consume = sinon.stub().yields(null, ['foo']);
        partitionConsumer.consumeNext();
        return delegated.called.should.be["false"];
      });
      it('should publish the received message via the stream', function() {
        var expected;
        partitionConsumer.connect();
        sinon.spy(partitionConsumer, 'push');
        expected = _.extend({
          offset: {
            current: given['new offset'],
            previous: given['current offset']
          }
        }, given['partition details']);
        partitionConsumer.consumeNext();
        partitionConsumer.push.calledOnce.should.be["true"];
        partitionConsumer.push.getCall(0).args[0].meta.should.eql(expected);
        return partitionConsumer.push.calledWith({
          messages: ['foo?'],
          meta: expected
        }).should.be["true"];
      });
      return it('should wait when messages are received', function() {
        sinon.spy(partitionConsumer, 'consumeNext');
        partitionConsumer.consumeNext();
        return partitionConsumer.consumeNext.calledOnce.should.be["true"];
      });
    });
  });
});

/*
//@ sourceMappingURL=PartitionConsumer.js.map
*/