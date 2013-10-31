// This file has been generated from coffee source files

var FakeProzess, mockery, should, sinon;

mockery = require('mockery');

should = require('should');

sinon = require('sinon');

FakeProzess = require('../../src/lib/FakeProzess');

describe('TopicProducer class', function() {
  var BrokerSelectionStrategy, TopicProducer, topicProducer;
  BrokerSelectionStrategy = TopicProducer = null;
  topicProducer = null;
  before(function() {
    BrokerSelectionStrategy = {
      pickOne: function() {
        return 1;
      }
    };
    mockery.enable({
      useCleanCache: true
    });
    mockery.registerMock('prozess', FakeProzess);
    mockery.registerMock('./brokerSelectionStrategy', BrokerSelectionStrategy);
    mockery.registerAllowables(['../../src/lib/TopicProducer', './brokerSelectionStrategy', './Message', 'events', 'stream', 'async', 'underscore']);
    return TopicProducer = require('../../src/lib/TopicProducer');
  });
  beforeEach(function() {
    return topicProducer = new TopicProducer();
  });
  describe('construction', function() {
    it('can be constructed', function() {
      return topicProducer = new TopicProducer();
    });
    return it('has a default broker selection strategy', function() {
      topicProducer = new TopicProducer();
      return topicProducer.brokerSelectionStrategy.should.be.equal(BrokerSelectionStrategy.pickOne);
    });
  });
  describe('#connect', function() {
    return it('calls broker selection strateggy', function() {
      sinon.spy(topicProducer, 'brokerSelectionStrategy');
      topicProducer.connect();
      return topicProducer.brokerSelectionStrategy.calledOnce.should.be["true"];
    });
  });
  describe('#connectBrokers', function() {});
  return describe('#_write', function() {
    beforeEach(function() {
      topicProducer.defaultBrokerId = 'test';
      return topicProducer.brokers['test'] = {
        send: sinon.stub().yields(null)
      };
    });
    it('should send messages', function() {
      topicProducer._write('test', null, function() {});
      return topicProducer.brokers.test.send.calledOnce.should.be["true"];
    });
    return it('should provide a correct message', function() {
      var firstMessage, messages;
      topicProducer._write('test', null, function() {});
      messages = topicProducer.brokers.test.send.getCall(0).args[0];
      firstMessage = messages[0];
      firstMessage.should.be["instanceof"](FakeProzess.Message);
      return firstMessage.payload.should.equal('test');
    });
  });
});

/*
//@ sourceMappingURL=TopicProducer.js.map
*/