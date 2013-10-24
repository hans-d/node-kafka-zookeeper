// This file has been generated from coffee source files

var TopicConsumer, ZooKafka, mockery, should, sinon, _;

mockery = require('mockery');

should = require('should');

sinon = require('sinon');

_ = require('underscore');

TopicConsumer = require('../../src/lib/TopicConsumer');

ZooKafka = require('../../src/lib/ZooKafka');

describe('Kafkazoo class', function() {
  var Kafkazoo, ZookeeperClientModuleStub, ZookeeperClientStub, kafka, kafkaConnectionsStub, stubbed;
  ZookeeperClientModuleStub = ZookeeperClientStub = kafkaConnectionsStub = null;
  stubbed = {};
  Kafkazoo = kafka = null;
  before(function() {
    var stubbedClass;
    ZookeeperClientStub = (function() {
      function ZookeeperClientStub() {}

      ZookeeperClientStub.prototype.connect = sinon.stub().yields(null);

      return ZookeeperClientStub;

    })();
    ZookeeperClientModuleStub = {
      PlusClient: sinon.spy(ZookeeperClientStub)
    };
    stubbedClass = function(stubId, extra) {
      return function() {
        var stub;
        stub = stubbed[stubId];
        stub._initArgs = arguments;
        if (extra) {
          extra(stub);
        }
        return stub;
      };
    };
    mockery.enable({
      useCleanCache: true
    });
    mockery.registerMock('zookeeper-hd', ZookeeperClientModuleStub);
    mockery.registerMock('./ZooKafka', stubbedClass('zooKafka'));
    mockery.registerMock('./TopicConsumer', stubbedClass('topicConsumer', function(stub) {
      return stub.connections = stub._initArgs[0];
    }));
    mockery.registerAllowables(['../../src/lib/Kafkazoo', 'events', 'util', 'underscore']);
    return Kafkazoo = require('../../src/lib/Kafkazoo');
  });
  after(function() {
    mockery.deregisterAll();
    return mockery.disable();
  });
  beforeEach(function() {
    stubbed = {};
    stubbed['zooKafka'] = sinon.stub(new ZooKafka());
    kafkaConnectionsStub = (function() {
      function kafkaConnectionsStub() {}

      return kafkaConnectionsStub;

    })();
    kafkaConnectionsStub.zookafka = stubbed['zooKafka'];
    stubbed['topicConsumer'] = sinon.stub(new TopicConsumer(kafkaConnectionsStub));
    return kafka = new Kafkazoo();
  });
  describe('construction', function() {
    it('should by default use localhost:2181 for the zookeeper connection', function() {
      var target;
      target = ZookeeperClientModuleStub.PlusClient;
      target.reset();
      kafka = new Kafkazoo();
      target.callCount.should.equal(1);
      target.calledWithNew().should.be["true"];
      return target.getCall(0).args[0].should.include({
        connect: 'localhost:2181'
      });
    });
    it('should use given zookeeper connection', function() {
      var target;
      target = ZookeeperClientModuleStub.PlusClient;
      target.reset();
      kafka = new Kafkazoo({
        zookeeper: {
          connect: "foobar:1234"
        }
      });
      target.callCount.should.equal(1);
      target.calledWithNew().should.be["true"];
      return target.getCall(0).args[0].should.include({
        connect: 'foobar:1234'
      });
    });
    it('should by default use / as zookeeper root', function() {
      var target;
      target = ZookeeperClientModuleStub.PlusClient;
      target.reset();
      kafka = new Kafkazoo();
      target.callCount.should.equal(1);
      target.calledWithNew().should.be["true"];
      return target.getCall(0).args[0].should.include({
        root: '/'
      });
    });
    it('should use given zookeeper root', function() {
      var target;
      target = ZookeeperClientModuleStub.PlusClient;
      target.reset();
      kafka = new Kafkazoo({
        zookeeper: {
          root: '/some/foor/bar'
        }
      });
      target.callCount.should.equal(1);
      target.calledWithNew().should.be["true"];
      return target.getCall(0).args[0].should.include({
        root: '/some/foor/bar'
      });
    });
    it('should have a connections object', function() {
      kafka = new Kafkazoo();
      return kafka.connections.should.exist;
    });
    return describe('connections object', function() {
      beforeEach(function() {
        return kafka = new Kafkazoo();
      });
      it('should have a zooKafka object', function() {
        kafka.connections.zooKafka.should.exist;
        return kafka.connections.zooKafka.should.equal(stubbed['zooKafka']);
      });
      return it('should have an empty topicConsumer', function() {
        kafka.connections.topicConsumer.should.exist;
        return kafka.connections.topicConsumer.should.eql({});
      });
    });
  });
  describe('#connect', function() {
    it('should connect zookeeper', function() {
      var target;
      target = ZookeeperClientStub.prototype.connect;
      target.reset();
      kafka.connect();
      return target.calledOnce.should.be["true"];
    });
    return it('should emit connected', function() {
      var triggered;
      triggered = false;
      kafka.on('connected', function() {
        return triggered = true;
      });
      kafka.connect();
      return triggered.should.be["true"];
    });
  });
  return describe('createConsumer', function() {
    it('should create a topic consumer with given group and topic', function() {
      kafka.createConsumer('topic', 'group');
      return stubbed['topicConsumer']._initArgs.should.include({
        1: 'group',
        2: 'topic'
      });
    });
    return it('should add created topic consumer to kafka connections', function() {
      kafka.createConsumer('topic', 'group');
      return kafka.connections.topicConsumer.should.have.keys('group-topic');
    });
  });
});

/*
//@ sourceMappingURL=Kafkazoo.js.map
*/