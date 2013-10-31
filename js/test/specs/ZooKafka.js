// This file has been generated from coffee source files

var FakeZookeeper, ZooKafka, mockery, should, sinon, _;

should = require('should');

sinon = require('sinon');

mockery = require('mockery');

FakeZookeeper = require('zookeeper-hd').FakeZookeeper;

ZooKafka = require('../../src/lib/ZooKafka');

_ = require('underscore');

describe('ZooKafka class', function() {
  var PlusClient, client, zooKafka;
  client = zooKafka = PlusClient = null;
  before(function() {
    var _ref;
    mockery.enable({
      useCleanCache: true
    });
    mockery.registerMock('zookeeper', FakeZookeeper);
    mockery.registerAllowables(['events', 'path', 'async', 'underscore', 'zookeeper-hd', './lib/SimpleClient', './lib/PlusClient', './lib/FakeZookeeper', './SimpleClient', '..']);
    return _ref = require('zookeeper-hd'), PlusClient = _ref.PlusClient, _ref;
  });
  after(function() {
    mockery.deregisterAll();
    return mockery.disable();
  });
  beforeEach(function(done) {
    var count, ready;
    client = new PlusClient();
    count = 0;
    zooKafka = new ZooKafka(client);
    ready = function() {
      count++;
      if (count === 9) {
        return done();
      }
    };
    client.mkdir('/brokers/ids', function() {
      client.create(['/brokers/ids', 1], '127.0.0.1-1373962374351:127.0.0.1:9092', ready);
      return client.create(['/brokers/ids', 5], '127.0.0.1-1373962374351:127.0.0.2:9093', ready);
    });
    client.mkdir('/brokers/topics', function() {
      client.mkdir(['/brokers/topics', 'topic1'], function() {
        client.create(['/brokers/topics', 'topic1', '1'], '1', ready);
        client.create(['/brokers/topics', 'topic1', '2'], '1', ready);
        return client.create(['/brokers/topics', 'topic1', '5'], '2', ready);
      });
      return client.mkdir(['/brokers/topics', 'topic2'], ready);
    });
    return client.mkdir(['/consumers', 'groupA', 'offsets', 'topic1'], function() {
      client.create(['/consumers', 'groupA', 'offsets', 'topic1', '1-0'], '100', ready);
      client.create(['/consumers', 'groupA', 'offsets', 'topic1', '2-0'], '200', ready);
      return client.create(['/consumers', 'groupA', 'offsets', 'topic1', '5-1'], '510', ready);
    });
  });
  describe('construct', function() {
    return it('can be constructed', function() {
      return zooKafka = new ZooKafka(client);
    });
  });
  describe('#getAllRegisteredBrokers', function() {
    return it('should return all registered brokers in a hash', function(done) {
      return zooKafka.getAllRegisteredBrokers(function(err, res) {
        should.not.exist(err);
        res.should.have.keys('1', '5');
        res['1'].should.include({
          host: '127.0.0.1',
          port: '9092',
          id: '1'
        });
        res['5'].should.include({
          host: '127.0.0.2',
          port: '9093',
          id: '5'
        });
        return done();
      });
    });
  });
  describe('#getRegisteredBroker', function() {
    it('should return registered broker details directly', function(done) {
      return zooKafka.getRegisteredBroker('1', function(err, res) {
        should.not.exist(err);
        res.should.include({
          host: '127.0.0.1',
          port: '9092',
          id: '1'
        });
        return done();
      });
    });
    return it('should return error for non-existing broker', function(done) {
      return zooKafka.getRegisteredBroker('2', function(err, res) {
        err.should.have.keys('msg', 'error');
        err.msg.should.equal('Error retrieving broker');
        err.error.should.include({
          rc: FakeZookeeper.ZKCONST.ZNONODE,
          msg: 'no node'
        });
        return done();
      });
    });
  });
  describe('#getAllRegisteredTopics', function() {
    return it('should return all registered topics in a collection', function(done) {
      return zooKafka.getAllRegisteredTopics(function(err, res) {
        should.not.exist(err);
        res.should.eql(['topic1', 'topic2']);
        return done();
      });
    });
  });
  describe('#getRegisteredTopicPartitions', function() {
    it('should return all topic partitions where broker is still registered', function(done) {
      return zooKafka.getRegisteredTopicPartitions('topic1', function(err, res) {
        should.not.exist(err);
        res.should.have.keys('1-0', '5-0', '5-1');
        res['1-0'].should.eql({
          topic: 'topic1',
          brokerPartitionId: '1-0',
          brokerId: '1',
          partitionId: 0
        });
        res['5-0'].should.eql({
          topic: 'topic1',
          brokerPartitionId: '5-0',
          brokerId: '5',
          partitionId: 0
        });
        res['5-1'].should.eql({
          topic: 'topic1',
          brokerPartitionId: '5-1',
          brokerId: '5',
          partitionId: 1
        });
        return done();
      });
    });
    return it('should return all topic partitions', function(done) {
      return zooKafka.getRegisteredTopicPartitions('topic1', {
        onlyRegisteredBrokers: false
      }, function(err, res) {
        should.not.exist(err);
        res.should.have.keys('1-0', '2-0', '5-0', '5-1');
        res['1-0'].should.eql({
          topic: 'topic1',
          brokerPartitionId: '1-0',
          brokerId: '1',
          partitionId: 0
        });
        res['2-0'].should.eql({
          topic: 'topic1',
          brokerPartitionId: '2-0',
          brokerId: '2',
          partitionId: 0
        });
        res['5-0'].should.eql({
          topic: 'topic1',
          brokerPartitionId: '5-0',
          brokerId: '5',
          partitionId: 0
        });
        res['5-1'].should.eql({
          topic: 'topic1',
          brokerPartitionId: '5-1',
          brokerId: '5',
          partitionId: 1
        });
        return done();
      });
    });
  });
  describe('#getRegisteredConsumerOffset', function() {
    it('should return consumer offset', function(done) {
      var topicPartition;
      topicPartition = {
        brokerPartitionId: '1-0',
        topic: 'topic1'
      };
      return zooKafka.getRegisteredConsumerOffset('groupA', topicPartition, function(err, res) {
        should.not.exist(err);
        res.should.include({
          consumerGroup: 'groupA',
          offset: '100',
          brokerPartitionId: '1-0',
          topic: 'topic1'
        });
        return done();
      });
    });
    it('should work with getRegisteredTopicPartitions output', function(done) {
      return zooKafka.getRegisteredTopicPartitions('topic1', function(err, res) {
        return zooKafka.getRegisteredConsumerOffset('groupA', res['1-0'], function(err, res) {
          should.not.exist(err);
          res.should.include({
            consumerGroup: 'groupA',
            offset: '100',
            brokerPartitionId: '1-0',
            topic: 'topic1'
          });
          return done();
        });
      });
    });
    return it('should return consumer offset 0 when no offset is registered', function(done) {
      var topicPartition;
      topicPartition = {
        brokerPartitionId: '5-0',
        topic: 'topic1'
      };
      return zooKafka.getRegisteredConsumerOffset('groupA', topicPartition, function(err, res) {
        should.not.exist(err);
        res.should.include({
          consumerGroup: 'groupA',
          offset: '0',
          brokerPartitionId: '5-0',
          topic: 'topic1'
        });
        return done();
      });
    });
  });
  describe('#_getPartitionConnectionAndOffsetDetails', function() {
    it('should return consumer offset and broker details', function(done) {
      var topicPartition;
      topicPartition = {
        brokerPartitionId: '1-0',
        topic: 'topic1',
        brokerId: '1'
      };
      return zooKafka.getPartitionConnectionAndOffsetDetails('groupA', topicPartition, function(err, res) {
        should.not.exist(err);
        res.should.include({
          consumerGroup: 'groupA',
          offset: '100',
          brokerPartitionId: '1-0',
          brokerId: '1',
          topic: 'topic1'
        });
        res.should.have.property('broker');
        res['broker'].should.include({
          host: '127.0.0.1',
          port: '9092',
          id: '1'
        });
        return done();
      });
    });
    it('should work with getRegisteredTopicPartitions output', function(done) {
      return zooKafka.getRegisteredTopicPartitions('topic1', function(err, res) {
        return zooKafka.getPartitionConnectionAndOffsetDetails('groupA', res['1-0'], function(err, res) {
          should.not.exist(err);
          res.should.include({
            consumerGroup: 'groupA',
            offset: '100',
            brokerPartitionId: '1-0',
            brokerId: '1',
            topic: 'topic1'
          });
          res.should.have.property('broker');
          res['broker'].should.include({
            host: '127.0.0.1',
            port: '9092',
            id: '1'
          });
          return done();
        });
      });
    });
    it('should return consumer offset 0 when no offset is registered', function(done) {
      var topicPartition;
      topicPartition = {
        brokerPartitionId: '5-0',
        topic: 'topic1',
        brokerId: '5'
      };
      return zooKafka.getPartitionConnectionAndOffsetDetails('groupA', topicPartition, function(err, res) {
        should.not.exist(err);
        res.should.include({
          consumerGroup: 'groupA',
          offset: '0',
          brokerPartitionId: '5-0',
          brokerId: '5',
          topic: 'topic1'
        });
        res.should.have.property('broker');
        res['broker'].should.include({
          host: '127.0.0.2',
          port: '9093',
          id: '5'
        });
        return done();
      });
    });
    return it('should return error when broker is not registered', function(done) {
      var topicPartition;
      topicPartition = {
        brokerPartitionId: '2-0',
        topic: 'topic1',
        brokerId: '2'
      };
      return zooKafka.getPartitionConnectionAndOffsetDetails('groupA', topicPartition, function(err, res) {
        err.should.have.keys('msg', 'error');
        err.msg.should.equal('Error retrieving broker');
        err.error.should.include({
          rc: FakeZookeeper.ZKCONST.ZNONODE,
          msg: 'no node'
        });
        return done();
      });
    });
  });
  return describe('#_registerConsumerOffset', function() {
    return it('should create or update an offset', function(done) {
      var topicPartition;
      topicPartition = {
        brokerPartitionId: '1-0',
        topic: 'topic1',
        brokerId: '1'
      };
      return zooKafka.registerConsumerOffset('groupA', topicPartition, 1000, function(error) {
        should.not.exist(error);
        return done();
      });
    });
  });
});

/*
//@ sourceMappingURL=ZooKafka.js.map
*/