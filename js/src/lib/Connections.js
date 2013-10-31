// This file has been generated from coffee source files

var Connections, EventEmitter, TopicConsumer, TopicProducer, ZooKafka,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

EventEmitter = require('events').EventEmitter;

TopicConsumer = require('./TopicConsumer');

TopicProducer = require('./TopicProducer');

ZooKafka = require('./ZooKafka');

module.exports = Connections = (function(_super) {
  __extends(Connections, _super);

  function Connections(zkClient) {
    this.zooKafka = new ZooKafka(zkClient);
    this.topicConsumer = {};
    this.topicProducer = {};
    this.brokerProducer = {};
  }

  Connections.prototype.newTopicConsumer = function(consumerGroup, topic, options) {
    return this.topicConsumer["" + consumerGroup + "-" + topic] = new TopicConsumer(this, consumerGroup, topic, options);
  };

  Connections.prototype.newTopicProducer = function(topic, options) {
    return this.topicProducer[topic] = new TopicProducer(this, topic, options);
  };

  Connections.prototype.newConnectedBrokerProducer = function(brokerDetails) {
    var producer,
      _this = this;
    this.brokerProducer[brokerDetails.brokerId] = producer = new Producer('undefined', {
      host: brokerDetails.host,
      port: brokerDetails.port
    });
    producer.on('error', function(err) {
      _this.emit('brokerProducerError', brokerDetails, err);
      return _this.emit('error', "broker error: " + err, brokerDetails);
    });
    producer.on('brokerReconnectError', function(err) {
      return _this.emit('brokerProducerReconnectError', brokerDetails, err);
    });
    producer.connect();
    return producer;
  };

  Connections.prototype.getOrCreateBrokerProducer = function(brokerDetails) {
    if (brokerId in this.brokerProducer) {
      return this.brokerProducer[brokerDetails.brokerId];
    }
    return this.newConnectedBrokerProducer(brokerDetails);
  };

  return Connections;

})(EventEmitter);

/*
//@ sourceMappingURL=Connections.js.map
*/