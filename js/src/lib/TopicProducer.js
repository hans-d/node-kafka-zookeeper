// This file has been generated from coffee source files

var EventEmitter, Message, Producer, TopicProducer, Writable, async, brokerSelectionStrategy, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

EventEmitter = require('events').EventEmitter;

Writable = require('stream').Writable;

async = require('async');

Producer = require('prozess').Producer;

_ = require('underscore');

brokerSelectionStrategy = require('./brokerSelectionStrategy');

Message = require('./Message');

/*
  Central producer for a topic, coordinates the actual producers for producing
  to brokers and partitions
*/


module.exports = TopicProducer = (function(_super) {
  __extends(TopicProducer, _super);

  /*
    Constructs the topic producer.
  */


  function TopicProducer(connections, topic, options) {
    options = options || {};
    options.objectMode = true;
    options.decodeStrings = false;
    TopicProducer.__super__.constructor.call(this, options);
    this.connections = connections;
    this.topic = topic;
    this.brokerSelectionStrategy = options.brokerSelectionStrategy || brokerSelectionStrategy.pickOne;
    this.on('brokers', this.connectBrokers);
    this.brokers = {};
  }

  TopicProducer.prototype.connect = function() {
    return this.brokerSelectionStrategy.apply(this);
  };

  TopicProducer.prototype.connectBrokers = function(brokers, defaultId) {
    var _this = this;
    if (_.keys(this.brokers).length !== 0) {
      return this.emit('error', 'reconnect not implemented yet');
    }
    return async.each(_.keys(brokers), function(brokerId, asyncReady) {
      _this.brokers[brokerId] = _this.connections.getOrCreateBrokerProducer(brokers[brokerId]);
      return asyncReady();
    }, function(err) {
      if (err) {
        return _this.emit('error', err);
      }
      _this.defaultBrokerId = defaultId;
      return _this.emit('connected');
    });
  };

  TopicProducer.prototype._write = function(data, encoding, done) {
    var messages, perBroker,
      _this = this;
    messages = Message.asListOfMessages(data);
    perBroker = _.groupBy(messages, function(message) {
      return message.brokerId;
    });
    return async.each(_.keys(perBroker), function(brokerId, asyncBrokerReady) {
      var messagesPerBroker, perPartition;
      messagesPerBroker = perBroker[brokerId];
      perPartition = _.groupBy(messagesPerBroker, function(message) {
        return message.partitionId;
      });
      return async.each(_.keys(perPartition), function(partitionId, asyncPartitionReady) {
        var messagesPerPartition;
        messagesPerPartition = perPartition[partitionId];
        return _this.brokers[_this.defaultBrokerId].send(messagesPerPartition, {
          topic: _this.topic,
          partition: 0
        }, asyncPartitionReady);
      }, function(err) {
        return asyncBrokerReady(err);
      });
    }, function(err) {
      return done(err);
    });
  };

  return TopicProducer;

})(Writable);

/*
//@ sourceMappingURL=TopicProducer.js.map
*/