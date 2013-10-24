// This file has been generated from coffee source files

var Compression, PartitionConsumer, Readable, StandaloneStrategy, TopicConsumer, async, util, uuid, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

Readable = require('stream').Readable;

util = require('util');

async = require('async');

uuid = require('node-uuid');

_ = require('underscore');

Compression = require('./Compression');

PartitionConsumer = require('./PartitionConsumer');

StandaloneStrategy = require('./rebalanceStrategy/Standalone');

module.exports = TopicConsumer = (function(_super) {
  __extends(TopicConsumer, _super);

  function TopicConsumer(connections, consumerGroup, topic, options) {
    var rebalanceStrategy,
      _this = this;
    TopicConsumer.__super__.constructor.call(this, {
      objectMode: true
    });
    options = options || {};
    this.connections = connections;
    this.topic = topic;
    this.consumerGroup = consumerGroup;
    this.consumerId = options.consumerId || uuid.v1();
    rebalanceStrategy = options.rebalanceStrategy || StandaloneStrategy;
    this.rebalancer = new rebalanceStrategy(this.connections, this.consumerGroup, this.topic, this.consumerId);
    this.rebalancer.on('partitions', this.rebalance);
    this.partitionConsumers = {};
    this.partitionConsumerConfig = {};
    this.preprocess = new Compression.Decompressor();
    this.preprocess.on('error', function(msg, detail) {
      return _this.emit('error', msg, detail);
    });
    this.preprocess.on('readable', function() {
      var data;
      data = _this.preprocess.read();
      return _this.push(data);
    });
  }

  TopicConsumer.prototype.connect = function() {
    return this.rebalancer.connect();
  };

  TopicConsumer.prototype._read = function() {};

  TopicConsumer.prototype.rebalance = function(partitions) {
    var _this = this;
    if (_.keys(this.partitionConsumers).length !== 0) {
      return this.emit('error', 'rebalance not implemented yet');
    }
    return async.each(partitions, function(partition, asyncReady) {
      _this.connectPartitionConsumer(partition);
      return asyncReady();
    });
  };

  TopicConsumer.prototype.connectPartitionConsumer = function(partition) {
    var event, id, partitionConsumer, _fn, _i, _len, _ref,
      _this = this;
    id = partition.brokerPartitionId;
    this.emit('connecting', partition);
    partitionConsumer = new PartitionConsumer(this, partition, this.partitionConsumerConfig);
    partitionConsumer.pipe(this.preprocess);
    partitionConsumer.on('readable', function() {
      return _this.emit('partitionReadable', id);
    });
    _ref = ['connected', 'offsetUpdate', 'consuming', 'consumed', 'offsetOutOfRange'];
    _fn = function(event) {
      return partitionConsumer.on(event, function(arg1, arg2) {
        return _this.emit(event, id, arg1, arg2);
      });
    };
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      event = _ref[_i];
      _fn(event);
    }
    partitionConsumer.on('error', function(msg, detail) {
      return _this.emit('partitionError', id, msg, detail);
    });
    partitionConsumer.connect();
    return this.partitionConsumers[id] = partitionConsumer;
  };

  return TopicConsumer;

})(Readable);

/*
//@ sourceMappingURL=TopicConsumer.js.map
*/