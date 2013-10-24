// This file has been generated from coffee source files

var EventEmitter, StandaloneStrategy,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

EventEmitter = require('events').EventEmitter;

StandaloneStrategy = (function(_super) {
  __extends(StandaloneStrategy, _super);

  function StandaloneStrategy(connections, consumerGroup, topic, consumerId, options) {
    this.zooKafka = connections.zooKafka;
    this.consumerGroup = consumerGroup;
    this.topic = topic;
    this.consumerId = consumerId;
  }

  StandaloneStrategy.prototype.connect = function() {
    var _this = this;
    return this.zooKafka.getRegisteredTopicPartitions(this.topic, function(err, partitions) {
      if (err) {
        return _this.emit('error', err);
      }
      return _this.emit('partitions', partitions);
    });
  };

  return StandaloneStrategy;

})(EventEmitter);

module.exports = StandaloneStrategy;

/*
//@ sourceMappingURL=Standalone.js.map
*/