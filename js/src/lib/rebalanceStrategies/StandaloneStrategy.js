// This file has been generated from coffee source files

var EventEmitter, StandaloneStrategy,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

EventEmitter = require('events').EventEmitter;

/*
  Consumer strategy to connect to all available partitions as registered in zookeeper
  assuming there are no other consumers (thus no distributed read).

  A rebalance strategy determines what topic partitions should be consumed for a given
  topic by a consumer in consumer group. The {@link TopicConsumer} is responsible for
  the actual rebalancing (connect and disconnect) of the provided topic partitions.

  @event partitions The partitions that should be consumed
  @event {String[]} partitions.partitions
  @event error On errors
  @event error.error Error details
*/


module.exports = StandaloneStrategy = (function(_super) {
  __extends(StandaloneStrategy, _super);

  /*
    Constructs the strategy.
  */


  function StandaloneStrategy(connections, consumerGroup, topic, consumerId, options) {
    this.zooKafka = connections.zooKafka;
    this.consumerGroup = consumerGroup;
    this.topic = topic;
    this.consumerId = consumerId;
  }

  /*
    Initiate providing partitions via the 'partitions' event.
  
    Currently only gets the registered topic partition once.
  
    TODO: watch zookeeper for changes, and emit new set of partitions
  
    @event partitions If new partitions
    @event error If error
  */


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

/*
//@ sourceMappingURL=StandaloneStrategy.js.map
*/